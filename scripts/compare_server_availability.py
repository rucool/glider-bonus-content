#!/usr/bin/env python
"""
Realtime old-vs-new screen of the Rutgers glider ERDDAP servers for ACTIVE deployments - which look fine, and which
to run through the full comparison notebooks (server_comparison_wrangle.ipynb -> server_comparison_plots.ipynb):

  old  Rutgers operational ERDDAP (slocum-data)  {glider}-{ts}-profile-sci-{mode}, {glider}-{ts}-trajectory-raw-{mode}
  new  Rutgers test ERDDAP (slocum-test)          {glider}-{ts}-{mode}

Lifecycle: while a glider is deployed, the new processing pushes rt to new - this script checks it, morning and
evening. After recovery, delayed is added to new and check_new_delayed.py (weekly) checks it, with the same checks
(it imports this script's functions).

Which deployments: the active ones - still in the water, per the RUCOOL glider deployments API (no end date) - that
are on new, in rt mode. If the API can't be read, it falls back to rt deployments with data in the last --days (2).
--all-recent (with --days / --gliders) checks by data time instead, e.g. a manual run on a just-recovered glider.
The test server only holds a subset of deployments, so a deployment missing from new is never a problem.

Independent of the processing itself - it only looks at what each server is serving (tabledap):
  - existence, first and latest data time: one allDatasets request per server (minTime/maxTime metadata)
  - data check: time/position/depth/temperature/salinity pulled from old (profile-sci, depth_interpolated) and new
    (depth), every row, with the wrangle notebook's position filters (rows at 0,0 and rows > 500 km from the median
    position dropped) - so the scripts and notebooks compare the same rows

For each deployment checked:
  - last updated: latest data time on old and new, and the lag between them
  - value check: where old and new have the same timestamp, temperature and salinity should match (within storage
    precision: old is float32, new double - +/-1e-4 deg C, +/-1e-3 salinity); mean/median/std/min/max on each server
  - coverage at the same timestamp, temperature and salinity separately: the share of old's values new also has at
    exactly the same time; missing ones split into "no new row" and "new row, value blank" (catches scattered losses)
  - gap ranges: stretches where old has temperature/salinity and new has nothing within 10 s (catches runs of
    missing data); each server's own long gaps for context
  - temperature and salinity cross-sections, old and new side by side, embedded in the HTML
  - a verdict: OK, or RUN NOTEBOOKS (with a TEST_LIST / RUN_MODES to paste into the wrangle notebook)
Coverage and gap ranges use the same rules as find_old_only_ranges in the wrangle notebook (apples to apples).

Same-time coverage is PROVISIONAL: the 10 s gap rule can't see single dropped samples or blanked values (each has a
new neighbour ~2 s away), and same-time coverage can - e.g. ru43-20260227T1611 delayed: new has a row at every one
of old's timestamps, but has temperature blank at 6,330 of them (3.3%) and salinity blank at 13,540 (7.1%), while its
gap ranges show nothing. It isn't confirmed yet whether the new processing blanks these on purpose (QC, filtering) or
by mistake, so for now it raises WATCH at most (below 99%) and never INVESTIGATE on its own.

Every problem has a severity:
  INVESTIGATE  missing on old, values that differ, gaps of 10+ min where old has data and new doesn't,
               first/latest data more than --investigate-hours (6) apart
  WATCH        first/latest data between --lag-hours (3) and --investigate-hours apart; same-time coverage under 99%
  info         expected differences, listed but not counted (e.g. a deployment on new that has never been on old)
Each problem is listed once and stays listed every run until it clears, with the date it was first flagged; problems
that cleared since the previous run are listed once as resolved.

Usage:
  python compare_server_availability.py --html realtime_report.html --archive-dir reports
  python compare_server_availability.py --all-recent --days 30 --gliders "ru40-20261026T1706" --html check.html

Suggested cron (morning + evening):
  0 7,19 * * *  cd /path/to/here && /path/to/geo_env_test/bin/python compare_server_availability.py --html realtime_report.html --archive-dir reports >> availability.log 2>&1

Prints a plain-text report; --html also writes it as a self-contained HTML page (overwritten each run) and
--archive-dir keeps a dated copy. Exit code is 1 if anything is at INVESTIGATE level (or a server couldn't be
listed), 0 otherwise.
"""

import argparse
import base64
import html
import io
import json
import os
import re
import sys
import time
from urllib.parse import quote

import matplotlib
matplotlib.use('Agg')   # no display - figures are only saved into the HTML report
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests

SERVERS = {
    'old': 'https://slocum-data.marine.rutgers.edu/erddap',
    'new': 'https://slocum-test.marine.rutgers.edu/erddap',
}
SERVER_NAMES = {'old': 'old (Rutgers operational)', 'new': 'new (Rutgers test)'}

# --check presets. Any option given explicitly on the command line overrides these.
#   realtime: active deployments only (still in the water, per the RUCOOL deployments API - no end date). Once a
#             glider is recovered its delayed dataset is checked by check_new_delayed.py. 'days' is the fallback if
#             the API can't be reached: rt deployments with data in the last 2 days.
CHECKS = {
    'realtime': {'days': 2, 'new_days': 3, 'mode': 'rt', 'lag_hours': 3, 'investigate_hours': 6, 'data_check': True,
                 'active': True},
}
ACTIVE_API = 'https://marine.rutgers.edu/cool/data/gliders/api/deployments/?active=true'


def fetch_active_deployments():
    """Names (glider-timestamp) of the deployments currently in the water, from the RUCOOL glider deployments API
    (deployments with no end date). None if the API can't be read."""
    try:
        r = polite_get(ACTIVE_API)
        r.raise_for_status()
        return {d['deployment_name'] for d in r.json().get('data', []) if d.get('deployment_name')}
    except Exception:
        return None

OLD_ID = re.compile(r'^(?P<glider>.+)-(?P<ts>\d{8}T\d{4})-(?P<kind>profile-sci|trajectory-raw)-(?P<mode>rt|delayed)$')
NEW_ID = re.compile(r'^(?P<glider>.+)-(?P<ts>\d{8}T\d{4})-(?P<mode>rt|delayed)$')

# Latest-data-time columns, and the server each one comes from. First-data-time columns are the same + '_start'.
COLS = {'old_sci': 'old profile-sci', 'old_traj': 'old trajectory-raw', 'new': 'new'}
SERVER_OF = {'old_sci': 'old', 'old_traj': 'old', 'new': 'new'}

# Requests: a retry with back-off on timeouts / gateway errors, and a User-Agent so server admins can see who's asking
RETRY_STATUS = {429, 502, 503, 504}
RETRIES = 1
BACKOFF_S = 30
REQUEST_TIMEOUT_S = 120
HEADERS = {'User-Agent': 'RUCOOL glider server availability check (compare_server_availability.py, '
                         'Rutgers University glider data team)'}
STATE_DIR = os.path.dirname(os.path.abspath(__file__))   # .availability_state_<check>.json live next to this script

# Data check. Where old and new have a value at the same timestamp it should be the same value; the servers store
# them at different precision (old float32, e.g. 30.2111; new double, e.g. 30.21109962463379, and new's salinity is
# recalculated - ~1e-6 off), so "the same" means within these tolerances.
VALUE_TOL = {'temperature': 1e-4, 'salinity': 1e-3}
DATA_VARS = [('temperature', 'RdYlBu_r', 'deg C'), ('salinity', 'viridis', '')]
COVER_TOL = '10s'             # gap ranges: an old sample is "covered" if new has a value of that variable within this
COVER_WATCH = 0.99            # share of old's temperature (or salinity) values new has at the same timestamp: below -> watch
# Same-time coverage is provisional (see the module docstring): until it's confirmed whether the new processing drops
# or blanks samples on purpose, it raises WATCH at most. Set to e.g. 0.95 to make coverage below that INVESTIGATE.
COVER_INVESTIGATE = None
OLD_ONLY_MERGE = '5min'       # uncovered old samples closer together than this form one gap
GAP_FLAG_MINUTES = 10         # a gap (old has data, new doesn't) at least this long is INVESTIGATE
INTERNAL_GAP_MINUTES = 30     # stretches with no temperature in a dataset at least this long are listed for context
N_GAPS_SHOWN = 8
PLOT_MAX_POINTS = 60000       # per panel - every Nth point is drawn beyond this (statistics always use every value)

# Plain-language meaning of each problem type, shown in the HTML report
PROBLEM_KINDS = {
    'missing': ('Missing', 'The deployment exists on one server but not the other (never reported for new - it only '
                           'holds a subset).'),
    'behind': ('Behind', 'The latest data point on the named server is older than on the reference - it has not '
                         'picked up the most recent data yet.'),
    'ahead': ('Ahead', 'The named server has data later than the reference - the reference stopped updating early, '
                       'or never received the end of the deployment.'),
    'start': ('Late start', "The named server's first data point is later than the reference's - the beginning of "
                            'the deployment is missing there.'),
    'values': ('Values differ', 'Old and new have a value at the same timestamp but it isn\'t the same (beyond the '
                                'storage-precision tolerance) - the processing changed the calibrated data.'),
    'coverage': ('Coverage', "Same-time coverage: the share of old's temperature (or salinity) values that new also has "
                             'at exactly the same timestamp. It catches scattered losses the 10 s gap rule cannot - single '
                             'dropped samples (no new row at that time) and blanked values (new row, value blank). '
                             'PROVISIONAL: it is not confirmed yet whether the new processing drops/blanks these on '
                             'purpose (QC, filtering) or by mistake, so it is WATCH at most (below 99%), never '
                             'Investigate on its own.'),
    'gap': ('Data gap', 'A stretch where old has data and new has none.'),
}


# ---------------------------------------------------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------------------------------------------------

def polite_get(url, timeout=REQUEST_TIMEOUT_S):
    """requests.get, retried RETRIES times (after BACKOFF_S, or the server's Retry-After) on timeouts, connection
    errors and 429/502/503/504. Raises the last error if every attempt fails; otherwise returns the response."""
    for attempt in range(RETRIES + 1):
        try:
            r = requests.get(url, timeout=timeout, headers=HEADERS)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt == RETRIES:
                raise
            time.sleep(BACKOFF_S)
            continue
        if r.status_code in RETRY_STATUS and attempt < RETRIES:
            retry_after = r.headers.get('Retry-After', '')
            time.sleep(min(int(retry_after), 300) if retry_after.isdigit() else BACKOFF_S)
            continue
        return r


def fetch_all_datasets(key, id_regex=None):
    """datasetID / minTime / maxTime for every dataset on server `key` (only IDs matching `id_regex`, if given)."""
    query = 'datasetID,minTime,maxTime'
    if id_regex:
        query += '&datasetID=~' + quote(f'"{id_regex}"')
    r = polite_get(f'{SERVERS[key]}/tabledap/allDatasets.csv?{query}')
    if r.status_code == 404 and 'nRows = 0' in r.text:  # ERDDAP's "no matching datasets" - e.g. mid-restart
        return pd.DataFrame(columns=['datasetID', 'minTime', 'maxTime'])
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), skiprows=[1])  # row 1 is the units row
    for c in ('minTime', 'maxTime'):
        df[c] = pd.to_datetime(df[c], utc=True, errors='coerce')
    return df


def dataset_id(key, deployment, mode):
    """The science dataset ID for a deployment/mode on each server."""
    return f'{deployment}-profile-sci-{mode}' if key == 'old' else f'{deployment}-{mode}'


def _haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two lat/lon points (vectorized)."""
    lat1, lon1, lat2, lon2 = np.radians(lat1), np.radians(lon1), np.radians(lat2), np.radians(lon2)
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0 * np.arcsin(np.sqrt(a))


def drop_bad_positions(df, max_km_from_median=500):
    """The wrangle notebook's drop_null_island + drop_position_outliers, in the same order: drop rows at (0, 0),
    then rows more than 500 km from the remaining rows' median position. Rows with no position are kept."""
    df = df[~((df['latitude'] == 0) & (df['longitude'] == 0))]
    if len(df) < 3:
        return df
    dist_km = _haversine_km(df['latitude'], df['longitude'], df['latitude'].median(), df['longitude'].median())
    return df[~(dist_km > max_km_from_median)]


def pull_sci(key, deployment, mode):
    """time / latitude / longitude / depth / temperature / salinity for every row of one deployment's science dataset,
    with the wrangle notebook's position filters applied (drop_bad_positions) - so the same rows are compared. Old's
    depth is depth_interpolated (as in the comparison notebooks), new's is depth. Duplicate timestamps are kept, as in
    the notebook, so value counts and coverage match it. (The notebook's dropna(how='all') is left out: time is never
    blank, so it never drops a row.)"""
    depth_var = 'depth_interpolated' if key == 'old' else 'depth'
    url = (f'{SERVERS[key]}/tabledap/{dataset_id(key, deployment, mode)}.csv'
           f'?time,latitude,longitude,{depth_var},temperature,salinity')
    r = polite_get(url, timeout=600)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), skiprows=[1]).rename(columns={depth_var: 'depth'})
    df['time'] = pd.to_datetime(df['time'], utc=True)
    df = drop_bad_positions(df)
    return df.sort_values('time', kind='stable').reset_index(drop=True)


# ---------------------------------------------------------------------------------------------------------------------
# Building the comparison table
# ---------------------------------------------------------------------------------------------------------------------

def parse_ids(df, key):
    """allDatasets rows -> [{deployment, mode, col, minTime, maxTime}] for one server."""
    rows = []
    for r in df.itertuples():
        if key == 'old':
            m = OLD_ID.match(r.datasetID)
            col = ('old_sci' if m['kind'] == 'profile-sci' else 'old_traj') if m else None
        else:
            m = NEW_ID.match(r.datasetID)
            col = 'new'
        if m:
            rows.append({'deployment': f"{m['glider']}-{m['ts']}", 'mode': m['mode'], 'col': col,
                         'minTime': r.minTime, 'maxTime': r.maxTime})
    return rows


def build_table(long_rows):
    """One row per (deployment, mode): latest data time per server (COLS), first data time per server
    ('<col>_start'), and the deployment start time from its ID ('started')."""
    long_df = pd.DataFrame(long_rows, columns=['deployment', 'mode', 'col', 'minTime', 'maxTime'])
    grouped = long_df.groupby(['deployment', 'mode', 'col'])
    latest = grouped['maxTime'].max().unstack('col').reindex(columns=list(COLS))
    first = grouped['minTime'].min().unstack('col').reindex(columns=list(COLS)).add_suffix('_start')
    table = latest.join(first)
    # unstack/reindex can leave all-NaT or missing columns as object/float - force every column to UTC datetime
    table = table.apply(lambda s: pd.to_datetime(s, utc=True))
    starts = table.index.get_level_values('deployment').str.extract(r'(\d{8}T\d{4})$')[0]
    table['started'] = pd.to_datetime(starts.values, format='%Y%m%dT%H%M', utc=True)
    return table.sort_index()


# ---------------------------------------------------------------------------------------------------------------------
# Catalog checks - each returns a list of (kind, severity, message)
# ---------------------------------------------------------------------------------------------------------------------

def _ts(t):
    return '-' if pd.isna(t) else f'{t:%Y-%m-%d %H:%M}'


def _dur(td):
    """Signed duration, e.g. '+2.5 h' / '-3.1 days' / '-' for missing."""
    if pd.isna(td):
        return '-'
    h = td.total_seconds() / 3600
    if abs(h) < 0.05:
        return '0.0 h'
    return f'{h:+.1f} h' if abs(h) < 48 else f'{h / 24:+.1f} days'


def _minutes(td):
    m = td.total_seconds() / 60
    return f'{m:.0f} min' if m < 120 else f'{m / 60:.1f} h' if m < 48 * 60 else f'{m / 1440:.1f} days'


def check_missing(row, reachable, rt_retired=False, never_on_old=False):
    """Missing on old (profile-sci or trajectory-raw). New is never checked - it only holds a subset.
    rt_retired: an rt row whose deployment has a delayed dataset on old - old drops rt once delayed is processed.
    never_on_old: old has no dataset for this deployment in either mode (it's only on new) - listed as info."""
    if rt_retired or not reachable['old']:
        return []
    if never_on_old:
        return [('missing', 'info', 'on new but not on old in either mode - only run through the new processing?')]
    missing = [COLS[c] for c in ('old_sci', 'old_traj') if pd.isna(row[c])]
    if not missing:
        return []
    present = [COLS[c] for c in COLS if pd.notna(row[c])]
    return [('missing', 'investigate', f"missing on {', '.join(missing)} (present on {', '.join(present)})")]


def _lag_severity(td, watch, investigate):
    """None if within `watch`, else 'watch' or (beyond `investigate`) 'investigate'."""
    if abs(td) <= watch:
        return None
    return 'investigate' if abs(td) > investigate else 'watch'


def check_latest(row, ref, other, watch, investigate):
    """Latest data point on `other` more than `watch` behind/ahead of `ref`."""
    if pd.isna(row[ref]) or pd.isna(row[other]):
        return []
    diff = row[other] - row[ref]
    sev = _lag_severity(diff, watch, investigate)
    if not sev:
        return []
    kind = 'behind' if diff < pd.Timedelta(0) else 'ahead'
    word = 'behind' if kind == 'behind' else 'AHEAD of'
    return [(kind, sev, f'latest data on {COLS[other]} is {word} {COLS[ref]} by {_dur(diff).lstrip("+-")} '
                        f'({COLS[ref]} {_ts(row[ref])}, {COLS[other]} {_ts(row[other])})')]


def check_start(row, ref, other, watch, investigate):
    """First data point on `other` more than `watch` after `ref`'s."""
    a, b = row[f'{ref}_start'], row[f'{other}_start']
    if pd.isna(a) or pd.isna(b) or b <= a:
        return []
    sev = _lag_severity(b - a, watch, investigate)
    if not sev:
        return []
    return [('start', sev, f'first data on {COLS[other]} is {_dur(b - a).lstrip("+")} after {COLS[ref]} '
                           f'({COLS[ref]} {_ts(a)}, {COLS[other]} {_ts(b)})')]


def run_checks(table, checks):
    """Problems (dicts: dep, mode, kind, sev, msg) from applying each check(row, deployment, mode) to every row."""
    probs = []
    for (dep, mode), row in table.iterrows():
        for check in checks:
            probs += [{'dep': dep, 'mode': mode, 'kind': kind, 'sev': sev, 'msg': msg}
                      for kind, sev, msg in check(row, dep, mode)]
    return probs


# ---------------------------------------------------------------------------------------------------------------------
# Data check (realtime): pull temperature/salinity from old and new and compare the values themselves
# ---------------------------------------------------------------------------------------------------------------------

def _uncovered(a_ns, b_ns, tol_ns):
    """True for each time in a_ns with no time in (sorted) b_ns within tol_ns."""
    if len(b_ns) == 0:
        return np.ones(len(a_ns), bool)
    pos = np.searchsorted(b_ns, a_ns)
    before = b_ns[np.clip(pos - 1, 0, len(b_ns) - 1)]
    after = b_ns[np.clip(pos, 0, len(b_ns) - 1)]
    return np.minimum(np.abs(a_ns - before), np.abs(after - a_ns)) > tol_ns


def old_only_ranges(old, new):
    """Time ranges where old has a temperature or salinity and new has no value of that variable within COVER_TOL,
    merged across breaks shorter than OLD_ONLY_MERGE - the same rule as find_old_only_ranges in the wrangle notebook.
    Returns ([(start, end, n_old_timestamps)], n_uncovered_timestamps)."""
    t = old['time'].astype('int64').to_numpy()
    tol = pd.Timedelta(COVER_TOL).value
    any_miss = np.zeros(len(t), bool)
    for var, _, _ in DATA_VARS:
        valid = old[var].notna().to_numpy()
        n = new.loc[new[var].notna(), 'time'].astype('int64').to_numpy()
        any_miss[valid] |= _uncovered(t[valid], n, tol)
    miss = t[any_miss]
    if len(miss) == 0:
        return [], 0
    breaks = np.flatnonzero(np.diff(miss) > pd.Timedelta(OLD_ONLY_MERGE).value) + 1
    ranges = [(pd.Timestamp(run[0], tz='UTC'), pd.Timestamp(run[-1], tz='UTC'), len(run))
              for run in np.split(miss, breaks)]
    return ranges, len(miss)


def exact_coverage(old, new, var, new_all_times):
    """How many of old's `var` values new also has at exactly the same timestamp - the same rule as the wrangle
    notebook's exact coverage. Missing ones are split into 'no new row at that time' and 'new row, value blank';
    new_all_times is every new row's timestamp (after the position filters), so a new row without a temperature counts as a row."""
    o_times = old.loc[old[var].notna(), 'time']
    new_valid = set(new.loc[new[var].notna(), 'time'])
    new_rows = new_all_times
    missing = ~o_times.isin(new_valid)
    blank = missing & o_times.isin(new_rows)
    n_old = len(o_times)
    return {'old': n_old, 'matched': int((~missing).sum()), 'no_row': int((missing & ~blank).sum()),
            'blank': int(blank.sum()), 'frac': (~missing).sum() / n_old if n_old else np.nan}


def internal_gaps(times):
    """Stretches of at least INTERNAL_GAP_MINUTES with no temperature: [(start, end)], longest first."""
    t = times.reset_index(drop=True)
    d = t.diff()
    idx = d[d >= pd.Timedelta(minutes=INTERNAL_GAP_MINUTES)].sort_values(ascending=False).index
    return [(t[i - 1], t[i]) for i in idx]


def _ordinal(n):
    """2 -> '2nd', 4 -> '4th', 11 -> '11th'."""
    return f'{n}{"th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")}'


def cross_section_png(old, new, title):
    """Temperature (top) and salinity (bottom) depth-vs-time cross-sections, old (left) and new (right), on a shared
    color scale per variable (2nd-98th percentile of both) - the comparison notebooks' plot, side by side. Base64 PNG."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 8), sharex=True, sharey=True, constrained_layout=True)
    panels = {var: [(label, df[['time', 'depth', var]].dropna()) for label, df in (('old', old), ('new', new))]
              for var, _, _ in DATA_VARS}
    # one step for all four panels (set by the biggest), so old and new are thinned the same way
    step = max(1, -(-max(len(sub) for subs in panels.values() for _, sub in subs) // PLOT_MAX_POINTS))  # ceil division
    for i, (var, cmap, unit) in enumerate(DATA_VARS):
        subs = panels[var]
        both = pd.concat([s[var] for _, s in subs])
        vmin, vmax = (both.quantile(0.02), both.quantile(0.98)) if len(both) else (0, 1)
        sc = None
        for j, (label, sub) in enumerate(subs):
            shown = sub.iloc[::step]
            ax = axes[i, j]
            sc = ax.scatter(shown['time'].dt.tz_convert(None), shown['depth'], c=shown[var], cmap=cmap,
                            vmin=vmin, vmax=vmax, s=2, rasterized=True)
            ax.set_title(f'{label} - {var} ({len(sub):,} values)')
            if j == 0:
                ax.set_ylabel('depth (m)')
        fig.colorbar(sc, ax=list(axes[i]), label=f'{var} ({unit})' if unit else var)
    axes[0, 0].invert_yaxis()
    for ax in axes[1]:
        ax.set_xlabel('time (UTC)')
        ax.tick_params(axis='x', labelrotation=30)
    fig.suptitle(title + (f'\n(plotting every {_ordinal(step)} point)' if step > 1 else ''))
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=80)
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode('ascii')


def _stats(s):
    if not len(s):
        return ['0', '-', '-', '-', '-', '-']
    return [f'{len(s):,}', f'{s.mean():.4f}', f'{s.median():.4f}', f'{s.std():.4f}', f'{s.min():.4f}', f'{s.max():.4f}']


def data_check(deployment, mode, row, plots=True):
    """Pull old and new and compare. Returns (problems [(kind, sev, msg)], summary dict for the section table,
    block dict for the per-deployment detail in the report)."""
    block = {'title': f'{deployment} {mode}', 'lines': [], 'tables': [], 'lists': [], 'img': None, 'error': None}
    try:
        old = pull_sci('old', deployment, mode)
        new = pull_sci('new', deployment, mode)
    except Exception as ex:
        block['error'] = f'could not pull data: {type(ex).__name__}: {ex}'
        return [], {}, block
    # every new row's timestamp (after the position filters), to tell 'no new row at that time' from 'value blank'
    new_all_times = set(new['time'])
    # the rest works on rows with a temperature (salinity is computed from temperature, so none is lost)
    old = old[old['temperature'].notna()].reset_index(drop=True)
    new = new[new['temperature'].notna()].reset_index(drop=True)

    probs, summary = [], {}
    block['lines'].append(f'Last updated: old {_ts(row["old_sci"])}, new {_ts(row["new"])} UTC '
                          f'(new - old {_dur(row["new"] - row["old_sci"])}).')

    # 1. Values where both have the same timestamp
    # one value per timestamp for the pairing (a repeated timestamp would otherwise pair with every copy)
    pairs = old.drop_duplicates('time').merge(new.drop_duplicates('time'), on='time', suffixes=('_old', '_new'))
    value_rows, stat_rows = [], []
    for var, _, _ in DATA_VARS:
        both = pairs[[f'{var}_old', f'{var}_new']].dropna()
        diff = both[f'{var}_new'] - both[f'{var}_old']
        beyond = int((diff.abs() > VALUE_TOL[var]).sum())
        max_abs = diff.abs().max() if len(diff) else np.nan
        summary[f'{var}_max_diff'] = max_abs
        value_rows.append([var, f'{len(both):,}', f'{diff.mean():.2e}' if len(diff) else '-',
                           f'{max_abs:.2e}' if len(diff) else '-', f'+/-{VALUE_TOL[var]:g}', f'{beyond:,}'])
        if beyond:
            probs.append(('values', 'investigate',
                          f'{var} differs where both have a value: {beyond:,} of {len(both):,} same-time pairs beyond '
                          f'+/-{VALUE_TOL[var]:g} (largest difference {max_abs:.3g})'))
        for label, df in (('old', old), ('new', new)):
            stat_rows.append([var, label] + _stats(df[var].dropna()))
    summary['pairs'] = len(pairs)
    block['tables'].append(('All values on each server',
                            ['variable', 'server', 'n values', 'mean', 'median', 'std', 'min', 'max'], stat_rows))
    block['tables'].append(('Same timestamp on both servers - these should be identical',
                            ['variable', 'same-time pairs', 'mean diff (new - old)', 'max |diff|', 'tolerance',
                             'beyond tolerance'], value_rows))

    # 2. Coverage at the same timestamp, temperature and salinity separately: does new have every value old has?
    #    Catches scattered losses (single dropped samples, blanked values) that the gap ranges below can't.
    cover_rows = []
    for var, _, _ in DATA_VARS:
        c = exact_coverage(old, new, var, new_all_times)
        summary[f'{var}_covered'] = c['frac']
        cover_rows.append([var, f'{c["old"]:,}', f'{c["matched"]:,}', f'{c["frac"]:.2%}' if c['old'] else '-',
                           f'{c["no_row"]:,}', f'{c["blank"]:,}'])
        if c['old'] and c['frac'] < COVER_WATCH:
            sev = 'investigate' if COVER_INVESTIGATE is not None and c['frac'] < COVER_INVESTIGATE else 'watch'
            probs.append(('coverage', sev,
                          f'new has only {c["frac"]:.1%} of old\'s {var} values at the same timestamp '
                          f'({c["no_row"]:,} old timestamps have no new row, {c["blank"]:,} have a new row with {var} '
                          f'blank) - provisional check, see "Coverage" in How to read this report'))
    block['tables'].append(('Old values also in new at exactly the same timestamp (provisional - catches single '
                            'dropped samples and blanked values that the 10 s gap rule cannot; not yet confirmed '
                            'whether the new processing drops these on purpose)',
                            ['variable', 'old values', 'also in new', 'share', 'missing: no new row',
                             'missing: new row, value blank'], cover_rows))
    block['lines'].append(f'New has {len(new):,} temperature samples, old {len(old):,}.')

    # 3. Gap ranges: stretches where old has data and new has nothing within COVER_TOL (temperature or salinity)
    ranges, _ = old_only_ranges(old, new)
    summary['longest_gap'] = max((e - s for s, e, _ in ranges), default=pd.Timedelta(0))
    for s, e, n in ranges:
        if e - s >= pd.Timedelta(minutes=GAP_FLAG_MINUTES):
            probs.append(('gap', 'investigate', f'new has no data for {_minutes(e - s)} where old does '
                                                f'({_ts(s)} to {_ts(e)} UTC, {n:,} old samples)'))
    long_first = sorted(ranges, key=lambda r: r[1] - r[0], reverse=True)[:N_GAPS_SHOWN]
    block['lists'].append(('Gaps where old has data and new does not (longest first)',
                           [f'{_ts(s)} to {_ts(e)} UTC - {_minutes(e - s)}, {n:,} old samples' for s, e, n in long_first]
                           or ['none']))

    # 4. Each dataset's own long gaps (no temperature at all) - context: a gap on both is the glider, not processing
    for label, df in (('old', old), ('new', new)):
        gaps = internal_gaps(df['time'])
        block['lists'].append((f'Stretches of {INTERNAL_GAP_MINUTES}+ min with no temperature on {label} '
                               f'({len(gaps)} total, longest first)',
                               [f'{_ts(s)} to {_ts(e)} UTC - {_minutes(e - s)}' for s, e in gaps[:N_GAPS_SHOWN]]
                               or ['none']))

    if plots:
        block['img'] = cross_section_png(old, new, f'{deployment} {mode} - temperature and salinity, old vs new')
    return probs, summary, block


# ---------------------------------------------------------------------------------------------------------------------
# Tracking problems across runs
# ---------------------------------------------------------------------------------------------------------------------

def problem_key(p):
    """Identity of a problem across runs and sections: numbers and timestamps in the message are blanked, so a lag
    that grows from 3.5 h to 7.1 h is still the same problem (its severity is re-evaluated every run)."""
    return f'{p["dep"]}|{p["mode"]}|{p["kind"]}|' + re.sub(r'\d[\d,.:\- ]*', '#', p['msg'])


def track_since_last_run(state_path, scope_id, problems, can_save, run_time):
    """Sets p['since'] (when each problem was first flagged) from the previous run of this check, and returns
    {'prev_run', 'resolved', 'note'}. Saves this run as the new baseline unless can_save is False (a server couldn't
    be listed - its problems would vanish and then all come back with a reset 'since')."""
    prev = None
    if state_path and os.path.exists(state_path):
        try:
            with open(state_path) as f:
                prev = json.load(f)
        except (OSError, ValueError):
            prev = None
    usable = prev is not None and prev.get('scope') == scope_id
    prev_problems = prev['problems'] if usable else {}

    current = {}
    for p in problems:
        k = problem_key(p)
        p['since'] = prev_problems.get(k, {}).get('since', run_time)
        current[k] = p

    info = {'prev_run': prev['run'] if usable else None,
            'resolved': [v for k, v in prev_problems.items() if k not in current], 'note': ''}
    if prev is not None and not usable:
        info['note'] = 'Check settings changed since the last saved run - "flagged since" dates start over from this run.'
    elif prev is None:
        info['note'] = 'No previous run saved - "flagged since" dates start from this run.'
    if state_path and can_save:
        with open(state_path, 'w') as f:
            json.dump({'run': run_time, 'scope': scope_id, 'problems': current}, f)
    elif state_path:
        # A server's problems vanish when it can't be listed - they aren't resolved, so don't report them as such
        info['resolved'] = []
        info['note'] += (' ' if info['note'] else '') + \
            ('A server could not be listed, so nothing is reported as resolved and this run was NOT saved for the '
             'next comparison.')
    return info


# ---------------------------------------------------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------------------------------------------------

SEV_ORDER = {'investigate': 0, 'watch': 1, 'info': 2}
SEV_LABELS = {'investigate': 'Investigate', 'watch': 'Watch', 'info': 'For reference'}
PROBLEM_LIST_OPEN_MAX = 15   # longer problem lists start collapsed in the HTML


def make_section(title, intro, table, cols, extra, problems, empty_msg, blocks=None):
    """Everything both renderers need for one report section. extra: [(label, func(row, dep, mode))].
    blocks: optional per-deployment detail (data check) shown after the table."""
    header = ['deployment', 'mode'] + [COLS[c] for c in cols] + [label for label, _ in extra]
    rows = [[dep, mode] + [_ts(row[c]) for c in cols] + [func(row, dep, mode) for _, func in extra]
            for (dep, mode), row in table.iterrows()]
    return {'title': title, 'intro': intro, 'header': header, 'rows': rows, 'problems': problems,
            'problem_keys': {(p['dep'], p['mode']) for p in problems if p['sev'] != 'info'}, 'empty_msg': empty_msg,
            'blocks': blocks or []}


def notebook_test_list(to_run):
    """The deployments to run through server_comparison_wrangle/plots, as lines to paste into its config cell."""
    return [f"TEST_LIST = {sorted({d for d, _ in to_run})!r}", f"RUN_MODES = {sorted({m for _, m in to_run})!r}"]


def _text_table(header, rows):
    widths = [max(len(str(c)) for c in col) for col in zip(header, *rows)]
    fmt = lambda cells: '  '.join(f'{str(c):>{w}}' if i else f'{str(c):<{w}}' for i, (c, w) in enumerate(zip(cells, widths)))
    return [fmt(header), '-' * (sum(widths) + 2 * (len(widths) - 1))] + [fmt(r) for r in rows]


def _sorted(problems):
    return sorted(problems, key=lambda p: (SEV_ORDER[p['sev']], p['dep'], p['mode']))


def _sev_counts(problems):
    return sum(p['sev'] == 'investigate' for p in problems), sum(p['sev'] == 'watch' for p in problems)


def _split_info(problems):
    """(actionable problems, 'for reference' ones) - info items are listed separately and never counted."""
    return [p for p in problems if p['sev'] != 'info'], [p for p in problems if p['sev'] == 'info']


def render_text(meta, sections, changes=None):
    all_p = [p for s in sections for p in s['problems']]
    n_inv, n_watch = _sev_counts(all_p)
    lines = [f'Glider server availability check ({meta["check"]}) - {meta["now"]:%Y-%m-%d %H:%M} UTC']
    lines += [f'  {k} = {SERVERS[k]}' for k in SERVERS] + [''] + meta['server_lines'] + ['']
    lines.append(f'SUMMARY: {n_inv} to INVESTIGATE, {n_watch} to WATCH '
                 f'(lags: watch over {meta["watch_hours"]:g} h, investigate over {meta["investigate_hours"]:g} h; '
                 f'missing on old, value differences and data gaps are always investigate)')
    if meta.get('to_run'):
        lines += ['', 'RUN THROUGH THE COMPARISON NOTEBOOKS (server_comparison_wrangle -> plots):']
        lines += [f'  {d} {m}' for d, m in meta['to_run']]
        lines += ['  paste into the wrangle notebook config cell:'] + [f'    {l}' for l in notebook_test_list(meta['to_run'])]
    if meta.get('checked_ok'):
        lines += ['', 'DATA-CHECKED AND OK (no notebook run needed):'] + [f'  {d} {m}' for d, m in meta['checked_ok']]
    if changes is not None:
        if changes['note']:
            lines.append(changes['note'])
        if changes['prev_run']:
            lines.append(f'{len(changes["resolved"])} resolved since the last run ({changes["prev_run"]})'
                         + (':' if changes['resolved'] else ''))
            lines += [f'  RESOLVED  {p["dep"]} {p["mode"]}: {p["msg"]}  (flagged since {p["since"]})'
                      for p in changes['resolved']]
    for s in sections:
        lines += ['', '=' * 100, s['title'], '=' * 100]
        if not s['rows']:
            lines.append(s['empty_msg'])
            continue
        act, info = _split_info(s['problems'])
        inv, watch = _sev_counts(act)
        lines.append(f'{inv} to investigate, {watch} to watch' + (':' if act else ''))
        lines += [f'  [{p["sev"].upper():11s}] {p["dep"]} {p["mode"]}: {p["msg"]}'
                  + (f'  (flagged since {p["since"]})' if p.get('since') else '') for p in _sorted(act)]
        if info:
            lines.append(f'{len(info)} for reference (not counted):')
            lines += [f'  [INFO]        {p["dep"]} {p["mode"]}: {p["msg"]}' for p in _sorted(info)]
        # timestamp columns are 20 wide, the extra (difference/age/stat) columns 12 (or the header, if longer)
        widths = [28, 8] + [max(20 if h in COLS.values() else 12, len(h) + 2,
                                max((len(str(r[i])) + 2 for r in s['rows']), default=0))
                            for i, h in enumerate(s['header'][2:], start=2)]
        fmt = lambda cells: f'{cells[0]:{widths[0]}s} {cells[1]:{widths[1]}s}' + ''.join(
            f'{c:>{w}s}' for c, w in zip(cells[2:], widths[2:]))
        lines += ['', fmt(s['header']), '-' * (sum(widths) + 1)] + [fmt(r) for r in s['rows']]
        for b in s['blocks']:
            lines += ['', f'--- {b["title"]} ' + '-' * max(0, 90 - len(b['title']))]
            if b['error']:
                lines.append(f'!! {b["error"]}')
                continue
            lines += b['lines']
            for caption, header, rows in b['tables']:
                lines += ['', caption + ':'] + ['  ' + l for l in _text_table(header, rows)]
            for caption, items in b['lists']:
                lines += ['', caption + ':'] + [f'  {i}' for i in items]
    lines += ['', f'TOTAL: {n_inv} to investigate, {n_watch} to watch (check={meta["check"]}, mode={meta["mode"]})']
    return '\n'.join(lines)


HTML_CSS = """
:root { --bg:#f7f8fa; --card:#ffffff; --text:#1d2330; --muted:#5d6676; --border:#dde1e8; --head:#eef1f5;
        --ok:#1f7a4d; --ok-bg:#e3f4ea; --bad:#b42318; --bad-bg:#fdecea; --warn:#9a5b00; --warn-bg:#fff4dd;
        --info:#1d5fa8; --info-bg:#e6f0fb; --purple:#6b3fa0; --purple-bg:#f1eafb; --row-bad:#fff6f5;
        --on-solid:#ffffff; }
@media (prefers-color-scheme: dark) {
  :root { --bg:#14171c; --card:#1c2027; --text:#e6e9ee; --muted:#9aa3b2; --border:#2e343e; --head:#232832;
          --ok:#6fd39c; --ok-bg:#16301f; --bad:#ff8a80; --bad-bg:#3a1c1a; --warn:#f2c46b; --warn-bg:#352a12;
          --info:#8cbcf2; --info-bg:#172a40; --purple:#c9a8f2; --purple-bg:#2b2040; --row-bad:#2a1d1d;
          --on-solid:#14171c; }
}
* { box-sizing: border-box; }
body { margin:0; background:var(--bg); color:var(--text);
       font:15px/1.5 -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
main { max-width:1280px; margin:0 auto; padding:24px 16px 48px; }
h1 { font-size:24px; margin:0 0 4px; }
h2 { font-size:19px; margin:0 0 6px; }
.sub, .intro { color:var(--muted); margin:0 0 12px; }
.cards { display:grid; grid-template-columns:repeat(auto-fit, minmax(200px, 1fr)); gap:12px; margin:20px 0; }
.card, section { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:14px 16px; }
section { margin:16px 0; }
.card .n { font-size:28px; font-weight:700; }
.card .label { color:var(--muted); font-size:13px; }
.card.ok .n { color:var(--ok); } .card.bad .n { color:var(--bad); } .card.warn .n { color:var(--warn); }
.servers { margin:0; padding-left:18px; }
.alert { background:var(--warn-bg); color:var(--warn); border-radius:8px; padding:8px 12px; margin:8px 0; }
.badge { display:inline-block; font-size:12px; font-weight:600; border-radius:999px; padding:1px 9px; margin-right:6px; }
.badge.sev-investigate { background:var(--bad); color:var(--on-solid); }
.badge.sev-watch { background:var(--warn); color:var(--on-solid); }
.badge.sev-info { background:var(--head); color:var(--muted); }
.badge.missing, .badge.values, .badge.gap, .badge.coverage { background:var(--bad-bg); color:var(--bad); }
.badge.behind { background:var(--warn-bg); color:var(--warn); }
.badge.start { background:var(--purple-bg); color:var(--purple); }
.badge.ahead { background:var(--info-bg); color:var(--info); }
.badge.ok, .badge.resolved { background:var(--ok-bg); color:var(--ok); }
ul.problems { list-style:none; padding:0; margin:8px 0 14px; }
ul.problems li { padding:6px 0; border-bottom:1px solid var(--border); }
ul.problems li:last-child { border-bottom:none; }
.dep { font-weight:600; font-family:ui-monospace, Consolas, monospace; }
.since { color:var(--muted); font-size:12px; white-space:nowrap; }
.scroll { overflow-x:auto; }
table { border-collapse:collapse; width:100%; font-size:13px; font-variant-numeric:tabular-nums; }
th, td { padding:6px 10px; border-bottom:1px solid var(--border); text-align:right; white-space:nowrap; }
th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) { text-align:left; }
th { background:var(--head); font-weight:600; position:sticky; top:0; }
tr.bad td { background:var(--row-bad); }
td.none { color:var(--bad); font-weight:600; }
details summary { cursor:pointer; color:var(--muted); margin:6px 0; }
.note { color:var(--muted); font-style:italic; }
dl.legend { display:grid; grid-template-columns:max-content 1fr; gap:6px 12px; margin:6px 0 0; }
dl.legend dd { margin:0; color:var(--muted); }
.block { border-top:1px solid var(--border); margin-top:20px; padding-top:14px; }
.block h3 { font-size:16px; margin:0 0 6px; font-family:ui-monospace, Consolas, monospace; }
.block p { margin:4px 0; }
.caption { color:var(--muted); font-size:13px; margin:12px 0 4px; }
.block ul { margin:4px 0; padding-left:20px; font-size:13px; }
.block img { max-width:100%; height:auto; border:1px solid var(--border); border-radius:6px; margin-top:12px;
             background:#ffffff; }
pre.testlist { background:var(--head); padding:8px 10px; border-radius:6px; overflow-x:auto; font-size:13px; }
"""


def _problem_li(e, p, lead_badge=''):
    since = f' <span class="since">flagged since {e(p["since"])}</span>' if p.get('since') else ''
    return (f'<li>{lead_badge}<span class="badge sev-{p["sev"]}">{SEV_LABELS[p["sev"]]}</span>'
            f'<span class="badge {p["kind"]}">{PROBLEM_KINDS[p["kind"]][0]}</span>'
            f'<span class="dep">{e(p["dep"])}</span> {e(p["mode"])} &ndash; {e(p["msg"])}{since}</li>')


def render_html(meta, sections, changes=None):
    e = html.escape
    now = meta['now']
    all_p = [p for s in sections for p in s['problems']]
    n_inv, n_watch = _sev_counts(all_p)
    out = ['<!doctype html><html lang="en"><head><meta charset="utf-8">',
           '<meta name="viewport" content="width=device-width, initial-scale=1">',
           '<title>Glider Server Availability</title>', f'<style>{HTML_CSS}</style></head><body><main>',
           f'<h1>Glider servers, old vs new &ndash; {e(meta["check"])} check</h1>',
           f'<p class="sub">Checked {now:%Y-%m-%d %H:%M} UTC &middot; {e(meta["scope"])}</p>']

    # Summary cards: investigate / watch / resolved, then one per section
    out.append('<div class="cards">')
    out.append(f'<div class="card {"bad" if n_inv else "ok"}"><div class="n">{n_inv}</div>'
               f'<div class="label">to investigate</div></div>')
    out.append(f'<div class="card {"warn" if n_watch else "ok"}"><div class="n">{n_watch}</div>'
               f'<div class="label">to watch</div></div>')
    if changes is not None and changes['prev_run']:
        out.append(f'<div class="card ok"><div class="n">{len(changes["resolved"])}</div>'
                   f'<div class="label">resolved since last run ({e(changes["prev_run"])})</div></div>')
    for s in sections:
        inv, watch = _sev_counts(s['problems'])
        out.append(f'<div class="card {"bad" if inv else "warn" if watch else "ok"}"><div class="n">{inv + watch}</div>'
                   f'<div class="label">{e(s["title"].split(" (")[0])} &middot; {inv} investigate, {watch} watch '
                   f'&middot; {len(s["rows"])} checked</div></div>')
    out.append('</div>')

    # Good / bad: which deployments to take further with the full comparison notebooks, and which looked fine
    if meta.get('to_run') or meta.get('checked_ok'):
        out.append('<section><h2>Which deployments need the full notebook comparison</h2>')
        if meta.get('to_run'):
            out.append('<p><span class="badge sev-investigate">Run notebooks</span>Something to investigate or watch '
                       'below. Paste into the config cell of <code>server_comparison_wrangle.ipynb</code> (with '
                       '<code>LET_IT_RIP = False</code>), then run <code>server_comparison_plots.ipynb</code>.</p>'
                       '<ul class="problems">')
            out += [f'<li><span class="dep">{e(d)}</span> {e(m)}</li>' for d, m in meta['to_run']]
            out.append(f'</ul><pre class="testlist">{e(chr(10).join(notebook_test_list(meta["to_run"])))}</pre>')
        if meta.get('checked_ok'):
            out.append('<p><span class="badge ok">OK</span>Data-checked with nothing flagged - same values where both '
                       'servers have data, nothing old has that new is missing, no lag. No notebook run needed.</p>'
                       '<ul class="problems">')
            out += [f'<li><span class="dep">{e(d)}</span> {e(m)}</li>' for d, m in meta['checked_ok']]
            out.append('</ul>')
        out.append('</section>')

    # Servers + how to read the report
    out.append('<section><h2>Servers</h2><ul class="servers">')
    for k in SERVERS:
        out.append(f'<li><b>{e(SERVER_NAMES[k])}</b>: <a href="{e(SERVERS[k])}">{e(SERVERS[k])}</a> '
                   f'&ndash; {meta["counts"].get(k, "?")} datasets listed</li>')
    out.append('</ul>')
    out += [f'<div class="alert">{e(msg)}</div>' for msg in meta['alerts']]
    if changes is not None and changes['note']:
        out.append(f'<p class="note">{e(changes["note"])}</p>')
    out.append('<details><summary>How to read this report</summary><dl class="legend">')
    out.append(f'<dt><span class="badge sev-investigate">Investigate</span></dt><dd>Missing on old, temperature/'
               f'salinity values that differ, data gaps of {GAP_FLAG_MINUTES}+ min, and lags over '
               f'{meta["investigate_hours"]:g} h.</dd>')
    out.append(f'<dt><span class="badge sev-watch">Watch</span></dt><dd>Lags between {meta["watch_hours"]:g} and '
               f'{meta["investigate_hours"]:g} h &ndash; usually catches up; becomes Investigate if it grows past '
               f'{meta["investigate_hours"]:g} h.</dd>')
    out.append('<dt><span class="badge sev-info">For reference</span></dt><dd>Expected differences, listed '
               '(collapsed) but not counted - e.g. a deployment on new that has never been on old.</dd>')
    for kind, (label, desc) in PROBLEM_KINDS.items():
        out.append(f'<dt><span class="badge {kind}">{label}</span></dt><dd>{e(desc)}</dd>')
    out.append('<dt>flagged since</dt><dd>When this problem was first seen by this check. It stays listed every run '
               'until it clears, then shows once under &ldquo;Resolved since last run&rdquo;.</dd>')
    out.append('<dt>&ndash;</dt><dd>In the tables, a red &ldquo;&ndash;&rdquo; in a time column means the dataset '
               'does not exist on that server. Rows with a problem are highlighted. &ldquo;new &ndash; old&rdquo; is '
               'positive when new has later data. Each problem is listed once, in the first section it appears in.'
               '</dd></dl></details></section>')

    # Resolved since the last run
    if changes is not None and changes['resolved']:
        out.append(f'<section><h2>Resolved since last run ({e(changes["prev_run"])})</h2><ul class="problems">')
        out += [_problem_li(e, p, '<span class="badge resolved">Resolved</span>') for p in changes['resolved']]
        out.append('</ul></section>')

    for s in sections:
        out.append(f'<section><h2>{e(s["title"])}</h2><p class="intro">{e(s["intro"])}</p>')
        if not s['rows']:
            out.append(f'<p>{e(s["empty_msg"])}</p></section>')
            continue
        act, info = _split_info(s['problems'])
        if act:
            inv, watch = _sev_counts(act)
            out.append('<details' + (' open' if len(act) <= PROBLEM_LIST_OPEN_MAX else '') +
                       f'><summary>{inv} to investigate, {watch} to watch</summary><ul class="problems">')
            out += [_problem_li(e, p) for p in _sorted(act)]
            out.append('</ul></details>')
        else:
            out.append('<p><span class="badge ok">OK</span>No problems.</p>')
        if info:
            out.append(f'<details><summary>{len(info)} for reference (not counted)</summary><ul class="problems">')
            out += [_problem_li(e, p) for p in _sorted(info)]
            out.append('</ul></details>')
        out.append('<details' + (' open' if len(s['rows']) <= 20 else '') +
                   f'><summary>All {len(s["rows"])} rows</summary><div class="scroll"><table><thead><tr>')
        out += [f'<th>{e(h)}</th>' for h in s['header']]
        out.append('</tr></thead><tbody>')
        time_cols = {i for i, h in enumerate(s['header']) if h in COLS.values()}
        for r in s['rows']:
            cls = ' class="bad"' if (r[0], r[1]) in s['problem_keys'] else ''
            cells = ''.join('<td class="none">&ndash;</td>' if (i in time_cols and c == '-') else f'<td>{e(c)}</td>'
                            for i, c in enumerate(r))
            out.append(f'<tr{cls}>{cells}</tr>')
        out.append('</tbody></table></div></details>')
        for b in s['blocks']:
            out.append(f'<div class="block"><h3>{e(b["title"])}</h3>')
            if b['error']:
                out.append(f'<div class="alert">{e(b["error"])}</div></div>')
                continue
            out += [f'<p>{e(line)}</p>' for line in b['lines']]
            for caption, header, rows in b['tables']:
                out.append(f'<p class="caption">{e(caption)}</p><div class="scroll"><table><thead><tr>')
                out += [f'<th>{e(h)}</th>' for h in header]
                out.append('</tr></thead><tbody>')
                out += ['<tr>' + ''.join(f'<td>{e(str(c))}</td>' for c in r) + '</tr>' for r in rows]
                out.append('</tbody></table></div>')
            for caption, items in b['lists']:
                out.append(f'<p class="caption">{e(caption)}</p><ul>' + ''.join(f'<li>{e(i)}</li>' for i in items)
                           + '</ul>')
            if b['img']:
                out.append(f'<img src="data:image/png;base64,{b["img"]}" alt="{e(b["title"])} temperature and '
                           f'salinity cross-sections, old (left) and new (right)">')
            out.append('</div>')
        out.append('</section>')

    out.append('</main></body></html>')
    return '\n'.join(out)


# ---------------------------------------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description=__doc__.split('\n\n')[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--check', choices=list(CHECKS), default='realtime',
                   help='preset (only realtime now): active rt deployments + data check. Default realtime')
    p.add_argument('--days', type=float, default=None,
                   help='with --all-recent, or if the active-deployment list can\'t be read: deployments whose latest '
                        'data on either server is within this many days (0 = all). Default 2')
    p.add_argument('--all-recent', dest='active', action='store_false', default=None,
                   help='check deployments with data in the last --days instead of the active ones (e.g. a manual run '
                        'on a just-recovered glider with --gliders and --days)')
    p.add_argument('--new-days', type=float, default=None,
                   help='"recently started" section: deployments that started within this many days')
    p.add_argument('--lag-hours', type=float, default=None,
                   help='first/latest data points this far apart are flagged WATCH. Default 3')
    p.add_argument('--investigate-hours', type=float, default=None,
                   help='... and this far apart are flagged INVESTIGATE. Default 6')
    p.add_argument('--mode', choices=['rt', 'delayed', 'both'], default=None)
    p.add_argument('--gliders', default=None, help='regex to limit deployments, e.g. "ru40|ru43"')
    p.add_argument('--data-check', dest='data_check', action='store_true', default=None,
                   help='pull temperature/salinity for the rt deployments on new and compare values, gaps and '
                        'cross-sections (default on)')
    p.add_argument('--no-data-check', dest='data_check', action='store_false', help='skip the data check')
    p.add_argument('--no-plots', action='store_true', help='data check without the cross-section figures')
    p.add_argument('--html', default=None, help='also write the report as a self-contained HTML page to this path')
    p.add_argument('--archive-dir', default=None,
                   help='also save a dated copy of the HTML report here, e.g. reports/realtime_20261027_0700.html')
    p.add_argument('--csv', default=None, help='also save the full comparison table to this CSV path')
    p.add_argument('--state', default=None,
                   help='where this check\'s problem list is saved for the "flagged since"/resolved comparison. '
                        'Default: .availability_state_<check>.json next to this script; not used with --gliders '
                        '(a filtered run would wipe the cron\'s baseline) unless given explicitly')
    args = p.parse_args()
    for k, v in CHECKS[args.check].items():
        if getattr(args, k) is None:
            setattr(args, k, v)

    now = pd.Timestamp.now(tz='UTC')
    watch, inv = pd.Timedelta(hours=args.lag_hours), pd.Timedelta(hours=args.investigate_hours)
    id_regex = f'({args.gliders}).*' if args.gliders else None
    scope = (f'deployments with data in the last {args.days:g} days' if args.days > 0 else 'all deployments') \
        + f', mode {args.mode}'
    meta = {'now': now, 'check': args.check, 'scope': scope, 'watch_hours': args.lag_hours,
            'investigate_hours': args.investigate_hours, 'mode': args.mode,
            'counts': {}, 'alerts': [], 'server_lines': []}

    # A server that answers with no datasets is treated like an unreachable one (its deployments aren't reported as
    # missing) - an ERDDAP mid-restart lists nothing, or a slowly growing list while it reloads.
    long_rows, reachable = [], {}
    for key in SERVERS:
        try:
            df = fetch_all_datasets(key, id_regex)
        except Exception as ex:
            reachable[key] = False
            msg = f'could not reach {key} ({SERVERS[key]}): {type(ex).__name__}: {ex}'
            meta['alerts'].append(msg)
            meta['server_lines'].append(f'!! {msg}')
            continue
        reachable[key] = len(df) > 0
        meta['counts'][key] = len(df)
        meta['server_lines'].append(f'{key}: {len(df)} datasets listed')
        if len(df) == 0:
            msg = f'{key} returned no datasets - server may be restarting/reloading; not reporting "missing" for it'
            meta['alerts'].append(msg)
            meta['server_lines'].append(f'!! {msg}')
        long_rows += parse_ids(df, key)
    all_listed = all(reachable.values())
    if not long_rows:
        print(render_text(meta, []))
        return 1

    table = build_table(long_rows)

    # Old drops a deployment's rt datasets once its delayed dataset is in, so "rt missing on old" is expected for
    # those. Worked out before the mode filter so it also applies to the rt-only realtime check.
    old_has_delayed = {dep for (dep, mode), row in table.iterrows() if mode == 'delayed' and pd.notna(row['old_sci'])}
    retired = lambda d, m: m == 'rt' and d in old_has_delayed
    on_old_ever = {dep for (dep, mode), row in table.iterrows() if pd.notna(row['old_sci']) or pd.notna(row['old_traj'])}

    if args.mode != 'both':
        table = table[table.index.get_level_values('mode') == args.mode]

    # Which deployments to check: the active ones (still in the water, per the RUCOOL deployments API). If the API
    # can't be read, fall back to deployments with data in the last --days.
    deps = table.index.get_level_values('deployment')
    active = fetch_active_deployments() if args.active else None
    if active is not None:
        in_window = pd.Series(deps.isin(active), index=table.index)
        scope = meta['scope'] = f'active deployments (RUCOOL deployments API), mode {args.mode}'
        not_on_new = sorted(active - set(deps[table['new'].notna()]))
        meta['server_lines'].append(f'active deployments: {len(active)} - '
                                    f'{len(active) - len(not_on_new)} on new and checked' +
                                    (f'; not on new (not checked): {", ".join(not_on_new)}' if not_on_new else ''))
    else:
        if args.active:
            msg = (f'could not read the active-deployment list ({ACTIVE_API}) - checking deployments with data in the '
                   f'last {args.days:g} days instead')
            meta['alerts'].append(msg)
            meta['server_lines'].append(f'!! {msg}')
        latest_any = table[list(COLS)].max(axis=1)
        in_window = latest_any >= now - pd.Timedelta(days=args.days) if args.days > 0 else latest_any.notna()
    recent = table[table['started'] >= now - pd.Timedelta(days=args.new_days)]
    nw = table[table['new'].notna() & in_window].sort_values('new', ascending=False)

    # Each problem is listed once, in the first section it turns up in. The rows stay in every section's table.
    reported = set()

    def once(probs):
        out = []
        for prob in probs:
            k = problem_key(prob)
            if k not in reported:
                reported.add(k)
                out.append(prob)
        return out

    catalog_checks = [
        lambda r, d, m: check_missing(r, reachable, retired(d, m), d not in on_old_ever),
        lambda r, d, m: check_start(r, 'old_sci', 'new', watch, inv) + check_latest(r, 'old_sci', 'new', watch, inv),
    ]

    # --- Recently started deployments ---
    sections = [make_section(
        f'Recently started deployments (last {args.new_days:g} days)',
        'Deployments that started recently. They should be on old; the test server (new) only holds a subset of '
        'deployments for now, so new is compared only when the deployment is on it.',
        recent, list(COLS),
        [('started', lambda r, d, m: f'{r["started"]:%m-%d %H:%M}'),
         ('newest age', lambda r, d, m: _dur(now - r[list(COLS)].max()).lstrip('+'))],
        once(run_checks(recent, catalog_checks)), f'No deployments started in the last {args.new_days:g} days.')]

    # --- Everything on new, checked against old: last-updated time / lag ---
    sections.append(make_section(
        f'New (test server) vs old - last updated ({scope})',
        'Every deployment on the test server, with the latest data time on each server. New often has later data '
        f'than old (it keeps data old drops); more than {args.lag_hours:g} h apart is watch, more than '
        f'{args.investigate_hours:g} h is investigate. Most recent first.',
        nw, ['new', 'old_sci'], [('new - old', lambda r, d, m: _dur(r['new'] - r['old_sci']))],
        once(run_checks(nw, catalog_checks)), 'Nothing on new in range.'))

    # --- Data check: pull temperature/salinity from old and new and compare the values themselves ---
    if args.data_check:
        dc_rows = nw[(nw.index.get_level_values('mode') == 'rt') & nw['old_sci'].notna()]
        dc_probs, dc_summary, dc_blocks = [], {}, []
        for (dep, mode), row in dc_rows.iterrows():
            print(f'Data check {dep} {mode}...', file=sys.stderr)
            found, summary, block = data_check(dep, mode, row, plots=not args.no_plots)
            dc_probs += [{'dep': dep, 'mode': mode, 'kind': k, 'sev': s, 'msg': msg} for k, s, msg in found]
            dc_summary[(dep, mode)] = summary
            dc_blocks.append(block)
            if block['error']:
                meta['alerts'].append(f'data check {dep} {mode}: {block["error"]}')

        def sm(field, fmt):
            return lambda r, d, m: fmt(dc_summary[(d, m)][field]) if field in dc_summary.get((d, m), {}) else '-'

        # Verdict per deployment, from everything found for it in this report (lag in the earlier sections plus the
        # data check): anything to investigate or watch -> run it through the comparison notebooks.
        flagged = {(p['dep'], p['mode']) for p in dc_probs + [p for s in sections for p in s['problems']]
                   if p['sev'] in ('investigate', 'watch')}
        failed = {(dep, mode) for (dep, mode), b in zip(dc_rows.index, dc_blocks) if b['error']}

        def verdict(d, m):
            return 'NO DATA' if (d, m) in failed else 'RUN NOTEBOOKS' if (d, m) in flagged else 'OK'

        for (dep, mode), b in zip(dc_rows.index, dc_blocks):
            b['title'] = f'{b["title"]} - {verdict(dep, mode)}'
        meta['checked_ok'] = [(d, m) for d, m in dc_rows.index if verdict(d, m) == 'OK']

        sections.append(make_section(
            'Data check - temperature and salinity, old vs new (rt)',
            'For each rt deployment on the test server: temperature and salinity pulled from both servers. Where both '
            'have a value at the same timestamp the values should be the same (within the storage-precision tolerance: '
            f'+/-{VALUE_TOL["temperature"]:g} for temperature, +/-{VALUE_TOL["salinity"]:g} for salinity). New usually '
            'has MORE samples than old; what matters is that it has every sample old has - gaps where old has data and '
            f'new does not are listed, and flagged when {GAP_FLAG_MINUTES}+ min. Cross-sections: old left, new right, '
            'same color scale. Verdict: OK = nothing flagged for it anywhere in this report; RUN NOTEBOOKS = something '
            'to investigate or watch - take it through the full comparison notebooks.',
            dc_rows, ['old_sci', 'new'],
            [('verdict', lambda r, d, m: verdict(d, m)),
             ('same-time pairs', sm('pairs', lambda v: f'{v:,}')),
             ('T max |diff|', sm('temperature_max_diff', lambda v: f'{v:.1e}')),
             ('S max |diff|', sm('salinity_max_diff', lambda v: f'{v:.1e}')),
             ('T in new', sm('temperature_covered', lambda v: f'{v:.2%}')),
             ('S in new', sm('salinity_covered', lambda v: f'{v:.2%}')),
             ('longest gap', sm('longest_gap', lambda v: _minutes(v) if v > pd.Timedelta(0) else '-'))],
            once(dc_probs), 'No rt deployments on new with recent data.', blocks=dc_blocks))

    for i, s in enumerate(sections, start=1):
        s['title'] = f'{i}. {s["title"]}'
    all_problems = [prob for s in sections for prob in s['problems']]

    # The deployments worth running through the full comparison notebooks: anything to investigate or watch
    meta['to_run'] = sorted({(p['dep'], p['mode']) for p in all_problems if p['sev'] in ('investigate', 'watch')})

    # "Flagged since" + resolved, from the last run of this same check (the cron's baseline; filtered --gliders runs
    # don't touch it). Severity isn't part of the scope, so changing the thresholds keeps the history.
    state_path = args.state or (None if args.gliders else
                                os.path.join(STATE_DIR, f'.availability_state_{args.check}.json'))
    scope_id = f'old-new|{args.check}|{id_regex}|{args.days:g}|{args.new_days:g}|{args.mode}|{args.data_check}|active={args.active}'
    changes = (track_since_last_run(state_path, scope_id, all_problems, all_listed, f'{now:%Y-%m-%d %H:%M} UTC')
               if state_path else None)

    print(render_text(meta, sections, changes))

    if args.html or args.archive_dir:
        page = render_html(meta, sections, changes)
        paths = [args.html] if args.html else []
        if args.archive_dir:
            os.makedirs(args.archive_dir, exist_ok=True)
            paths.append(os.path.join(args.archive_dir, f'{args.check}_{now:%Y%m%d_%H%M}.html'))
        for path in paths:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(page)
            print(f'Saved HTML report to {path}')

    if args.csv:
        out = table.copy()
        out['new_minus_old_h'] = (out['new'] - out['old_sci']).dt.total_seconds() / 3600
        out.to_csv(args.csv)
        print(f'Saved table to {args.csv}')

    # Exit 1 (e.g. for cron mail) when there's something to investigate, or a server couldn't be listed
    needs_look = any(prob['sev'] == 'investigate' for prob in all_problems)
    return 1 if needs_look or not all_listed else 0


if __name__ == '__main__':
    sys.exit(main())
