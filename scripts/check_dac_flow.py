"""Daily data-flow check for the active Rutgers glider deployments: is the data reaching the IOOS Glider DAC, and is it
keeping up?

For every deployment in the water (RUCOOL deployments API, no end date - the deployments Rutgers is responsible for,
including other institutions' gliders we process) it reports how far each server's latest data is behind the
glider's last call-in - each server measured on its own against the call-in:

  glider last call-in (API)  vs  Rutgers operational ERDDAP (rt)
  glider last call-in (API)  vs  IOOS Glider DAC ERDDAP (rt dataset)

The DAC is not measured against the Rutgers ERDDAP: the DAC is not fed from it (the processing sends the DAC its
files), so the DAC can have data the Rutgers ERDDAP doesn't show yet, and "DAC behind Rutgers" would hide a slow
Rutgers ERDDAP.

Latest data times come from each ERDDAP's allDatasets listing (maxTime - what each server is advertising to users).
Rutgers' time is the newer of profile-sci-rt and trajectory-raw-rt: the DAC dataset carries flight data as well as
science, so this is the like-for-like time (science can be a dive behind while the last science file comes in).
That's three small requests per run, one to each place, and only for the active deployments, so the DAC is barely
touched.

Flags (thresholds are options):
  - not on Rutgers / not on the DAC: INVESTIGATE (WATCH while the deployment is less than a day old)
  - Rutgers or DAC behind the glider's last call-in: WATCH over 12 h, INVESTIGATE over 24 h (same for both)
    The HTML colors the columns the same way: green under 12 h, orange 12-24 h, red over 24 h.
  - glider hasn't called in for 12+ h: for reference only (a piloting matter, not a data-flow one)

Every run is appended to dac_flow_history.csv (next to this script), which gives each deployment's lag over time:
the report shows when a lag started (the first run in the current streak over the watch threshold), how long the
latest times have been unchanged ("frozen since"), and the last runs as a small trend table.

Run it once a day by cron through the wrapper, which also emails the report (settings in run_glider_check.conf):
  0 8 * * *  bash /path/to/run_glider_check.sh dac >> /path/to/dac_flow.log 2>&1
or on its own:
  python check_dac_flow.py --html dac_flow_report.html --archive-dir reports

A manual run (e.g. --gliders ru37) is not added to the history unless --save-history is given.
"""
import argparse
import html
import io
import os
import sys
from urllib.parse import quote

import pandas as pd

import compare_server_availability as csa

DAC = 'https://gliders.ioos.us/erddap'
RUTGERS = csa.SERVERS['old']
# The DAC dataset carries the flight data as well as the science, so Rutgers' like-for-like latest time is the newer
# of these - science (profile-sci) can be a dive behind flight (trajectory-raw) while the last science file comes in.
RUTGERS_IDS = ['{}-profile-sci-rt', '{}-trajectory-raw-rt']
AHEAD_SLACK = pd.Timedelta(minutes=15)   # data up to this far past the call-in still counts as "caught up"
HISTORY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dac_flow_history.csv')
# dac_behind_callin_h replaced dac_behind_h (DAC behind Rutgers) on 2026-11-06; a history file with the old columns
# is moved aside rather than mixed in (read_history).
HISTORY_COLS = ['run', 'deployment', 'last_call_in', 'rutgers_latest', 'dac_latest',
                'rutgers_behind_h', 'dac_behind_callin_h', 'status']
TREND_RUNS = 10          # history rows shown per deployment
SILENT_HOURS = 12        # no call-in for this long -> listed for reference
NEW_DEPLOYMENT_HOURS = 24
STATUS_ORDER = {'investigate': 0, 'watch': 1, 'ok': 2}


def fetch_active():
    """[{deployment, started, last_call_in}] for the deployments in the water, or raises."""
    r = csa.polite_get(csa.ACTIVE_API)
    r.raise_for_status()
    out = []
    for d in r.json().get('data', []):
        if not d.get('deployment_name'):
            continue
        surf = d.get('last_surfacing') or {}
        ts = lambda v: pd.Timestamp(v, unit='s', tz='UTC') if v else pd.NaT
        out.append({'deployment': d['deployment_name'], 'started': ts(d.get('start_date_epoch')),
                    'last_call_in': ts(surf.get('connect_time_epoch'))})
    return out


def latest_times(server, id_patterns, names):
    """{deployment: newest maxTime across its datasets} from one allDatasets request; each of id_patterns turns a
    deployment name into one of its dataset IDs."""
    ids = {p.format(n): n for p in id_patterns for n in names}
    regex = '(' + '|'.join(ids) + ')'
    r = csa.polite_get(f'{server}/tabledap/allDatasets.csv?datasetID,maxTime&datasetID=~' + quote(f'"{regex}"'))
    if r.status_code == 404 and 'nRows = 0' in r.text:   # ERDDAP's "nothing matched"
        return {}
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), skiprows=[1])
    df = df[df['datasetID'].isin(ids)]
    df['maxTime'] = pd.to_datetime(df['maxTime'], utc=True, errors='coerce')
    return df.groupby(df['datasetID'].map(ids))['maxTime'].max().to_dict()


def _h(td):
    return td.total_seconds() / 3600 if pd.notna(td) else float('nan')


def _lag(td):
    """How far behind: '2.4 h' / '1.5 days'; 'caught up' within a few minutes (or up to AHEAD_SLACK past);
    'ahead 0.8 h' when the data is newer than the reference time; '-' when either time is missing."""
    if pd.isna(td):
        return '-'
    if td < -AHEAD_SLACK:
        return f'ahead {csa._dur(-td).lstrip("+")}'
    return 'caught up' if td <= pd.Timedelta(minutes=3) else csa._dur(td).lstrip('+')


def _sev(td, watch_h, inv_h):
    h = _h(td)
    return None if not h > watch_h else 'investigate' if h > inv_h else 'watch'


def check(dep, now, args):
    """Flags for one deployment: [(sev, msg)]."""
    probs = []
    young = pd.notna(dep['started']) and now - dep['started'] < pd.Timedelta(hours=NEW_DEPLOYMENT_HOURS)
    missing_sev = 'watch' if young else 'investigate'
    young_note = ' (deployment is less than a day old)' if young else ''
    if pd.isna(dep['rutgers_latest']):
        probs.append((missing_sev, f'not on the Rutgers ERDDAP (no {dep["deployment"]}-profile-sci-rt or '
                                   f'-trajectory-raw-rt){young_note}'))
    if pd.isna(dep['dac_latest']):
        probs.append((missing_sev, f'not on the DAC ({dep["deployment"]}){young_note}'))
    sev = _sev(dep['rutgers_behind'], args.rutgers_watch_hours, args.rutgers_investigate_hours)
    if sev:
        probs.append((sev, f'Rutgers ERDDAP is {_lag(dep["rutgers_behind"])} behind the glider\'s last call-in '
                           f'(call-in {csa._ts(dep["last_call_in"])}, Rutgers latest {csa._ts(dep["rutgers_latest"])})'))
    sev = _sev(dep['dac_behind'], args.dac_watch_hours, args.dac_investigate_hours)
    if sev:
        probs.append((sev, f'DAC is {_lag(dep["dac_behind"])} behind the glider\'s last call-in '
                           f'(call-in {csa._ts(dep["last_call_in"])}, DAC latest {csa._ts(dep["dac_latest"])})'))
    silent = now - dep['last_call_in']
    if pd.notna(silent) and _h(silent) > SILENT_HOURS:
        probs.append(('info', f'glider has not called in for {_lag(silent)} (last call-in {csa._ts(dep["last_call_in"])})'
                              ' - nothing new to flow downstream until it does'))
    return probs


def read_history(meta, will_save):
    """The saved runs. A file with other columns (written before the DAC lag was measured from the call-in) is not
    mixed in: on a run that saves, it is renamed to dac_flow_history_old_<date>.csv and a new history starts."""
    if not os.path.exists(HISTORY_PATH):
        return pd.DataFrame(columns=HISTORY_COLS)
    h = pd.read_csv(HISTORY_PATH)
    if list(h.columns) != HISTORY_COLS:
        if will_save:
            old = HISTORY_PATH.replace('.csv', f'_old_{meta["now"]:%Y%m%d_%H%M}.csv')
            os.replace(HISTORY_PATH, old)
            meta['alerts'].append(f'the history had the old columns (DAC lag measured behind Rutgers) - moved to '
                                  f'{os.path.basename(old)}; a new history starts with this run')
        else:
            meta['alerts'].append('the history has the old columns (DAC lag measured behind Rutgers) - not used; '
                                  'the next saved run moves it aside and starts a new one')
        return pd.DataFrame(columns=HISTORY_COLS)
    for c in ('run', 'last_call_in', 'rutgers_latest', 'dac_latest'):
        h[c] = pd.to_datetime(h[c], utc=True, errors='coerce')
    return h


def streak_start(rows, test):
    """Run time of the first row in the latest unbroken streak of rows where test(row) is true (rows oldest first)."""
    start = None
    for _, r in rows.iterrows():
        start = (start or r['run']) if test(r) else None
    return start


def track(dep, hist, args):
    """From this deployment's history (this run included): when the current DAC / Rutgers lag started, and since when
    each server's latest time has been unchanged."""
    rows = hist[hist['deployment'] == dep['deployment']].sort_values('run').copy()
    # lags from the saved times (not the rounded saved hours), so the trend table matches the main table exactly
    for col, latest in (('rutgers_behind_h', 'rutgers_latest'), ('dac_behind_callin_h', 'dac_latest')):
        rows[col] = (rows['last_call_in'] - rows[latest]).dt.total_seconds() / 3600
    same =lambda col: (lambda r: pd.notna(r[col]) and r[col] == dep[col])
    dep['dac_lag_since'] = streak_start(rows, lambda r: r['dac_behind_callin_h'] > args.dac_watch_hours)
    dep['rutgers_lag_since'] = streak_start(rows, lambda r: r['rutgers_behind_h'] > args.rutgers_watch_hours)
    dep['dac_frozen_since'] = streak_start(rows, same('dac_latest')) if pd.notna(dep['dac_latest']) else None
    dep['rutgers_frozen_since'] = streak_start(rows, same('rutgers_latest')) if pd.notna(dep['rutgers_latest']) else None
    dep['trend'] = rows.tail(TREND_RUNS).iloc[::-1]


def _frozen_for(dep, key, now):
    """How long a server's latest time has been the same across runs (Timedelta), or None (changed this run / no
    earlier run to compare with)."""
    since = dep[f'{key}_frozen_since']
    return None if since is None or since >= now else now - since


def _unchanged(dep, key, now):
    """'unchanged for 2.0 days' when a latest time has been the same over more than one run, else ''."""
    td = _frozen_for(dep, key, now)
    return '' if td is None else f'unchanged for {_lag(td)} (since the {now - td:%Y-%m-%d %H:%M} run)'


def frozen_cells(dep, now, meta):
    """[(text, hours for the color or NaN)] for the RU / DAC unchanged columns. Colored only when the server is more
    than the watch threshold behind the call-in - frozen while the glider is quiet is expected, so left uncolored."""
    out = []
    for key, idx, watch in (('rutgers', 0, meta['rutgers_watch']), ('dac', 1, meta['dac_watch'])):
        td = _frozen_for(dep, key, now)
        behind_h = -diffs(dep)[idx][0].total_seconds() / 3600 if pd.notna(diffs(dep)[idx][0]) else float('nan')
        out.append(('-', float('nan')) if td is None else
                   (_lag(td), _h(td) if behind_h > watch else float('nan')))
    return out


# ---------------------------------------------------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------------------------------------------------

DIFF_HEADER = ['RU - call-in', 'DAC - call-in', 'DAC - RU']


def diffs(r):
    """The three signed differences for a deployment or a history row (negative = the first is older):
    [(Timedelta, hours used for the color)]. The call-in columns are colored by how far behind the call-in the server
    is (at or after the call-in = green); DAC - RU by the size of the gap either way (the servers disagree)."""
    ru, dac = r['rutgers_latest'] - r['last_call_in'], r['dac_latest'] - r['last_call_in']
    dac_ru = r['dac_latest'] - r['rutgers_latest']
    return [(ru, -_h(ru)), (dac, -_h(dac)), (dac_ru, abs(_h(dac_ru)))]


TABLE_HEADER = ['deployment', 'status', 'last call-in', 'Rutgers latest', 'DAC latest', *DIFF_HEADER,
                'RU unchanged', 'DAC unchanged', 'DAC lag since']


def table_row(d, now, meta):
    return [d['deployment'], d['status'].upper() if d['status'] != 'ok' else 'OK', csa._ts(d['last_call_in']),
            csa._ts(d['rutgers_latest']), csa._ts(d['dac_latest']), *(csa._dur(td) for td, _ in diffs(d)),
            *(t for t, _ in frozen_cells(d, now, meta)),
            '-' if d['dac_lag_since'] is None else 'this run' if d['dac_lag_since'] >= now
            else csa._ts(d['dac_lag_since'])]


TREND_HEADER = ['run', 'last call-in', 'Rutgers latest', 'DAC latest', *DIFF_HEADER, 'status']


def trend_rows(d):
    return [[csa._ts(r['run']), csa._ts(r['last_call_in']), csa._ts(r['rutgers_latest']), csa._ts(r['dac_latest']),
             *(csa._dur(td) for td, _ in diffs(r)), str(r['status']).upper()] for _, r in d['trend'].iterrows()]


def render_text(meta, deps):
    n_inv = sum(d['status'] == 'investigate' for d in deps)
    n_watch = sum(d['status'] == 'watch' for d in deps)
    lines = [f'Glider DAC data-flow check - run {meta["now"]:%Y-%m-%d %H:%M} UTC',
             f'  each server vs the glider\'s last call-in (RUCOOL API): Rutgers {RUTGERS}, DAC {DAC}', '']
    lines += [f'!! {a}' for a in meta['alerts']]
    lines.append(f'SUMMARY: {len(deps)} active deployments - {n_inv} to INVESTIGATE, {n_watch} to WATCH, '
                 f'{len(deps) - n_inv - n_watch} OK')
    lines += ['', *csa._text_table(TABLE_HEADER, [table_row(d, meta['now'], meta) for d in deps])]
    for d in deps:
        lines += ['', f'--- {d["deployment"]} - {d["status"].upper()} ' + '-' * max(0, 80 - len(d['deployment']))]
        lines += [f'  [{s.upper():11s}] {m}' for s, m in d['problems']] or ['  data is flowing - nothing flagged']
        for key, label in (('rutgers', 'Rutgers'), ('dac', 'DAC')):
            if (u := _unchanged(d, key, meta['now'])):
                lines.append(f'  {label} latest time {u}')
        if len(d['trend']):
            lines += ['  last runs (newest first):'] + ['    ' + l for l in csa._text_table(TREND_HEADER, trend_rows(d))]
    return '\n'.join(lines)


LAG_CSS = """
.lag { display:inline-block; min-width:72px; text-align:center; border-radius:6px; padding:1px 8px; font-weight:600; }
.lag-ok { background:var(--ok-bg); color:var(--ok); }
.lag-watch { background:var(--warn-bg); color:var(--warn); }
.lag-bad { background:var(--bad); color:var(--on-solid); }
"""


def _lag_cell(e, text, hours, watch_h, inv_h):
    """A difference table cell as a color block: green up to watch_h, orange up to inv_h, red beyond."""
    if pd.isna(hours):
        return f'<td>{e(text)}</td>'
    cls = 'lag-bad' if hours > inv_h else 'lag-watch' if hours > watch_h else 'lag-ok'
    return f'<td><span class="lag {cls}">{e(text)}</span></td>'


def _diff_cells(e, row, first, ds, meta):
    """{column index: colored cell} for the three difference columns starting at index `first`."""
    limits = [(meta['rutgers_watch'], meta['rutgers_inv']), (meta['dac_watch'], meta['dac_inv']),
              (meta['dac_watch'], meta['dac_inv'])]
    return {first + k: _lag_cell(e, row[first + k], hours, *limits[k]) for k, (_, hours) in enumerate(ds)}


def render_html(meta, deps):
    e = html.escape
    n_inv = sum(d['status'] == 'investigate' for d in deps)
    n_watch = sum(d['status'] == 'watch' for d in deps)
    n_ok = len(deps) - n_inv - n_watch
    out = ['<!doctype html><html lang="en"><head><meta charset="utf-8">',
           '<meta name="viewport" content="width=device-width, initial-scale=1">',
           '<title>Glider DAC Data Flow</title>', f'<style>{csa.HTML_CSS}{LAG_CSS}</style></head><body><main>',
           '<h1>Glider data flow to the IOOS DAC</h1>',
           f'<p class="sub">Run {meta["now"]:%Y-%m-%d %H:%M} UTC &middot; {len(deps)} active deployments</p>',
           '<div class="cards">',
           f'<div class="card {"bad" if n_inv else "ok"}"><div class="n">{n_inv}</div><div class="label">to investigate</div></div>',
           f'<div class="card {"warn" if n_watch else "ok"}"><div class="n">{n_watch}</div><div class="label">to watch</div></div>',
           f'<div class="card ok"><div class="n">{n_ok}</div><div class="label">flowing OK</div></div></div>']
    out += [f'<div class="alert">{e(a)}</div>' for a in meta['alerts']]

    out.append('<section><h2>What is measured</h2><p class="intro">Each server&rsquo;s latest data is measured on its '
               'own against the glider&rsquo;s last call-in (<a href="https://marine.rutgers.edu/cool/data/gliders/">'
               'RUCOOL deployments API</a>, last surfacing connect time): the Rutgers ERDDAP <a href="' + e(RUTGERS) +
               '">' + e(RUTGERS) + '</a> and the IOOS Glider DAC <a href="' + e(DAC) + '">' + e(DAC) + '</a>. The DAC '
               'is not fed from the Rutgers ERDDAP, so either one can be ahead of the other. Latest times are what each '
               'ERDDAP advertises (allDatasets maxTime). Rutgers\' is the newer of profile-sci-rt and '
               'trajectory-raw-rt, because the DAC dataset carries flight data as well as science (science can be a '
               'dive behind while the last science file comes in).</p>')
    out.append('<details><summary>How to read this report</summary><dl class="legend">')
    out.append(f'<dt><span class="badge sev-investigate">Investigate</span></dt><dd>Not on Rutgers or the DAC; Rutgers '
               f'more than {meta["rutgers_inv"]:g} h or the DAC more than {meta["dac_inv"]:g} h behind the glider\'s '
               f'last call-in.</dd>')
    out.append(f'<dt><span class="badge sev-watch">Watch</span></dt><dd>Rutgers {meta["rutgers_watch"]:g}&ndash;'
               f'{meta["rutgers_inv"]:g} h or the DAC {meta["dac_watch"]:g}&ndash;{meta["dac_inv"]:g} h behind the '
               f'call-in; missing while the deployment is less than {NEW_DEPLOYMENT_HOURS} h old.</dd>')
    out.append('<dt>RU - call-in<br>DAC - call-in</dt><dd>The server&rsquo;s latest data time minus the glider&rsquo;s '
               'last call-in. Negative = the newest data the server has is that much older than the call-in. 0.0 h or '
               'positive = it has data up to (or past) the call-in; positive means the API&rsquo;s call-in record is '
               'the one behind.</dd>')
    out.append('<dt>DAC - RU</dt><dd>The DAC&rsquo;s latest data time minus Rutgers&rsquo;. Positive = the DAC has newer '
               'data than the Rutgers ERDDAP; negative = older. For reference only: the two call-in columns show which '
               'server is the slow one and set the status.</dd>')
    band = lambda w, i: (f'<span class="lag lag-ok">green</span> under {w:g} h, <span class="lag lag-watch">orange'
                         f'</span> {w:g}&ndash;{i:g} h, <span class="lag lag-bad">red</span> over {i:g} h')
    out.append(f'<dt>colors</dt><dd>{band(meta["dac_watch"], meta["dac_inv"])}, for every column: RU - call-in and '
               f'DAC - call-in by how far behind the call-in (at/after the call-in = green), DAC - RU by the size of '
               f'the gap either way, RU / DAC unchanged by how long.</dd>')
    out.append(f'<dt><span class="badge sev-info">For reference</span></dt><dd>The glider has not called in for '
               f'{SILENT_HOURS}+ h - nothing new can flow until it does.</dd>')
    out.append('<dt>DAC lag since</dt><dd>The first daily run in the current unbroken streak with the DAC over the '
               'watch threshold behind the call-in - how long the lag has been going on.</dd>')
    out.append('<dt>RU / DAC unchanged</dt><dd>How long that server&rsquo;s latest time has stayed the same across the '
               'saved runs (frozen); &ldquo;-&rdquo; = it changed since the last run, or there is no earlier run yet. '
               'Colored (same hours) only when the server is also more than the watch threshold behind the call-in, '
               'i.e. the glider has sent data the server hasn&rsquo;t got; frozen while the glider is quiet is '
               'expected and left uncolored.</dd></dl></details></section>')

    out.append('<section><h2>Active deployments</h2><div class="scroll"><table><thead><tr>')
    out += [f'<th>{e(h)}</th>' for h in TABLE_HEADER]
    out.append('</tr></thead><tbody>')
    for d in deps:
        r = table_row(d, meta['now'], meta)
        badge = {'investigate': 'sev-investigate', 'watch': 'sev-watch', 'ok': 'ok'}[d['status']]
        cells = [f'<td>{e(r[0])}</td>', f'<td><span class="badge {badge}">{e(r[1])}</span></td>']
        lags = _diff_cells(e, r, 5, diffs(d), meta)
        for k, ((text, hours), (watch, inv)) in enumerate(zip(frozen_cells(d, meta['now'], meta),
                                                             [(meta['rutgers_watch'], meta['rutgers_inv']),
                                                              (meta['dac_watch'], meta['dac_inv'])])):
            lags[8 + k] = _lag_cell(e, text, hours, watch, inv)
        cells += [lags[i] if i in lags else '<td class="none">&ndash;</td>' if (i in (3, 4) and c == '-')
                  else f'<td>{e(c)}</td>' for i, c in enumerate(r[2:], start=2)]
        out.append(f'<tr{" class=bad" if d["status"] == "investigate" else ""}>{"".join(cells)}</tr>')
    out.append('</tbody></table></div></section>')

    for d in deps:
        out.append(f'<section><h2><span class="dep">{e(d["deployment"])}</span></h2><ul class="problems">')
        if d['problems']:
            out += [f'<li><span class="badge sev-{s}">{csa.SEV_LABELS[s]}</span>{e(m)}</li>' for s, m in d['problems']]
        else:
            out.append('<li><span class="badge ok">OK</span>Data is flowing - nothing flagged.</li>')
        for key, label in (('rutgers', 'Rutgers'), ('dac', 'DAC')):
            if (u := _unchanged(d, key, meta['now'])):
                out.append(f'<li>{label} latest time {e(u)}</li>')
        out.append('</ul>')
        if len(d['trend']):
            out.append('<p class="caption">Last runs, newest first</p><div class="scroll"><table><thead><tr>')
            out += [f'<th>{e(h)}</th>' for h in TREND_HEADER]
            out.append('</tr></thead><tbody>')
            for r, (_, h) in zip(trend_rows(d), d['trend'].iterrows()):
                lags = _diff_cells(e, r, 4, diffs(h), meta)
                out.append('<tr>' + ''.join(lags.get(i, f'<td>{e(c)}</td>') for i, c in enumerate(r)) + '</tr>')
            out.append('</tbody></table></div>')
        out.append('</section>')
    out.append('</main></body></html>')
    return '\n'.join(out)


# ---------------------------------------------------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description=__doc__.split('\n\n')[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--gliders', default=None, help='regex to limit deployments, e.g. "ru37|ru34" (not saved to history)')
    p.add_argument('--watch-hours', type=float, default=12,
                   help='Rutgers or DAC this far behind the glider\'s last call-in = WATCH. Default 12')
    p.add_argument('--investigate-hours', type=float, default=24, help='... = INVESTIGATE. Default 24')
    p.add_argument('--save-history', action='store_true', help='add a --gliders run to the history anyway')
    p.add_argument('--no-history', action='store_true', help='don\'t add this run to the history')
    p.add_argument('--html', default=None, help='also write the report as a self-contained HTML page to this path')
    p.add_argument('--archive-dir', default=None, help='also save a dated copy, e.g. reports/dac_flow_20261105_0800.html')
    args = p.parse_args()
    # one set of thresholds for both servers (the per-server names are what the rest of the code reads)
    args.rutgers_watch_hours = args.dac_watch_hours = args.watch_hours
    args.rutgers_investigate_hours = args.dac_investigate_hours = args.investigate_hours

    now = pd.Timestamp.now(tz='UTC').floor('min')
    meta = {'now': now, 'alerts': [], 'dac_watch': args.dac_watch_hours, 'dac_inv': args.dac_investigate_hours,
            'rutgers_watch': args.rutgers_watch_hours, 'rutgers_inv': args.rutgers_investigate_hours}

    try:
        deps = fetch_active()
    except Exception as ex:
        print(f'Could not read the active-deployment list ({csa.ACTIVE_API}): {type(ex).__name__}: {ex}')
        return 1
    if args.gliders:
        deps = [d for d in deps if pd.Series([d['deployment']]).str.match(f'({args.gliders})').iloc[0]]
    if not deps:
        print('No active deployments' + (f' matching {args.gliders}' if args.gliders else '') + '.')
        return 0
    names = [d['deployment'] for d in deps]

    # One request each. If a server can't be read its column is blank and nothing is saved to the history (the
    # deployments would otherwise look missing there).
    read_ok = True
    for key, server, patterns in (('rutgers_latest', RUTGERS, RUTGERS_IDS), ('dac_latest', DAC, ['{}'])):
        try:
            found = latest_times(server, patterns, names)
        except Exception as ex:
            read_ok = False
            found = {}
            meta['alerts'].append(f'could not read {server}: {type(ex).__name__}: {ex} - its column is blank, nothing is '
                                  'flagged as missing there, and this run is not saved to the history')
        for d in deps:
            d[key] = found.get(d['deployment'], pd.NaT)
    for d in deps:
        d['rutgers_behind'] = d['last_call_in'] - d['rutgers_latest']
        d['dac_behind'] = d['last_call_in'] - d['dac_latest']
        d['problems'] = [(s, m) for s, m in check(d, now, args)
                         if read_ok or not m.startswith('not on')]
        sevs = [s for s, _ in d['problems'] if s != 'info']
        d['status'] = min(sevs, key=STATUS_ORDER.get) if sevs else 'ok'

    # History: this run is added, then each deployment's streaks are read back from it
    save = read_ok and not args.no_history and (not args.gliders or args.save_history)
    hist = read_history(meta, save)
    this_run = pd.DataFrame([{'run': now, 'deployment': d['deployment'], 'last_call_in': d['last_call_in'],
                              'rutgers_latest': d['rutgers_latest'], 'dac_latest': d['dac_latest'],
                              'rutgers_behind_h': round(_h(d['rutgers_behind']), 2),
                              'dac_behind_callin_h': round(_h(d['dac_behind']), 2), 'status': d['status']}
                             for d in deps], columns=HISTORY_COLS)
    if save:
        this_run.to_csv(HISTORY_PATH, mode='a', header=not os.path.exists(HISTORY_PATH), index=False,
                        date_format='%Y-%m-%dT%H:%M:%SZ')
    elif read_ok:
        meta['alerts'].append('not added to the history (manual run) - lag tracking uses the daily runs only')
    hist = pd.concat([hist, this_run], ignore_index=True) if len(hist) else this_run
    for d in deps:
        track(d, hist, args)

    deps.sort(key=lambda d: (STATUS_ORDER[d['status']], d['deployment']))
    print(render_text(meta, deps))

    if args.html or args.archive_dir:
        page = render_html(meta, deps)
        paths = [args.html] if args.html else []
        if args.archive_dir:
            os.makedirs(args.archive_dir, exist_ok=True)
            paths.append(os.path.join(args.archive_dir, f'dac_flow_{now:%Y%m%d_%H%M}.html'))
        for path in paths:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(page)
            print(f'Saved HTML report to {path}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
