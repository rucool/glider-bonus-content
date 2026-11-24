#!/usr/bin/env python
r"""
Author: jengdahl 11/16/2026
One-page summary for a recovered glider deployment, as a PDF and a Word (.docx) version: header (project, PIs,
where the data went, length, sensors), a track map, a depth-time section for each sensor variable, and the
Robots4Whales (WHOI DCS) analyst-reviewed whale detections on one map when the glider carried a passive acoustic
monitor.

How to run
----------
Setup (once):
  1. Clone erddapgraph (not pip/conda installable):
       git clone https://github.com/kerfoot/erddapgraph.git /PATH/TO/erddapgraph
  2. A conda environment with: pandas, requests, pyyaml, matplotlib, cartopy, erddapy, python-dateutil,
     python-docx (for the Word version; without it only the PDF is made)

Each run:
  conda activate <your environment>
  export PYTHONPATH=/PATH/TO/erddapgraph                  # Windows PowerShell: $env:PYTHONPATH = "C:\PATH\TO\erddapgraph"
  python recovered_deployment_summary.py DEPLOYMENT [options]

  DEPLOYMENT       deployment name, e.g. ru43-20261111T1711
  -o DIR           folder for the PDF and .docx, created if needed [default: glider_deployment_summary, in the
                   folder you run it from]
  --no-docx        PDF only, no Word version
  --pis "A, B"     PI names to show when the dataset metadata lists none (no Principal Investigator role)
  --no-whales      leave out the whale detection map
  -l LEVEL         log level: debug, info (default), warning, error

Examples:
  python recovered_deployment_summary.py sbu01-20261105T1718
      -> glider_deployment_summary/sbu01-20261105T1718_summary.pdf and _summary.docx
  python recovered_deployment_summary.py maracoos_02-20240124T1445 -o /PATH/TO/glider_deployment_summary

Takes about 1-3 minutes (ERDDAP draws each plot). The deployment should have a profile-sci-rt or -delayed dataset on
the Rutgers ERDDAP; one that is still active works too (length then shows "so far").

Sources:
  - RUCOOL deployments API: project, start/end, distance flown
  - Rutgers ERDDAP (profile-sci-rt, or -delayed once rt is gone): PIs (contributor_role = Principal Investigator),
    sensors (instrument_* variables); the sections are made like the RUCOOL deployment page's (color limits =
    erddapgraph's erddap_plot_vars.yml min/max, else the variable's actual_range); the map and section images are
    ERDDAP Make A Graph PNGs made with erddapgraph (https://github.com/kerfoot/erddapgraph - clone it and put it on
    PYTHONPATH), colormaps from its config/erddap_plot_vars.yml
  - IOOS Glider DAC ERDDAP: delivered to the DAC (dataset exists) and GTS (gts_ingest = True)
  - WHOI DCS page (found through the https://robots4whales.whoi.edu/ index, e.g. rutgers2611_ru43, sbu2611_sbu01):
    daily manual analysis tables -> right/humpback/fin/sei detected / possibly detected positions, one per
    15-minute period (analyst-reviewed only)
"""
import argparse
import html
import io
import logging
import os
import re
import sys
import tempfile
from urllib.parse import quote

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import requests
import yaml

try:
    from erddapgraph.tabledap import TabledapPlotter
    import erddapgraph
except ImportError:
    sys.exit('erddapgraph not found - clone https://github.com/kerfoot/erddapgraph and add it to PYTHONPATH')

API = 'https://marine.rutgers.edu/cool/data/gliders/api/deployments/'
RUTGERS = 'https://slocum-data.marine.rutgers.edu/erddap'
DAC = 'https://gliders.ioos.us/erddap'
DCS = 'https://dcs.whoi.edu'

# Section variables in page order (the first one present per sensor); anything else in erddapgraph's
# erddap_plot_vars.yml is not plotted, to keep it one page.
SECTION_VARS = [('temperature', 'Temperature'), ('salinity', 'Salinity'),
                ('oxygen_concentration', 'Oxygen Concentration'), ('chlorophyll_a', 'Chlorophyll'),
                ('nitrate_concentration', 'Nitrate'), ('downwelling_par_irradiance', 'PAR')]
SPECIES = {'Right whale': '#d62728', 'Humpback whale': '#1f77b4', 'Fin whale': '#2ca02c', 'Sei whale': '#ff7f0e'}
TIMEOUT = 120


def get(url, **kw):
    return requests.get(url, timeout=TIMEOUT, headers={'User-Agent': 'rucool-glider-summary'}, **kw)


# ---------------------------------------------------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------------------------------------------------

def api_info(deployment):
    r = get(f'{API}?deployment={deployment}')
    r.raise_for_status()
    rows = [d for d in r.json().get('data', []) if d.get('deployment_name') == deployment]
    return rows[0] if rows else {}


def erddap_info(server, dataset_id):
    """ERDDAP /info as a DataFrame, or None if the dataset isn't there."""
    r = get(f'{server}/info/{dataset_id}/index.csv')
    if r.status_code != 200:
        return None
    return pd.read_csv(io.StringIO(r.text))


def attr(info, var, name):
    m = info[(info['Row Type'] == 'attribute') & (info['Variable Name'] == var) & (info['Attribute Name'] == name)]
    return None if m.empty else str(m['Value'].iloc[0])


def find_dataset(deployment):
    """Rutgers science profile dataset (rt while it exists, delayed after) and its /info."""
    for mode in ('rt', 'delayed'):
        ds = f'{deployment}-profile-sci-{mode}'
        info = erddap_info(RUTGERS, ds)
        if info is not None:
            return ds, info
    return None, None


def pis(info):
    """PIs = the contributor_name entries whose contributor_role (same position) is Principal Investigator."""
    names = (attr(info, 'NC_GLOBAL', 'contributor_name') or '').split(',')
    roles = (attr(info, 'NC_GLOBAL', 'contributor_role') or '').split(',')
    out = [n.strip() for n, r in zip(names, roles) if 'principal investigator' in r.lower()]
    return ', '.join(out) or 'not listed in the dataset metadata (no Principal Investigator contributor_role)'


def sensors(info):
    v = info.loc[info['Row Type'] == 'variable', 'Variable Name']
    return [s[len('instrument_'):] for s in v if s.startswith('instrument_')]


def delivered_to(deployment, sens, dcs_page):
    out = ['RU ERDDAP']
    for ds in (deployment, f'{deployment}-delayed'):
        info = erddap_info(DAC, ds)
        if info is not None:
            out.append('DAC')
            if (attr(info, 'NC_GLOBAL', 'gts_ingest') or '').lower() == 'true':
                out.append('GTS')
            break
    if dcs_page:
        out.append('Robots4Whales')
    if 'rxlive' in sens:          # acoustic telemetry receiver -> detections go to MATOS
        out.append('MATOS')
    return out


# ---------------------------------------------------------------------------------------------------------------------
# Whale detections (WHOI DCS)
# ---------------------------------------------------------------------------------------------------------------------

R4W = 'https://robots4whales.whoi.edu/'


def _code_month(code):
    """WHOI's 4-digit deployment code -> first of that month. Newer codes are YYMM, older MMYY; the two can't be
    confused because a 2-digit year (23+) is never a valid month."""
    a, b = int(code[:2]), int(code[2:])
    yy, mm = (a, b) if a > 12 else (b, a)
    return pd.Timestamp(year=2000 + yy, month=mm, day=1) if 1 <= mm <= 12 else None


def dcs_page(deployment):
    """The deployment's WHOI DCS page URL, or None. Looked up in the Robots4Whales index (every DCS page,
    e.g. rutgers2611_ru43, sbu2611_sbu01 - the prefix is the operator), taking the page for this glider whose code
    month is closest to our deployment start (within ~6 weeks). If the index can't be read or doesn't list it yet,
    the likely URLs are tried directly."""
    glider, ts = deployment.rsplit('-', 1)
    start = pd.Timestamp(ts[:8])
    try:
        links = set(re.findall(rf'https://dcs\.whoi\.edu/([a-z]+)(\d{{4}})/\1\2_{re.escape(glider)}\.shtml',
                               get(R4W).text))
        best = None
        for prefix, code in links:
            month = _code_month(code)
            if month is not None and abs((month - start.replace(day=1)).days) <= 45:
                d = abs((month - start).days)
                if best is None or d < best[0]:
                    best = (d, f'{DCS}/{prefix}{code}/{prefix}{code}_{glider}.shtml')
        if best:
            return best[1]
    except requests.RequestException as e:
        logging.warning(f'could not read the Robots4Whales index ({e}) - trying page URLs directly')
    prefixes = dict.fromkeys(['rutgers', re.match(r'[a-z]+', glider).group(0)])
    for prefix in prefixes:
        for code in (ts[2:4] + ts[4:6], ts[4:6] + ts[2:4]):
            url = f'{DCS}/{prefix}{code}/{prefix}{code}_{glider}.shtml'
            if get(url).status_code == 200:
                return url
    return None


ROW = re.compile(r'(\d{1,2}/\d{1,2}/\d{2})\s+(\d{1,2}:\d{2}:\d{2})\s+(Detected|Possibly detected)\s+'
                 r'(-?\d+\.\d+)\s+(-?\d+\.\d+)')


def dcs_detections(page_url):
    """All analyst-reviewed detections from the daily tables: species, occurrence, latitude, longitude."""
    base = page_url.rsplit('/', 1)[0]
    links = sorted(set(re.findall(r'href="([^"]*manual_analysis_table_\d+\.html)"', get(page_url).text)))
    rows = []
    for link in links:
        url = link if link.startswith('http') else f'{base}/{link}'
        text = html.unescape(re.sub(r'<[^>]+>', ' ', get(url).text))
        text = text.split('Notes:')[0]                       # the notes section repeats rows without occurrence
        for part in re.split(r'(?=(?:Right|Humpback|Fin|Sei) whale:)', text):
            m = re.match(r'(Right|Humpback|Fin|Sei) whale:', part)
            if not m:
                continue
            for d, t, occ, lat, lon in ROW.findall(part):
                rows.append({'species': f'{m.group(1)} whale', 'occurrence': occ,
                             'latitude': float(lat), 'longitude': float(lon)})
    return pd.DataFrame(rows, columns=['species', 'occurrence', 'latitude', 'longitude'])


# ---------------------------------------------------------------------------------------------------------------------
# Images
# ---------------------------------------------------------------------------------------------------------------------

def plot_vars_config():
    path = os.path.join(os.path.dirname(erddapgraph.__file__), '..', 'config', 'erddap_plot_vars.yml')
    with open(path) as f:
        return yaml.safe_load(f)


def erddap_images(dataset_id, info, img_dir):
    """Track map + one section per SECTION_VARS variable present: [(title, png path)]."""
    plotter = TabledapPlotter(RUTGERS)
    plotter.image_path = img_dir
    plotter.dataset_id = dataset_id
    plotter.image_type = 'largePng'
    cfg = plot_vars_config()
    out = []

    # track map colored by time
    plotter.set_marker_style('Borderless Filled Circle', 4)
    plotter.set_color_bar('Rainbow2')
    plotter.add_constraint('latitude', '!=', 'NaN')
    plotter.add_constraint('longitude', '!=', 'NaN')
    plotter.build_image_request('longitude', 'latitude', 'time')
    p = plotter.download_image(f'{dataset_id}_map.png', clobber=True)
    if p:
        out.append(('Map', p))

    # Sections made the same way as the RUCOOL deployment page's (erddapgraph scripts/rucool/
    # download_dataset_timeseries_plots.py): color limits = the yml's min/max for the variable, else its ERDDAP
    # actual_range; depth_interpolated on the y axis, limited by the yml's zmin/zmax; borderless filled circles, 5.
    y_var = 'depth_interpolated' if 'depth_interpolated' in plotter.dataset_variables else 'depth'
    for var, title in SECTION_VARS:
        if var not in plotter.dataset_variables:
            continue
        opts = cfg.get(var) or {}
        rng = attr(info, var, 'actual_range')
        lo, hi = ([float(x) for x in rng.split(',')] if rng else (None, None))
        plotter.clear_constraints()
        plotter.reset_plot_parameters()
        plotter.image_type = 'largePng'
        plotter.set_marker_style('Borderless Filled Circle', 5)
        plotter.set_marker_color('black')
        plotter.set_line_style('markers')
        plotter.set_color_bar(opts.get('colorbar', 'Rainbow2'), min_value=opts.get('min', lo),
                              max_value=opts.get('max', hi))
        plotter.set_x_range(ascending=True)
        plotter.set_y_range(ascending=False)
        if 'zmin' in opts:
            plotter.add_constraint(y_var, '>=', opts['zmin'])
        if 'zmax' in opts:
            plotter.add_constraint(y_var, '<=', opts['zmax'])
        plotter.add_constraint(var, '!=', 'NaN')
        plotter.build_image_request('time', y_var, var)
        p = plotter.download_image(f'{dataset_id}_{var}_section.png', clobber=True)
        if p:
            out.append((title, p))
    return out


def track(dataset_id):
    """Profile positions for the whale map."""
    url = (f'{RUTGERS}/tabledap/{dataset_id}.csv?latitude,longitude&latitude!=NaN&longitude!=NaN'
           f'&distinct()')
    r = get(url)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text), skiprows=[1])


def whale_map(ax, trk, det):
    """All species on one map: track in grey, detected = filled, possibly detected = open. Each species/occurrence
    group is drawn in order of its number of detections - most first (bottom), fewest last (top) - so rare
    detections aren't hidden under common ones. Returns {(species, occurrence): handle}."""
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    pad = 0.08
    ax.set_extent([trk['longitude'].min() - pad, trk['longitude'].max() + pad,
                   trk['latitude'].min() - pad, trk['latitude'].max() + pad], crs=ccrs.PlateCarree())
    ax.add_feature(cfeature.LAND.with_scale('10m'), facecolor='#d9d9d9', edgecolor='#808080', linewidth=0.5)
    ax.plot(trk['longitude'], trk['latitude'], '.', color='#9e9e9e', markersize=1.2, transform=ccrs.PlateCarree(),
            zorder=2)
    groups = det.groupby(['species', 'occurrence']).size().sort_values(ascending=False)   # most -> fewest
    handles = {}
    for z, ((sp, occ), n) in enumerate(groups.items()):
        color = SPECIES[sp]
        d = det[(det['species'] == sp) & (det['occurrence'] == occ)]
        handles[(sp, occ)] = ax.scatter(d['longitude'], d['latitude'], s=22,
                                        facecolors=color if occ == 'Detected' else 'none', edgecolors=color,
                                        linewidths=1.1, transform=ccrs.PlateCarree(), zorder=3 + z)
    from matplotlib.ticker import MaxNLocator
    gl = ax.gridlines(draw_labels=True, linewidth=0.3, color='#bdbdbd')
    gl.top_labels = gl.right_labels = False
    gl.xlocator = MaxNLocator(4)           # narrow tracks otherwise get longitude labels running into each other
    gl.xlabel_style = gl.ylabel_style = {'size': 7}
    ax.set_rasterized(True)        # the 10m coastline as vectors makes the PDF several MB
    if not handles:
        ax.text(0.5, 0.5, 'no detections', transform=ax.transAxes, ha='center', fontsize=9)
    return handles


def whale_legend(fig, x, y, det, handles):
    """Legend to the right of the whale map: a block per species (in SPECIES order) with its detected / possibly
    detected counts, then how to read it."""
    from matplotlib.lines import Line2D

    def amount(n):
        """15-minute periods -> '327 periods (82 h)' / '1 period (15 min)' / 'none'."""
        if not n:
            return 'none'
        span = f'{n * 15} min' if n < 4 else f'{n * 0.25:.0f} h' if n >= 40 else f'{n * 0.25:.1f} h'
        return f'{n} period{"s" if n != 1 else ""} ({span})'

    fig.text(x, y, 'Whale detections', fontsize=10, fontweight='bold', va='top')
    fig.text(x, y - 0.02, '15-minute periods with calls heard', fontsize=7.5, va='top', color='#4d4d4d')
    y -= 0.045
    for sp, color in SPECIES.items():
        counts = {occ: int(((det['species'] == sp) & (det['occurrence'] == occ)).sum())
                  for occ in ('Detected', 'Possibly detected')}
        fig.text(x, y, sp, fontsize=9, fontweight='bold', va='top', color=color if sum(counts.values()) else '#9e9e9e')
        y -= 0.019
        for occ, face in (('Detected', color), ('Possibly detected', 'none')):
            ax = fig.add_axes([x, y - 0.012, 0.02, 0.012])
            ax.axis('off')
            ax.add_line(Line2D([0.5], [0.5], marker='o', markersize=6, markerfacecolor=face, markeredgecolor=color,
                               linestyle='none'))
            fig.text(x + 0.025, y, f'{"detected" if occ == "Detected" else "possibly"}: {amount(counts[occ])}',
                     fontsize=8, va='top', color='black' if counts[occ] else '#9e9e9e')
            y -= 0.017
        y -= 0.006
    fig.text(x, y - 0.004, 'Analyst-reviewed (Robots4Whales).\nCounts are periods, not the\nnumber of whales or calls.\n'
             'Grey = glider track; fewest\nperiods drawn on top.', fontsize=7, va='top', color='#4d4d4d',
             linespacing=1.3)


# ---------------------------------------------------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------------------------------------------------

LINK = '#1a5fb4'
X0, X1 = 0.06, 0.94          # page text margins (figure fraction)


def _write(fig, x, y, text, **kw):
    """Draw text at (x, y) and return the x where it ends (figure fraction)."""
    t = fig.text(x, y, text, va='top', **kw)
    bb = t.get_window_extent(renderer=fig.canvas.get_renderer())
    return x + bb.width / fig.bbox.width


def header_line(fig, y, label, parts, x0=X0, x1=X1, size=9):
    """'Label: value' between x0 and x1, the value wrapped under itself; parts = [(text, url or None)], drawn in
    sequence (comma-separated pieces can be links). Returns the y below the line."""
    x = _write(fig, x0, y, f'{label}: ', fontsize=size, fontweight='bold')
    xv, line_h = x, 0.0165
    # word by word so long values wrap; the ', ' between parts is plain text, not part of a link
    seq = []
    for i, (text, url) in enumerate(parts):
        words = text.split(' ')
        seq += [(w + (' ' if j < len(words) - 1 else ''), url) for j, w in enumerate(words)]
        if i < len(parts) - 1:
            seq.append((', ', None))
    for w, url in seq:
        t = fig.text(x, y, w, va='top', fontsize=size, color=LINK if url else 'black', url=url)
        wpx = t.get_window_extent(renderer=fig.canvas.get_renderer()).width / fig.bbox.width
        if x + wpx > x1 and x > xv:              # wrap onto a new line under the value
            t.remove()
            y -= line_h
            x = xv
            t = fig.text(x, y, w, va='top', fontsize=size, color=LINK if url else 'black', url=url)
        x += wpx
    return y - line_h - 0.003


def _trim(img, pad=4):
    """Crop the white margin around an ERDDAP image so it fills its panel."""
    rgb = img[..., :3]
    if rgb.max() > 1:
        rgb = rgb / 255.0
    rows, cols = (rgb.min(axis=2) < 0.97).nonzero()
    if not len(rows):
        return img
    return img[max(rows.min() - pad, 0):rows.max() + pad + 1, max(cols.min() - pad, 0):cols.max() + pad + 1]


def make_pdf(path, header, panels, whale, whale_png=None):
    """Draw the page and save it as a PDF to path; with whale_png, also save the whale section (map + legend) as an
    image for the Word version. header: [(label, [(text, url)])]; panels: [(title, png)]; whale: (track, detections)
    or None."""
    import cartopy.crs as ccrs
    fig = plt.figure(figsize=(8.5, 11))
    # Header in two columns: the four short lines on the left, the two long ones (PIs, delivered to) in a wider
    # right column; anything after that, e.g. a whale-data note, runs full width underneath.
    mid = 0.42
    yl = yr = 0.97
    for label, parts in header[:4]:
        yl = header_line(fig, yl, label, parts, X0, mid - 0.02)
    for label, parts in header[4:6]:
        yr = header_line(fig, yr, label, parts, mid, X1)
    y = min(yl, yr)
    for label, parts in header[6:]:
        y = header_line(fig, y, label, parts)

    # ERDDAP panels in a grid under the header. With a whale map: 3 across in the upper half, and the whale map gets
    # the bottom half on its own with its legend to the right. Without one: 2 across, filling the page.
    grid_top = y - 0.012
    ncols = 3 if whale else 2
    nrows = max(1, -(-len(panels) // ncols))
    grid_bottom = 0.405 if whale else 0.02
    grid = fig.add_gridspec(nrows, ncols, left=X0 - 0.03, right=X1 + 0.03, top=grid_top, bottom=grid_bottom,
                            hspace=0.12, wspace=0.02)
    for i, (title, png) in enumerate(panels):
        ax = fig.add_subplot(grid[i // ncols, i % ncols])
        ax.imshow(_trim(plt.imread(png)))
        ax.set_title(title, fontsize=9, pad=2)
        ax.axis('off')
    if whale:
        trk, det = whale
        fig.add_artist(plt.Line2D([X0, X1], [0.392, 0.392], color='#bdbdbd', linewidth=0.6))
        ax = fig.add_axes([X0, 0.03, 0.60, 0.33], projection=ccrs.Mercator())
        handles = whale_map(ax, trk, det)
        ax.set_title('Whale Detections', fontsize=10)
        whale_legend(fig, X0 + 0.64, 0.37, det, handles)
    fig.savefig(path, dpi=200)
    if whale and whale_png:      # the bottom of the page below the divider line, page margins cropped off
        from matplotlib.transforms import Bbox
        fig.savefig(whale_png, dpi=200, bbox_inches=Bbox.from_extents((X0 - 0.03) * 8.5, 0, X1 * 8.5, 0.385 * 11))
    plt.close(fig)


def _docx_link(paragraph, text, url):
    """A clickable link in a Word paragraph (python-docx has no direct call for this)."""
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    from docx.opc.constants import RELATIONSHIP_TYPE
    r_id = paragraph.part.relate_to(url, RELATIONSHIP_TYPE.HYPERLINK, is_external=True)
    link = OxmlElement('w:hyperlink')
    link.set(qn('r:id'), r_id)
    run = OxmlElement('w:r')
    props = OxmlElement('w:rPr')
    for tag, val in (('w:color', '1A5FB4'), ('w:u', 'single')):
        el = OxmlElement(tag)
        el.set(qn('w:val'), val)
        props.append(el)
    run.append(props)
    t = OxmlElement('w:t')
    t.text = text
    t.set(qn('xml:space'), 'preserve')
    run.append(t)
    link.append(run)
    paragraph._p.append(link)


def make_docx(path, header, panels, whale_png, img_dir):
    """Word version of the page, one page: the header as editable text in two columns (with links), the ERDDAP
    plots in a table, the whale section (map + legend) as one image, and an empty Deployment Notes box to type in."""
    from docx import Document
    from docx.enum.table import WD_ROW_HEIGHT_RULE
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.shared import Inches, Pt
    doc = Document()
    sec = doc.sections[0]
    sec.page_width, sec.page_height = Inches(8.5), Inches(11)
    sec.left_margin = sec.right_margin = Inches(0.5)
    sec.top_margin = sec.bottom_margin = Inches(0.4)
    doc.styles['Normal'].font.name = 'Calibri'
    doc.styles['Normal'].font.size = Pt(10)

    def tight(par, after=0):
        par.paragraph_format.space_before = Pt(0)
        par.paragraph_format.space_after = Pt(after)
        par.paragraph_format.line_spacing = 1.0
        return par

    def header_par(par, label, parts):
        tight(par, after=2)
        par.add_run(f'{label}: ').bold = True
        for i, (text, url) in enumerate(parts):
            if url:
                _docx_link(par, text, url)
            else:
                par.add_run(text)
            if i < len(parts) - 1:
                par.add_run(', ')

    # Header in two columns like the PDF: the four short lines left, the two long ones right; extras full width
    ht = doc.add_table(rows=1, cols=2)
    for cell, w in zip(ht.rows[0].cells, (3.0, 4.5)):
        cell.width = Inches(w)
    for col, items in ((0, header[:4]), (1, header[4:6])):
        cell = ht.cell(0, col)
        for k, (label, parts) in enumerate(items):
            header_par(cell.paragraphs[0] if k == 0 else cell.add_paragraph(), label, parts)
    for label, parts in header[6:]:
        header_par(doc.add_paragraph(), label, parts)
    tight(doc.add_paragraph())

    # 3 plots across, sized so the page (with an empty notes box) stays one page in Word
    ncols = 3
    width = Inches(2.05 if whale_png else 2.4)
    table = doc.add_table(rows=-(-len(panels) // ncols), cols=ncols)
    for i, (title, png) in enumerate(panels):
        trimmed = os.path.join(img_dir, f'trim_{i}.png')
        plt.imsave(trimmed, _trim(plt.imread(png)))
        cell = table.cell(i // ncols, i % ncols)
        cp = tight(cell.paragraphs[0])
        cp.alignment = WD_ALIGN_PARAGRAPH.CENTER
        cp.add_run(title).bold = True
        pp = tight(cell.add_paragraph())
        pp.alignment = WD_ALIGN_PARAGRAPH.CENTER
        pp.add_run().add_picture(trimmed, width=width)
    if whale_png:
        wp = tight(doc.add_paragraph())
        wp.alignment = WD_ALIGN_PARAGRAPH.CENTER
        wp.add_run().add_picture(whale_png, width=Inches(5.4))

    # Deployment notes: a bordered box to type in (grows as text is added)
    np_ = tight(doc.add_paragraph(), after=2)
    np_.paragraph_format.keep_with_next = True
    np_.add_run('Deployment Notes:').bold = True
    notes = doc.add_table(rows=1, cols=1)
    notes.style = 'Table Grid'
    notes.rows[0].height = Inches(1.0)
    notes.rows[0].height_rule = WD_ROW_HEIGHT_RULE.AT_LEAST
    # Word always ends a document with an empty line after a table; make it 1 pt so it doesn't spill onto page 2
    from docx.enum.text import WD_LINE_SPACING
    end = tight(doc.add_paragraph())
    end.paragraph_format.line_spacing_rule = WD_LINE_SPACING.EXACTLY
    end.paragraph_format.line_spacing = Pt(1)
    doc.save(path)


def main(args):
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=getattr(logging, args.loglevel.upper()))
    dep = args.deployment
    os.makedirs(args.outputdir, exist_ok=True)

    api = api_info(dep)
    dataset_id, info = find_dataset(dep)
    if dataset_id is None:
        logging.error(f'no {dep}-profile-sci-rt or -delayed dataset on {RUTGERS}')
        return 1
    logging.info(f'using {dataset_id}')

    sens = sensors(info)
    page = dcs_page(dep) if 'dmon' in sens else None
    start = pd.Timestamp(api['start_date_epoch'], unit='s') if api.get('start_date_epoch') else None
    end = pd.Timestamp(api['end_date_epoch'], unit='s') if api.get('end_date_epoch') else None
    if start is not None and end is not None:
        days = f'{(end - start).total_seconds() / 86400:.1f} days'
    elif start is not None:      # no end date yet: not marked recovered
        days = f'{(pd.Timestamp.utcnow().tz_localize(None) - start).total_seconds() / 86400:.1f} days so far ' \
               '(not marked recovered yet)'
    else:
        days = '-'
    km = f', {api["distance_flown_km"]:.0f} km' if api.get('distance_flown_km') else ''
    # order matters: the first four go in the left header column, the next two (long) in the right one
    header = [('Deployment', [(dep, f'{RUTGERS}/tabledap/{dataset_id}.html')]),
              ('Project', [(api.get('project_name') or attr(info, 'NC_GLOBAL', 'project') or '-', None)]),
              ('Length', [(days + km, None)]),
              ('Sensors', [(', '.join(sens) or '-', None)]),
              ('PIs/Project Managers', [(args.pis or pis(info), None)]),
              ('Data Delivered To', [(d, page if d == 'Robots4Whales' else None)
                                     for d in delivered_to(dep, sens, page)])]
    if 'dmon' in sens and not page:
        logging.warning('dmon on board but no Robots4Whales page found for this deployment')
        header.append(('Whale Detections', [('dmon on board, but no Robots4Whales page found for this deployment '
                                             'yet - WHOI may not have processed it yet', None)]))

    whale = None
    if page and not args.no_whales:
        det = dcs_detections(page)
        logging.info(f'{len(det)} whale detections from {page}')
        whale = (track(dataset_id), det)

    # the ERDDAP images are only needed to build the page - downloaded to a temporary folder, removed afterwards
    pdf = os.path.join(args.outputdir, f'{dep}_summary.pdf')
    docx_path = pdf.replace('.pdf', '.docx')
    with tempfile.TemporaryDirectory() as img_dir:
        panels = erddap_images(dataset_id, info, img_dir)
        whale_png = os.path.join(img_dir, 'whale.png') if whale else None
        make_pdf(pdf, header, panels, whale, whale_png)
        print(pdf)
        if not args.no_docx:
            try:
                make_docx(docx_path, header, panels, whale_png, img_dir)
                print(docx_path)
            except ImportError:      # the PDF is the main product - don't fail the run over the Word copy
                logging.warning('python-docx is not installed - no Word version made '
                                '(conda install -c conda-forge python-docx)')
    return 0


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('deployment', help='deployment name, e.g. ru43-20261111T1711')
    p.add_argument('-o', '--outputdir', default='glider_deployment_summary',
                   help='folder for the PDF and Word versions (<deployment>_summary.pdf / .docx); created if needed. '
                        'Default: glider_deployment_summary')
    p.add_argument('--no-docx', action='store_true', help='make the PDF only, no Word version')
    p.add_argument('--pis', default=None,
                   help='PI names to show, e.g. "Name One, Name Two" - for when the dataset metadata has no '
                        'Principal Investigator contributor_role (otherwise taken from contributor_name/role)')
    p.add_argument('--no-whales', action='store_true',
                   help='leave out the whale detection map (Robots4Whales still listed under Data Delivered To)')
    p.add_argument('-l', '--loglevel', default='info', choices=['debug', 'info', 'warning', 'error'])
    sys.exit(main(p.parse_args()))
