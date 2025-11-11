#!/usr/bin/env python

"""
Author: lnazzaro 10/28/2025
Last modified: lnazzaro 10/28/2025
Create deployments map and summary statistics, limited by project and/or glider name if interested
"""

import requests
from erddapy import ERDDAP
import pandas as pd
import numpy as np
import argparse
import os
import sys
import cool_maps.plot as cplt
from cool_maps.download import get_bathymetry
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import cmocean as cmo

def main(args):
    projects = args.projects
    gliders = args.gliders
    if projects:
        projects = args.projects.split(',')
    if gliders:
        gliders = args.gliders.split(',')
    extent = [args.west_bound, args.east_bound, args.south_bound, args.north_bound]
    extent_inset = [args.inset_west_bound, args.inset_east_bound, args.inset_south_bound, args.inset_north_bound]
    t0 = pd.to_datetime(args.min_time)
    if args.max_time:
        t1 = pd.to_datetime(args.max_time)
    else:
        t1 = pd.to_datetime('now')+pd.Timedelta(hours=24)
    lo = args.logical_operator.lower()
    ask = args.ask
    csv_file = args.stats
    map_file = args.map
    plot_bathy = args.plot_bathymetry
    plot_bathy_inset = plot_bathy
    bathy_file = args.bathymetry_file
    bathy_file_inset = args.inset_bathymetry_file
    bathy_type = args.bathymetry_type
    projection = args.projection
    if projection:
        try:
            projclass = getattr(ccrs, projection)
            proj = {'map': projclass(), 'data': ccrs.PlateCarree()}
            proj_inset = proj.copy()
        except:
            print(f'Projection {projection} not recognized. Using default.')
            projection = None
    if not projection:
        if extent[1]-extent[0]<90 and extent[3]-extent[2]<90:
            proj = {'map': ccrs.Mercator(), 'data': ccrs.PlateCarree()}
        else:
            proj = {'map': ccrs.Robinson(), 'data': ccrs.PlateCarree()}
        if all(extent_inset) and extent_inset[1]-extent_inset[0]<90 and extent_inset[3]-extent_inset[2]<90:
            proj_inset = {'map': ccrs.Mercator(), 'data': ccrs.PlateCarree()}
        else:
            proj_inset = {'map': ccrs.Robinson(), 'data': ccrs.PlateCarree()}

    ru_gliders = ['maracoos_02', 'maracoos_04', 'maracoos_05', 'ru25d', 'ru26d']
    for ru in range(100):
        ru_gliders.append('ru'+str(ru).zfill(2))

    glider_api = 'https://marine.rutgers.edu/cool/data/gliders/api/'
    deployment_list = requests.get(f'{glider_api}deployments/').json()['data']
    glider_deployments_api = []
    deployment_year =[]
    project = []
    length = []
    distance = []
    glider = []
    internal = []
    for ad in deployment_list:
        if projects and gliders and lo=='or':
            if ad['project_name'] not in projects and ad['glider_name'] not in gliders:
                continue
        else:
            if projects and ad['project_name'] not in projects:
                continue
            if gliders and ad['glider_name'] not in gliders:
                continue
        if ask:
            confirm = input(f'Include deployment {ad["deployment_name"]}? ')
            if confirm and confirm[0].lower()=='n':
                continue
        if pd.to_datetime(ad['start_date_epoch'], unit='s')<t0 or pd.to_datetime(ad['start_date_epoch'], unit='s')>t1:
            continue
        print(f'Including deployment {ad["deployment_name"]}')
        glider_deployments_api.append(ad['deployment_name'])
        deployment_year.append(ad['start_year'])
        project.append(ad['project_name'])
        glider.append(ad['glider_name'])
        distance.append(ad['distance_flown_km'])
        if ad['glider_name'] in ru_gliders:
            internal.append(True)
        else:
            internal.append(False)
        if not ad['end_date_epoch']:
            length.append('ongoing')
        else:
            length.append((ad['end_date_epoch']-ad['start_date_epoch'])/60/60/24)

    deployment_info = pd.DataFrame({'deployment_name': glider_deployments_api, 'year': deployment_year, 
                                    'glider': glider, 'ru_glider': internal, 'project': project, 'nDays': length,
                                    'distance_km': distance, 'nProfiles': np.nan, 'mode': ''})

    ru_erddap = ERDDAP(server='http://slocum-data.marine.rutgers.edu/erddap', protocol='tabledap')

    ru_dataset_list = list(pd.read_csv(ru_erddap.get_search_url(response='csv'))['Dataset ID'])

    ru_erddap.constraints = {}
    ru_erddap.variables = ['profile_time']

    if map_file:
        state_lines = cfeature.NaturalEarthFeature(
            category='cultural',
            name='admin_1_states_provinces_lines',
            scale='50m',
            facecolor='none'
            )
        LAND = cfeature.NaturalEarthFeature('physical', 'land', '10m')
        fig, ax = plt.subplots(
                figsize=(11,8), #12,9
                subplot_kw=dict(projection=proj['map'])
            )
        ax.set_extent(extent)
        if all(extent_inset):
            fig_inset, ax_inset = plt.subplots(
                    figsize=(11,8), #12,9
                    subplot_kw=dict(projection=proj_inset['map'])
                )
            ax_inset.set_extent(extent_inset)
        elif any(extent_inset):
            print('All bounds (east, west, north, south) for inset must be provided for one to be generated. Skipping inset.')

        if plot_bathy:
            if all(extent_inset):
                bathy_inset = None
                if bathy_file_inset and os.path.isfile(bathy_file_inset):
                    bathy_inset = get_bathymetry(extent_inset, file=bathy_file_inset)
                else:
                    try:
                        bathy_inset = get_bathymetry(extent_inset)
                    except:
                        print('Unable to read bathymetry for inset, trying full domain bathy')
                        if not os.path.isfile(bathy_file):
                            plot_bathy_inset = False
            if bathy_file and os.path.isfile(bathy_file):
                bathy = get_bathymetry(extent, file=bathy_file)
                if all(extent_inset) and not bathy_inset:
                    try:
                        bathy_inset = get_bathymetry(extent_inset, file=bathy_file)
                    except:
                        plot_bathy_inset = False
            else:
                try:
                    bathy = get_bathymetry(extent)
                except:
                    print('Unable to read bathymetry')
                    plot_bathy = False
            vlim = None
            lons, lats = np.meshgrid(bathy['longitude'].data.copy(), bathy['latitude'].data.copy())
            elevation = bathy['z'].data.copy()
            elevation[np.abs(elevation)<1] = 0
            elevation[elevation>0] = np.log10(elevation[elevation>0])
            elevation[elevation<0] = -np.log10(np.abs(elevation[elevation<0]))
            vlim = np.nanquantile(np.abs(elevation), 0.975)
            if bathy_type=='blues':
                elevation[elevation>0] = np.nan
                h = ax.pcolormesh(lons, lats, elevation, cmap=plt.cm.Blues_r, vmin=-vlim, vmax=0, transform=proj['data'], zorder=5)
            elif bathy_type=='topo':
                h = ax.pcolormesh(lons, lats, elevation, cmap=cmo.cm.topo, vmin=-vlim, vmax=vlim, transform=proj['data'], zorder=5)
            if all(extent_inset):
                lons, lats = np.meshgrid(bathy_inset['longitude'].data.copy(), bathy_inset['latitude'].data.copy())
                elevation = bathy_inset['z'].data.copy()
                elevation[np.abs(elevation)<1] = 0
                elevation[elevation>0] = np.log10(elevation[elevation>0])
                elevation[elevation<0] = -np.log10(np.abs(elevation[elevation<0]))
                if not vlim:
                    vlim = np.nanquantile(np.abs(elevation), 0.975)
                if bathy_type=='blues':
                    elevation[elevation>0] = np.nan
                    hinset = ax_inset.pcolormesh(lons, lats, elevation, cmap=plt.cm.Blues_r, vmin=-vlim, vmax=0, transform=proj_inset['data'], zorder=5)
                elif bathy_type=='topo':
                    hinset = ax_inset.pcolormesh(lons, lats, elevation, cmap=cmo.cm.topo, vmin=-vlim, vmax=vlim, transform=proj_inset['data'], zorder=5)
            # cplt.add_features(ax,oceancolor='none', coast='high')
            # cplt.add_bathymetry(ax, bathy['longitude'].data, bathy['latitude'].data, bathy['z'].data,method='blues_log',levels=(-100,-50,-20), zorder=5)
        if not plot_bathy:
            ax.set_facecolor(cfeature.COLORS['water'])
            if all(extent_inset):
                ax_inset.set_facecolor(cfeature.COLORS['water'])
        if not plot_bathy or bathy_type=='blues':
            ax.add_feature(LAND, edgecolor='black', facecolor='tan', zorder=10)
            ax.add_feature(cfeature.RIVERS, zorder=10.2)
            ax.add_feature(cfeature.LAKES, zorder=10.2)
            ax.add_feature(cfeature.BORDERS, zorder=10.3)
            if extent[1]-extent[0]<60 and extent[3]-extent[2]<45:
                ax.add_feature(state_lines, edgecolor='gray', zorder=10.25)
            if all(extent_inset):
                ax_inset.add_feature(LAND, edgecolor='black', facecolor='tan', zorder=10)
                ax_inset.add_feature(cfeature.RIVERS, zorder=10.2)
                ax_inset.add_feature(cfeature.LAKES, zorder=10.2)
                ax_inset.add_feature(cfeature.BORDERS, zorder=10.3)
                if extent_inset[1]-extent_inset[0]<60 and extent_inset[3]-extent_inset[2]<45:
                    ax_inset.add_feature(state_lines, edgecolor='gray', zorder=10.25)
        try:
            cplt.add_ticks(ax, extent, gridlines=True)
        except:
            print('skipping ticks and gridlines')
        if all(extent_inset):
            ax.plot([extent_inset[0], extent_inset[0], extent_inset[1], extent_inset[1], extent_inset[0]], 
                    [extent_inset[2], extent_inset[3], extent_inset[3], extent_inset[2], extent_inset[2]], 
                    c='white', lw=5, transform=proj['data'], zorder=45)
            ax.plot([extent_inset[0], extent_inset[0], extent_inset[1], extent_inset[1], extent_inset[0]], 
                    [extent_inset[2], extent_inset[3], extent_inset[3], extent_inset[2], extent_inset[2]], 
                    c='black', lw=2, transform=proj['data'], zorder=48)
            try:
                cplt.add_ticks(ax_inset, extent_inset, gridlines=True)
            except:
                print('skipping inset ticks and gridlines')

    for n in range(len(deployment_info)):
        dep = deployment_info['deployment_name'][n]
        print(f'Plotting and counting profiles for {dep}')
        if csv_file:
            datasetid = None
            if f'{dep}-profile-sci-delayed' in ru_dataset_list:
                datasetid = f'{dep}-profile-sci-delayed'
                deployment_info['mode'][n] = 'delayed'
            elif f'{dep}-profile-sci-rt' in ru_dataset_list:
                datasetid = f'{dep}-profile-sci-rt'
                deployment_info['mode'][n] = 'rt'
            if datasetid:
                ru_erddap.dataset_id = datasetid
                protimes = ru_erddap.to_pandas(distinct=True)
                protimes['time'] = pd.to_datetime(protimes['profile_time (UTC)'])
                deployment_info['nProfiles'][n] = len(np.unique(protimes['time']))
        if map_file:
            deployment_track = np.vstack(requests.get(f'{glider_api}tracks/?deployment={dep}').json()['features'][0]['geometry']['coordinates'])
            ax.plot(deployment_track[:,0], deployment_track[:,1], c='red', lw=2, transform=proj['data'], zorder=50)
            if all(extent_inset):
                ax_inset.plot(deployment_track[:,0], deployment_track[:,1], c='red', lw=2, transform=proj_inset['data'], zorder=50)

    if csv_file:
        deployment_info.to_csv(csv_file, index=False)
    if map_file:
        fig.savefig(map_file, dpi=300, bbox_inches='tight')
        if extent_inset:
            mapdir, mapfile = os.path.split(map_file)
            mapfilename, mapfileext = os.path.splitext(mapfile)
            fig_inset.savefig(os.path.join(mapdir, f'{mapfilename}_inset{mapfileext}'), dpi=300, bbox_inches='tight')
        plt.close('all')

    return

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('-p', '--projects',
                            help='projects to include, as provided by acronym on https://marine.rutgers.edu/cool/data/gliders/configure/?type=projects; enclose in double quotes and separate by comma without following space ie "Caribbean PAM,Challenger"; default: all projects',
                            default=None,
                            type=str)
    
    arg_parser.add_argument('-g', '--gliders',
                            help='gliders to include, separated by comma no space; default: all gliders',
                            default=None,
                            type=str)
    
    arg_parser.add_argument('-lo', '--logical_operator',
                            help='logical operator used to match on project and glider, AND or OR; AND requires both glider and project to match search terms, OR will include all gliders provided and all projects provided regardless of overlap; default AND',
                            default='AND',
                            choices=['AND', 'OR'],
                            type=str)
    
    arg_parser.add_argument('-a', '--ask',
                            help='ask whether to include each deployment; default False',
                            default=False,
                            type=bool)
    
    arg_parser.add_argument('-csv', '--stats',
                            help='file to write stats info (deployment length, number profiles, etc); default None (do not write any file)',
                            default=None,
                            type=str)
    
    arg_parser.add_argument('-m', '--map',
                            help='file to write map; default None (do not print map)',
                            default=None,
                            type=str)
    
    arg_parser.add_argument('-proj', '--projection',
                            help='projection to use for map as listed at https://scitools.org.uk/cartopy/docs/v0.15/crs/projections.html; default Robinson when any axis size exceeds 90deg, otherwise Mercator',
                            default=None,
                            type=str)
    
    arg_parser.add_argument('-w', '--west_bound',
                            help='western boundary for map; default -180',
                            default=-180,
                            type=float)
    
    arg_parser.add_argument('-e', '--east_bound',
                            help='eastern boundary for map; default 180',
                            default=180,
                            type=float)
    
    arg_parser.add_argument('-s', '--south_bound',
                            help='southern boundary for map; default -90',
                            default=-90,
                            type=float)
    
    arg_parser.add_argument('-n', '--north_bound',
                            help='northern boundary for map; default 90',
                            default=90,
                            type=float)
    
    arg_parser.add_argument('-iw', '--inset_west_bound',
                            help='western boundary for inset map; default None (no inset)',
                            default=None,
                            type=float)
    
    arg_parser.add_argument('-ie', '--inset_east_bound',
                            help='eastern boundary for inset map; default None (no inset)',
                            default=None,
                            type=float)
    
    arg_parser.add_argument('-is', '--inset_south_bound',
                            help='southern boundary for inset map; default None (no inset)',
                            default=None,
                            type=float)
    
    arg_parser.add_argument('-in', '--inset_north_bound',
                            help='northern boundary for inset map; default None (no inset)',
                            default=None,
                            type=float)
    
    arg_parser.add_argument('-t0', '--min_time',
                            help='earliest START time to include, yyyy-mm-ddTHH:MM; default None',
                            default='1970-01-01',
                            type=str)
    
    arg_parser.add_argument('-t1', '--max_time',
                            help='latest START time to include, yyyy-mm-ddTHH:MM; default None',
                            default=None,
                            type=str)
    
    arg_parser.add_argument('-bf', '--bathymetry_file',
                            help='name of bathymetry file downloaded from GMRT; default None (reads from ERDDAP)',
                            default=None,
                            type=str)
    
    arg_parser.add_argument('-bfi', '--inset_bathymetry_file',
                            help='name of bathymetry file for inset downloaded from GMRT; default None (reads from ERDDAP)',
                            default=None,
                            type=str)
    
    arg_parser.add_argument('-bt', '--bathymetry_type',
                            help='type of bathymetry to plot (blues for shaded blues with tan land, topo for topography); default: blues',
                            choices=['blues', 'topo'],
                            default='blues',
                            type=str)
    
    arg_parser.add_argument('-b', '--plot_bathymetry',
                            help='whether to plot bathymetry; default True',
                            default=True,
                            type=bool)
    
    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))