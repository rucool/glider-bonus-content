#!/usr/bin/env python

"""
Author: lnazzaro 5/6/2025
Last modified: lnazzaro 5/6/2025
Get sensors included in deployments from RU ERDDAP
"""

import requests
from erddapy import ERDDAP
import pandas as pd
import numpy as np
import argparse
import os
import sys

def main(args):
    cfile = args.categories
    fname = args.output_file

    ru_gliders = ['maracoos_02', 'maracoos_04', 'maracoos_05', 'ru25d', 'ru26d']
    for ru in range(100):
        ru_gliders.append('ru'+str(ru).zfill(2))

    glider_api = 'https://marine.rutgers.edu/cool/data/gliders/api/'
    deployment_list = requests.get(f'{glider_api}deployments/').json()['data']
    glider_deployments_api = []
    deployment_year =[]
    project = []
    length = []
    glider = []
    internal = []
    for ad in deployment_list:
        glider_deployments_api.append(ad['deployment_name'])
        deployment_year.append(ad['start_year'])
        project.append(ad['project_name'])
        glider.append(ad['glider_name'])
        if ad['glider_name'] in ru_gliders:
            internal.append(True)
        else:
            internal.append(False)
        if not ad['end_date_epoch']:
            length.append('ongoing')
        else:
            length.append((ad['end_date_epoch']-ad['start_date_epoch'])/60/60/24)

    deployment_info = pd.DataFrame({'deployment_name': glider_deployments_api, 'year': deployment_year, 
                                    'glider': glider, 'ru_glider': internal, 'project': project, 'nDays': length})
    
    sensor_categories = pd.read_csv(cfile)
    for c in np.unique(sensor_categories['category']):
        deployment_info[c] = 0

    ru_erddap = ERDDAP(server='http://slocum-data.marine.rutgers.edu/erddap', protocol='tabledap')

    ru_dataset_list = list(pd.read_csv(ru_erddap.get_search_url(response='csv'))['Dataset ID'])

    for n in range(len(deployment_info)):
        dep = deployment_info['deployment_name'][n]
        print(f'{dep} ({n+1}/{len(deployment_info)})')
        try:
            dep_datasets = list(pd.read_csv(ru_erddap.get_search_url(search_for=dep, response='csv'))['Dataset ID'])
        except:
            continue
        ru_erddap.dataset_id = dep_datasets[0]
        all_info = pd.read_csv(ru_erddap.get_info_url(response='csv'))
        all_vars = list(np.unique(all_info['Variable Name']))
        all_sensors = [i for i in all_vars if i.startswith('instrument_')]
        for s in all_sensors:
            c = sensor_categories['category'][sensor_categories['sensor']==s]
            deployment_info.loc[deployment_info['deployment_name']==dep, c]+=1

    if os.path.isdir(os.path.split(fname)[0]):
        deployment_info.to_csv(fname, index=False)
    else:
        print(f'Unable to write detailed info to {fname}, directory does not exist.')

    return

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('-c', '--categories',
                            help='file containing sensor categories',
                            default=os.path.join(os.getcwd(),'files','sensor_categories.csv'))
    
    arg_parser.add_argument('-o', '--output_file',
                            help='output file.',
                            default=os.path.join(os.getcwd(),'files','ru_glider_sensors.csv'))
    
    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))