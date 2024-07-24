#!/usr/bin/env python

"""
Author: lnazzaro 7/24/2024
Last modified: lnazzaro 7/24/2024
Check deployments in RU ERDDAP and DAC
"""

import requests
from erddapy import ERDDAP
import pandas as pd
import numpy as np
import argparse
import os
import sys

def main(args):
    fname = args.status_file

    ru_gliders = ['maracoos_02', 'maracoos_04', 'maracoos_05']
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

    deployment_status = pd.DataFrame({'deployment_name': glider_deployments_api, 'year': deployment_year, 
                                    'glider': glider, 'ru_glider': internal, 'project': project, 'nDays': length,
                                    'ru_rt': False, 'ru_delayed': False, 'dac_rt': False, 'dac_delayed': False,
                                    'dac_alternate_name': '', 'notes': ''})

    ru_erddap = ERDDAP(server='http://slocum-data.marine.rutgers.edu/erddap', protocol='tabledap')
    dac_erddap = ERDDAP(server='https://gliders.ioos.us/erddap', protocol='tabledap')

    ru_dataset_list = list(pd.read_csv(ru_erddap.get_search_url(response='csv'))['Dataset ID'])
    dac_dataset_list = list(pd.read_csv(dac_erddap.get_search_url(response='csv'))['Dataset ID'])

    for n in range(len(deployment_status)):
        dep = deployment_status['deployment_name'][n]
        #print(f'{dep} ({n+1}/{len(deployment_status)})')
        if f'{dep}-profile-sci-rt' in ru_dataset_list:
            deployment_status.loc[n,'ru_rt'] = True
        if f'{dep}-profile-sci-delayed' in ru_dataset_list:
            deployment_status.loc[n,'ru_delayed'] = True
        if f'{dep}-delayed' in dac_dataset_list:
            deployment_status.loc[n,'dac_delayed'] = True
        if dep in dac_dataset_list:
            deployment_status.loc[n,'dac_rt'] = True

    deployment_status['ru_either'] = deployment_status[['ru_rt', 'ru_delayed']].max(axis=1)
    deployment_status['dac_either'] = deployment_status[['dac_rt', 'dac_delayed']].max(axis=1)

    if os.path.isfile(fname):
        deployment_status_old = pd.read_csv(fname)
        new_deployments = pd.DataFrame(columns=['deployment_name', 'ru_rt', 'ru_delayed', 'ru_either', 'dac_rt', 'dac_delayed', 'dac_either'])

        for n in range(len(deployment_status)):
            dep = deployment_status['deployment_name'][n]
            if dep not in list(deployment_status_old['deployment_name']):
                add_deployment = pd.DataFrame({'deployment_name': dep,
                                            'ru_rt': int(deployment_status['ru_rt'][n]),
                                            'ru_delayed': int(deployment_status['ru_delayed'][n]),
                                            'ru_either': int(deployment_status['ru_either'][n]),
                                            'dac_rt': int(deployment_status['dac_rt'][n]),
                                            'dac_delayed': int(deployment_status['dac_delayed'][n]),
                                            'dac_either': int(deployment_status['dac_either'][n])},
                                            index = [0])
                new_deployments = pd.concat((new_deployments, add_deployment), ignore_index=True)
                continue
            ni = list(np.where(deployment_status_old['deployment_name']==dep)[0])[0]
            if type(deployment_status_old['dac_alternate_name'][ni])==str:
                deployment_status.loc[n,'dac_either'] = True
                deployment_status.loc[n,'dac_alternate_name'] = deployment_status_old['dac_alternate_name'][ni]
            if type(deployment_status_old['notes'][ni])==str:
                deployment_status.loc[n,'notes'] = deployment_status_old['notes'][ni]
            add_deployment = pd.DataFrame({'deployment_name': dep,
                                        'ru_rt': int(deployment_status['ru_rt'][n]) - int(deployment_status_old['ru_rt'][ni]),
                                        'ru_delayed': int(deployment_status['ru_delayed'][n]) - int(deployment_status_old['ru_delayed'][ni]),
                                        'ru_either': int(deployment_status['ru_either'][n]) - int(deployment_status_old['ru_either'][ni]),
                                        'dac_rt': int(deployment_status['dac_rt'][n]) - int(deployment_status_old['dac_rt'][ni]),
                                        'dac_delayed': int(deployment_status['dac_delayed'][n]) - int(deployment_status_old['dac_delayed'][ni]),
                                        'dac_either': int(deployment_status['dac_either'][n]) - int(deployment_status_old['dac_either'][ni])},
                                        index = [0])
            if add_deployment[['ru_rt','ru_delayed','ru_either','dac_rt','dac_delayed','dac_either']].values.any()>0:
                new_deployments = pd.concat((new_deployments, add_deployment), ignore_index=True)

        for c in ['ru_rt','ru_delayed','ru_either','dac_rt','dac_delayed','dac_either']:
            new_deployments[c][new_deployments[c]<0]=0

    print(f"{sum(deployment_status['dac_either'])}/{len(deployment_status)} datasets on the DAC ({sum(deployment_status['dac_delayed'])} delayed-mode)\n")

    print(f"{sum(deployment_status['dac_either'][deployment_status['ru_glider']])}/{sum(deployment_status['ru_glider'])} Rutgers datasets on the DAC ({sum(deployment_status['dac_delayed'][deployment_status['ru_glider']])} delayed-mode)\n")

    if os.path.isfile(fname):
        dac_new = new_deployments[new_deployments['dac_either']==1]
        if len(dac_new)>0:
            print(f"{len(dac_new)} new deployments on DAC:")
            for x in dac_new.index:
                if dac_new['dac_rt'][x] and dac_new['dac_delayed'][x]:
                    print(f"{dac_new['deployment_name'][x]} (delayed and real-time)")
                elif dac_new['dac_delayed'][x]:
                    print(f"{dac_new['deployment_name'][x]} (delayed)")
                elif dac_new['dac_rt'][x]:
                    print(f"{dac_new['deployment_name'][x]} (real-time)")
            print('\n')
        dac_new = new_deployments[np.logical_and(new_deployments['dac_either']==0, new_deployments['dac_delayed']==1)]
        if len(dac_new)>0:
            print(f"{len(dac_new)} delayed-mode datasets added to DAC:")
            for x in dac_new['deployment_name']:
                print(x)
            print('\n')

    print(f"{sum(deployment_status['ru_either'])}/{len(deployment_status)} datasets on the DAC ({sum(deployment_status['ru_delayed'])} delayed-mode)\n")

    print(f"{sum(deployment_status['ru_either'][deployment_status['ru_glider']])}/{sum(deployment_status['ru_glider'])} Rutgers datasets on the DAC ({sum(deployment_status['ru_delayed'][deployment_status['ru_glider']])} delayed-mode)\n")

    if os.path.isfile(fname):
        ru_new = new_deployments[new_deployments['ru_either']==1]
        if len(ru_new)>0:
            print(f"{len(ru_new)} new deployments on RU ERDDAP:")
            for x in ru_new.index:
                if ru_new['ru_rt'][x] and ru_new['ru_delayed'][x]:
                    print(f"{ru_new['deployment_name'][x]} (delayed and real-time)")
                elif ru_new['ru_delayed'][x]:
                    print(f"{ru_new['deployment_name'][x]} (delayed)")
                elif ru_new['ru_rt'][x]:
                    print(f"{ru_new['deployment_name'][x]} (real-time)")
            print('\n')
        ru_new = new_deployments[np.logical_and(new_deployments['ru_either']==0, new_deployments['ru_delayed']==1)]
        if len(ru_new)>0:
            print(f"{len(ru_new)} delayed-mode datasets added to RU ERDDAP:")
            for x in ru_new['deployment_name']:
                print(x)
            print('\n')
    
    print(f"Note these statistics include {sum(deployment_status['notes']=='failed deployment')} failed deployments.")

    if os.path.isdir(os.path.split(fname)[0]):
        deployment_status.to_csv(fname, index=False)
        print(f'More detailed info available in {fname}.')
    else:
        print(f'Unable to write detailed info to {fname}, directory does not exist.')

    return

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('-f', '--status_file',
                            help='file containing detailed status information. overwritten by this script.',
                            default=os.path.join(os.getcwd(),'files','glider_deployment_data_status.csv'))
    
    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))