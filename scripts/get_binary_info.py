#!/usr/bin/env python

"""
Author: lnazzaro 8/18/2024
Last modified: lnazzaro 8/22/2024
Check binary files for relevance to a specific deployment
"""

import requests
import pandas as pd
import numpy as np
import os
import argparse
import sys

def main(args):
    #deployment = 'ru01-20120617T1449'
    #binary_list_file = f'/Users/nazzaro/Downloads/{deployment}_binary_open_times.txt'
    t0_warn = args.start_time_warning
    t1_warn = args.end_time_warning
    tgap_warn = args.gap_warning
    slocumdir = args.slocum_dir
    binary_list_file = args.binary_info_file

    glider_api = 'https://marine.rutgers.edu/cool/data/gliders/api/'

    sci_types = ['EBD', 'TBD', 'ebd', 'tbd']

    for deployment in args.deployments:
        print(f'\nchecking files for deployment: {deployment}')

        deployment_time = deployment.split('-')[-1]
        if not binary_list_file:
            binary_list_file = os.path.join(slocumdir, deployment_time[:4], deployment, 'data', 'in', 'binary', f'{deployment}_binary_open_times.txt')

        if not os.path.isfile(binary_list_file):
            print(f'{binary_list_file} not found, skipping deployment.\n')
            continue

        deployment_info = requests.get(f'{glider_api}deployments/?deployment={deployment}').json()['data'][0]

        t0 = pd.to_datetime(deployment_info['start_date_epoch'], unit='s')
        t1 = pd.to_datetime(deployment_info['end_date_epoch'], unit='s')
        osversion = deployment_info['os']

        print(f"deployed {t0.strftime('%Y-%m-%d %H:%M')}")
        print(f"recovered {t1.strftime('%Y-%m-%d %H:%M')}")

        binary_list = pd.read_csv(binary_list_file, header=None)
        binary_list.columns = ['line']
        binary_list['time'] = None
        binary_list['filetype'] = None
        binary_list['directory'] = None
        binary_list['filename'] = None
        cwd = os.getcwd()

        for i in binary_list.index:
            fullfile, t = binary_list['line'][i].replace(' ','').split(':fileopen_time:')
            binary_list.loc[i,'time'] = pd.to_datetime(t, format='%a_%b_%d_%H:%M:%S_%Y')
            datadir, file = os.path.split(fullfile)
            if datadir=='' or datadir=='.':
                datadir = cwd
            elif datadir[:2]=='./':
                datadir = cwd+datadir[1:]
            binary_list.loc[i,'directory'] = datadir
            binary_list.loc[i,'filename'] = file
            binary_list.loc[i,'filetype'] = file.split('.')[-1]

        filetypes = np.unique(binary_list['filetype'])
        if not any(x in sci_types for x in filetypes) and osversion>=7:
            print(f'Warning: os version={osversion} but no dbds or tbds found. Science data logging updated in v7.0.')

        for ftype in filetypes:
            files = binary_list[binary_list['filetype']==ftype].sort_values(by='time', ignore_index=True)
            dirs = pd.DataFrame({'directory': np.unique(files['directory']), 'count': np.nan})
            for i in dirs.index:
                dirs.loc[i,'count'] = sum(files['directory']==dirs['directory'][i])
            longest_list = list(files['filename'][files['directory']==dirs['directory'][np.where(dirs['count']==np.max(dirs['count']))[0][0]]])
            full_list = list(np.unique(files['filename'][np.logical_and(pd.to_datetime(files['time'])>=pd.to_datetime(t0), pd.to_datetime(files['time'])<=pd.to_datetime(t1))]))

            if set(full_list).issubset(longest_list):
                files = files[files['directory']==dirs['directory'][np.where(dirs['count']==np.max(dirs['count']))[0][0]]].sort_values(by='time', ignore_index=True)

            tgaps = np.diff(pd.to_datetime(files['time']).astype('int64')//1e9)/60/60
            dt0 = (pd.to_datetime(files['time']).astype('int64')//1e9 - pd.to_datetime(t0).value//1e9)/60/60
            dt1 = (pd.to_datetime(t1).value//1e9 - pd.to_datetime(files['time']).astype('int64')//1e9)/60/60

            print(f'\nfiletype: {ftype}')

            if len(np.unique(files['directory']))==1:
                print(f'{len(np.unique(files["filename"]))} files in: {files["directory"][0]}')
            else:
                print(f'{len(np.unique(files["filename"]))} files split between: {", ".join(np.unique(binary_list["directory"]))}')

            if (dt0<-t0_warn).any():
                print(f'Warning: these files include times {np.round(-np.min(dt0),2)} hours before deployment start time.')
            if not np.logical_and(dt0>0, dt0<tgap_warn).any():
                print(f'Warning: no files found within {tgap_warn} hours of deployment start time.')
            if (dt1<-t1_warn).any():
                print(f'Warning: these files include times {np.round(-np.min(dt1),2)} hours after deployment end time.')
            if not np.logical_and(dt1>0, dt1<tgap_warn).any():
                print(f'Warning: no files found within {tgap_warn} hours of deployment end time.')
            if (tgaps>tgap_warn).any():
                print(f'Warning: these files include {np.sum(tgaps>tgap_warn)} gaps over {tgap_warn} hours.')

        print('\n')

    return

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('deployments',
                            nargs='+',
                            help='Glider deployment name(s) formatted as glider-YYYYmmddTHHMM')
    
    arg_parser.add_argument('-d', '--slocum_dir',
                            help='base directory containing slocum data',
                            default=None)
    
    arg_parser.add_argument('-f', '--binary_info_file',
                            help='file containing list of files and fileopen_time (defaults to /SLOCUM_DIR/deployments/YYYY/DEPLOYMENT_NAME/data/in/binary/DEPLOYMENT_NAME_binary_open_times.txt)',
                            default=None)

    arg_parser.add_argument('-gw', '--gap_warning',
                            help='warn if gap between binary file times greater than this (hours)',
                            default=12)
    
    arg_parser.add_argument('-sw', '--start_time_warning',
                            help='warn if files exist earlier than start time of deployment minus this number (hours)',
                            default=2)
    
    arg_parser.add_argument('-ew', '--end_time_warning',
                            help='warn if files exist later than end time of deployment plus this number (hours)',
                            default=2)
    
    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))