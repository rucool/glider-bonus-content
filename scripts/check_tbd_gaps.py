#!/usr/bin/env python

"""
Author: lnazzaro 5/27/2024
Last modified: lnazzaro 5/27/2024
Check for data gaps in real-time deployments
"""

import sys
import argparse
import os
import glob
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from erddapy import ERDDAP
warnings.simplefilter("ignore")

def main(args):
    if type(args.check_tbds) is str and args.check_tbds.lower() in ['f', 'false']:
        args.check_tbds = False
    elif type(args.check_tbds) is str and args.check_tbds.lower() in ['t', 'true']:
        args.check_tbds = True
    if type(args.available_tbds_only) is str and args.available_tbds_only.lower() in ['f', 'false']:
        args.available_tbds_only = False
    elif type(args.available_tbds_only) is str and args.available_tbds_only.lower() in ['t', 'True']:
        args.available_tbds_only = True
    if type(args.slocum_dir) is str and args.slocum_dir.lower()=='none':
        args.slocum_dir = None
    for deployment in args.deployments:
        glider = '-'.join(deployment.split('-')[:-1])
        deployment_time = deployment.split('-')[-1]
        deployment_year = deployment_time[:4]   
        if args.check_tbds:
            if not args.slocum_dir:
                print(f'Data directory not provided, unable to check tbd availability for {deployment}.')
                args.check_tbds=False
            else:
                deployment_directory = os.path.join(args.slocum_dir, 'deployments', deployment_year, deployment)
                if not os.path.isdir(deployment_directory):
                    print(f'Deployment directory {deployment_directory} does not exist, check deployment name and/or data directory provided. Will not check for tbd availability.')
                    args.check_tbds=False

        try:
            ru_erddap = ERDDAP(server='http://slocum-data.marine.rutgers.edu/erddap', protocol='tabledap')
            ru_erddap.dataset_id = f'{deployment}-trajectory-raw-rt'

            ru_erddap.variables = ['source_file']
            segment_info = ru_erddap.to_pandas(distinct=True)
            segment_info['t0'] = np.nan
            segment_info['t1'] = np.nan
            segment_info['tLength'] = np.nan
            segment_info['nDepth'] = np.nan
            segment_info['nTemp'] = np.nan

            ru_erddap.variables = ['time', 'depth', 'sci_water_temp']

            for f in range(len(segment_info)):
                # print(f'{f}/{len(segment_info)}')
                ru_erddap.constraints = {'source_file=': segment_info['source_file'][f]}
                segment_data = ru_erddap.to_xarray()
                segment_info['t0'][f] = pd.to_datetime(min(segment_data['time'].data))
                segment_info['t1'][f] = pd.to_datetime(max(segment_data['time'].data))
                segment_info['tLength'][f] = (segment_info['t1'][f]-segment_info['t0'][f]).total_seconds()/60/60
                segment_info['nDepth'][f] = np.sum(np.logical_and(segment_data['depth'].data!=0, ~np.isnan(segment_data['depth'].data)))
                segment_info['nTemp'][f] = np.sum(np.logical_and(segment_data['sci_water_temp'].data!=0, ~np.isnan(segment_data['sci_water_temp'].data)))
        except:
            print(f'Issue reading from dataset {deployment}-trajectory-raw-rt using erddapy')
            continue

        try:
            segment_info = segment_info.sort_values(by='t0', ignore_index=True)
            bad_segments = segment_info[np.logical_and(segment_info['nTemp']==0, segment_info['tLength']>1)].copy().reset_index(drop=True)
            gap_times = pd.DataFrame()
            gap_times['t0'] = bad_segments['t0'][np.append(0, np.where(np.diff(bad_segments['t0'])>pd.Timedelta(hours=12))[0]+1)].copy().reset_index(drop=True)
            gap_times['t1'] = bad_segments['t1'][np.append(np.where(np.diff(bad_segments['t0'])>pd.Timedelta(hours=12))[0], len(bad_segments)-1)].copy().reset_index(drop=True)
            t_lag = (pd.to_datetime(datetime.now(timezone.utc)).replace(tzinfo=None)-segment_info['t1'][len(segment_info)-1]).total_seconds()/60/60

            print(f'*****  {deployment} data status:\n')

            if t_lag < args.max_lag and len(gap_times)==0:
                print('No data gap issues noted.\n')

            if t_lag > args.max_lag:
                print(f"Latest data {segment_info['t1'][len(segment_info)-1].strftime('%Y-%m-%dT%H:%M')} ({round(t_lag,1)} hours)\n")

            for i in range(len(gap_times)):
                print(f"Gap from {gap_times['t0'][i].strftime('%Y-%m-%dT%H:%M')} to {gap_times['t1'][i].strftime('%Y-%m-%dT%H:%M')}")
                print('Includes segments')
                if args.check_tbds:
                    for sf in segment_info['source_file'][np.logical_and(segment_info['t0']>=gap_times['t0'][i], segment_info['t0']<gap_times['t1'][i])]:
                        tbd = glob.glob(os.path.join(deployment_directory, 'data', 'in', 'binary', 'tbd', '-'.join(sf.split('-')[:5])+'.*'))
                        if len(tbd)==0:
                            tbd = glob.glob(os.path.join(deployment_directory, 'data', 'in', 'binary', 'tbd', sf.split('(')[-1][:-1]+'.*'))
                        if len(tbd)>0:
                            print(f'{sf} (tbd YES)')
                        elif not args.available_tbds_only:
                            print(f'{sf} (no tbd)')
                else:
                    print('\n'.join(list(segment_info['source_file'][np.logical_and(segment_info['t0']>=gap_times['t0'][i], segment_info['t0']<gap_times['t1'][i])])))
                print('\n')
        except:
            print(f'Issue getting data gap information for {deployment}.')
            continue
    
    return


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('deployments',
                            nargs='+',
                            help='Glider deployment name(s) formatted as glider-YYYYmmddTHHMM')
    
    arg_parser.add_argument('-l', '--max_lag',
                            help='longest data lag (hours since latest data) to allow before triggering email',
                            default=6)
    
    arg_parser.add_argument('-d', '--slocum_dir',
                            help='base directory containing slocum data (only used if checking to see if tbds are available)',
                            default=None)
    
    arg_parser.add_argument('-tbd', '--check_tbds',
                            help='whether to look in directory provided (SLOCUM_DIR/deployments/yyyy/deployment/data/in/binary/tbd) for tbd files',
                            default=True)
    
    arg_parser.add_argument('-ta', '--available_tbds_only',
                            help='whether to only return segments found in tbd directory provided (SLOCUM_DIR/deployments/yyyy/deployment/data/in/binary/tbd) (segments without a tbd file identified not listed in output if False)',
                            default=False)
    
    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))