#!/usr/bin/env python

"""
Author: lgarzio 8/2/2024
Last modified: lgarzio 8/5/2024
Reformat the older $HOME/deployments/ directory structure with current structure
1. add ngdac-profile directory to config
2. copy the correct sensor_defs.json files to ../config/ngdac-profile, ../config/raw-trajectory and ../config/sci-profile
3. make ngdac-profile data directory (if not already there)
4. make sure the rt and delayed mode sci-profile and ngdac-profile data directories have a qc_queue directory
"""

import os
import shutil

# modify these ****************************
years = [2022]
parent_dir = '/directory_structure/slocum'
# end modify ******************************

ddir = os.path.join(parent_dir, 'deployments')
template_dir = os.path.join(parent_dir, 'deployment-template')
for year in years:
    year_dir = os.path.join(ddir, str(year))
    if os.path.isdir(year_dir):
        deployment_list = os.listdir(year_dir)
        for deployment in deployment_list:
            deployment_dir = os.path.join(year_dir, deployment)
            if os.path.isdir(deployment_dir):
                # add to the config directory
                configdir = os.path.join(deployment_dir, 'config')
                if os.path.isdir(configdir):
                    # add ngdac-profile directory if not already there
                    ngdac_configdir = os.path.join(configdir, 'ngdac-profile')
                    if not os.path.isdir(ngdac_configdir):
                        os.makedirs(ngdac_configdir)

                    # copy correct sensor_defs.json files to the appropriate config directory
                    config_templates = os.path.join(template_dir, 'config')
                    cdirs = ['ngdac-profile', 'sci-profile', 'raw-trajectory']
                    for cdir in cdirs:
                        source = os.path.join(config_templates, cdir, 'sensor_defs.json')
                        target = os.path.join(configdir, cdir, 'sensor_defs.json')
                        shutil.copyfile(source, target)

                # make sure data directories are formatted correctly
                datadir = os.path.join(deployment_dir, 'data', 'out', 'nc')
                if os.path.isdir(datadir):
                    # add ngdac-profile directory if not already there
                    dest = os.path.join(datadir, 'ngdac-profile')
                    if not os.path.isdir(dest):
                        ngdac_templatedir = os.path.join(template_dir, 'data/out/nc/ngdac-profile')
                        shutil.copytree(ngdac_templatedir, dest)

                    # add qc_queue directories if not already there
                    cdirs = ['ngdac-profile', 'sci-profile']
                    modes = ['rt', 'delayed']
                    for cdir in cdirs:
                        for mode in modes:
                            qcdir = os.path.join(datadir, cdir, mode, 'qc_queue')
                            if not os.path.isdir(qcdir):
                                os.makedirs(qcdir)
