#!/usr/bin/env python

"""
Author: lnazzaro 7/26/2024
Last modified: lnazzaro 7/27/2024
Search glider deployment notes

There are 8 deployments that the notes reader/API fails on for some reason:
ru24-20120118T1748, ru07-20111214T1711, ru10-20111210T1200, ru06-20110920T1646, 
silbo-20110623T1215, ru23-20101104T1440, ru10-20101010T1730, ru23-20101001T1850
"""

import requests
from erddapy import ERDDAP
import pandas as pd
import numpy as np
import re
import sys
import warnings
warnings.simplefilter("ignore")


search_terms = input('Hey there! What are we looking for?\n')
search_method = input('How do you want to search? (any words/all words/exact phrase, default: exact phrase): ')
if 'word' in search_method.lower():
    search_terms = re.split(',| |, ', search_terms)

# limit deployments by:
# deployment_name
deployments = input('List specific deployments to search, separated by spaces (default: all): ')
deployments = re.split(',| |, ', deployments)
# glider_name
gliders = input('List specific gliders to search, separated by spaces (default: all): ')
gliders = re.split(',| |, ', gliders)
# start_date_epoch
t0 = input('List earliest time to search (YYYY-mm-dd HH:MM, default: beginning of time): ')
# end_date_epoch
t1 = input('List latest time to search (YYYY-mm-dd HH:MM, default: present): ')
# project_name
projects = input('List projects to search, separated by spaces (default: all): ')
projects = re.split(',| |, ', projects)

# limit notes by:
# category_name
categories = ['Informational', 'Alert', 'Action Needed', 'Action Taken', 'Operations']
category = input('Search all notes categories? (y/n, default: y): ')
if category.lower() in ['n', 'no', 'false']:
    for c in categories:
        csearch = input('Search {c} notes? (y/n, default: y): ')
        if csearch.lower() in ['n', 'no', 'false']:
            categories.remove(c)
# added_by
authors = input('List usernames of note authors to search, separated by spaces (default: all): ')
authors = re.split(',| |, ', authors)


glider_api = 'https://marine.rutgers.edu/cool/data/gliders/api/'
deployment_list = pd.DataFrame(requests.get(f'{glider_api}deployments/').json()['data'])

deployment_list['search'] = False

if deployments[0]:
    deployment_list['search'][deployment_list['deployment_name'].isin(deployments)] = True
if gliders[0]:
    deployment_list['search'][deployment_list['glider_name'].isin(gliders)] = True
if projects[0]:
    deployment_list['search'][deployment_list['project_name'].isin(projects)] = True
if not deployment_list['search'].any():
    deployment_list['search'] = True
if t0:
    deployment_list['search'][deployment_list['end_date_epoch']<pd.Timestamp(t0).timestamp()] = False
if t1:
    deployment_list['search'][deployment_list['start_date_epoch']>pd.Timestamp(t1).timestamp()] = False


print('\n\nSearch Results:\n\n')

for deployment in deployment_list['deployment_name'][deployment_list['search']]:
    try:
        deployment_notes = pd.DataFrame(requests.get(f'{glider_api}notes/?deployment={deployment}').json()['data'])
    except:
        print(f'\n******* Unable to access deployment notes for {deployment} *******\n')
        continue
    if len(deployment_notes)==0:
        continue
    deployment_notes['search'] = False

    if authors[0]:
        deployment_notes['search'][deployment_notes['added_by'].isin(authors)] = True
    if categories[0]:
        deployment_notes['search'][deployment_notes['category_name'].isin(categories)] = True
    
    if not deployment_notes['search'].any():
        continue

    deployment_notes = deployment_notes[deployment_notes['search']]
    s=False

    # search in: pilot_notes
    for n in range(len(deployment_notes)):
        printnote = True
        if search_terms:
            if type(search_terms) is not list:
                search_terms = [search_terms]
            printnote = False
            for w in search_terms:
                if w.lower() in deployment_notes['pilot_notes'][n].lower():
                    printnote = True
                elif 'all' in search_method.lower():
                    printnote = False
                    break
        if printnote:
            if not s:
                print(f'\n******* Search Results for Deployment {deployment} *******\n')
                s=True
            print(f"** {deployment_notes['category_name'][n]} note from {deployment_notes['added_by'][n]} on {deployment_notes['date_added'][n]}:\n")
            print(f"{deployment_notes['pilot_notes'][n]}\n")


sys.exit()