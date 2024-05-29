#!/bin/bash
# written by L Nazzaro on May 28 2024

# Source the global bashrc
if [ -f /etc/bashrc ]; then
. /etc/bashrc
fi

# Source the local bashrc
if [ -f ~/.bashrc ]; then
. ~/.bashrc
fi

EXECDIR=/PATH/TO/REPO 
EMAIL='email1@domain.edu, email2@domain.com, etc@domain.ext'

conda activate glider-bonus-content

TODAY=`date +%Y-%m-%d`
YEAR=`date +%Y`
YM=`date +%Y%m`
YMD=`date +%Y%m%d`
COMPTIME=`date`

status_info=$(cat <<EOF

Status of data flow for active gliders (as of ${COMPTIME}):

$(python ${EXECDIR}/scripts/check_tbd_gaps.py -d SLOCUM_DIR DEPLOYMENT1_NAME DEPLOYMENT2_NAME DEPLOYMENTN_NAME)


EOF
)

mail -s "Status of Glider Data Flows" $EMAIL << EOF
$status_info
EOF
