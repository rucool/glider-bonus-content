#!/bin/bash
# written by L Nazzaro on July 24 2024

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

Glider deployment data delivery status (as of ${COMPTIME}):

$(python ${EXECDIR}/scripts/get_glider_stats.py -f GLIDER_STATUS_FILENAME)


EOF
)

mail -s "Status of Glider Deployment Availability" $EMAIL << EOF
$status_info
EOF
