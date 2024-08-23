#! /bin/bash --

. ~/.bashrc;

# Deployment root path
deployment_root="${GLIDER_DATA_HOME}/deployments";
REPO_DIR=/home/glideradm/code/glider-bonus-content
conda_env='glider-bonus-content';
DATA_DIR=$(pwd)

# Usage message
USAGE="
NAME
    $app - check fileopen_time in binary files within a directory and compare to times for a given deployment.

SYNOPSIS
    $app [h d] DATASET_ID1 [DATASET_ID2...]

DESCRIPTION
    -h
        show help message
   
    -d DIRECTORY
        base directory to recursively search binary files in for fileopen_time (default: pwd)
";

# Process options
while getopts "hd:" option
do
    case "$option" in
        "h")
            echo -e "$USAGE";
            exit 0;
            ;;
        "d")
            DATA_DIR=$OPTARG;
            ;;
        "?")
            echo -e "$USAGE" >&2;
            exit 1;
            ;;
    esac
done

# Remove option from $@
shift $((OPTIND-1));

deployments="$@";

if [ -z "$deployments" ]
then
    info_msg "No deployments selected for processing";
    exit 0;
fi

if [ ! -d "$deployment_root" ]
then
    error_msg "Invalid destination specified: $deployment_root";
    exit 1;
fi

# Activate the conda environment
info_msg "Activing conda environment: $conda_env";
conda activate $conda_env;

[ "$?" -ne 0 ] && exit 1;

for deployment in $deployments
do
    ts="$(echo $deployment | awk -F- '{print $2}')";
    year=${ts:0:4};

    deployment_dir="${year}/${deployment}";
    echo "Deployment directory: $deployment_dir";

    d_path="${deployment_root}/$deployment_dir";
    if [ ! -d "$d_path" ]
    then
        warn_msg "Deployment path does not exist: $d_path";
        continue;
    fi

    BIN_FILE_DIR="${d_path}/data/in/binary";
    info_msg "Binary file destination directory: $BIN_FILE_DIR";
    if [ ! -d "$BIN_FILE_DIR" ]
    then
        warn_msg "Binary file destination directory does not exist: $BIN_FILE_DIR";
        continue;
    fi

    grep -r 'fileopen_time' $DATA_DIR > ${BIN_FILE_DIR}/${deployment}_binary_open_times.txt
    ${REPO_DIR}/scripts/get_binary_info.py -d ${GLIDER_DATA_HOME} $deployment

done

conda deactivate;

