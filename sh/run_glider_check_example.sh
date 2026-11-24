#! /bin/bash
# written by J Engdahl on November 5 2026

. ~/.bashrc;

app=$(basename $0);

# Where things are (found automatically): mail_report.sh sits next to this script; the python scripts are either in
# the same folder (everything in one folder), in scripts/ beside this script's folder (this repo: scripts/ + sh/),
# or in the folder above it (a copy with the .sh files in a sh/ subfolder)
sh_dir="$(cd "$(dirname "$0")" && pwd)";
base_dir="$(dirname "$sh_dir")";
if [ -f "$sh_dir/check_dac_flow.py" ]; then scripts_dir="$sh_dir";
elif [ -d "$base_dir/scripts" ]; then scripts_dir="$base_dir/scripts";
else scripts_dir="$base_dir"; fi

# Default values - fill these in on your copy
OUT_DIR=/PATH/TO/OUTPUT/DIR;                   # reports, text summaries and dated copies (reports/) go here
conda_env='glider-bonus-content';
email='email1@domain.edu, email2@domain.com';
web_dir='';                                    # optional, e.g. /PATH/TO/WEB/DIR - '' = no web copy
web_url='';                                    # URL web_dir is served at, e.g. https://URL/TO/WEB/DIR
erddapgraph_dir=/PATH/TO/erddapgraph;          # clone of https://github.com/kerfoot/erddapgraph (deployment summaries)

# Usage message
USAGE="
NAME
    $app - run a glider server check and email the HTML report

SYNOPSIS
    $app [-h] [-e ENVIRONMENT] [-d OUT_DIR] [-m EMAIL] [-w WEB_DIR] [-u WEB_URL] [CHECK]

DESCRIPTION

    Run one of the glider server checks, save the HTML report (plus a dated
    copy in OUT_DIR/reports/), copy it to a web folder, and email it through
    the server's sendmail (sh/mail_report.sh): a link to the web copy and the
    text summary in the body, the HTML attached.

    CHECK
        dac       daily DAC data-flow check (check_dac_flow.py) [Default]. Deployments
                  recovered since the last run each get a one-page summary
                  (recovered_deployment_summary.py) in OUT_DIR/glider_deployment_summary/,
                  also copied to WEB_DIR/glider_deployment_summary/ if there's a web
                  folder, and listed in the email (with its link).
        realtime  realtime old vs new (compare_server_availability.py)

    -h
        show help message

    -e ENVIRONMENT
        alternate conda environment [Default=$conda_env]

    -d OUT_DIR
        folder for the reports [Default=$OUT_DIR]

    -m EMAIL
        where to send the report [Default=$email]

    -w WEB_DIR
        web folder the report is copied to; '' = don't copy [Default=$web_dir]

    -u WEB_URL
        URL that WEB_DIR is served at, for the link in the email [Default=$web_url]
";

# Process options
while getopts "he:d:m:w:u:" option
do
    case "$option" in
        "h")
            echo -e "$USAGE";
            exit 0;
            ;;
        "e")
            conda_env="$OPTARG";
            ;;
        "d")
            OUT_DIR="$OPTARG";
            ;;
        "m")
            email="$OPTARG";
            ;;
        "w")
            web_dir="$OPTARG";
            ;;
        "u")
            web_url="$OPTARG";
            ;;
        "?")
            echo -e "$USAGE" >&2;
            exit 1;
            ;;
    esac
done

# Remove options from $@
shift $((OPTIND-1));

check="${1:-dac}";
case "$check" in
    "dac")
        script='check_dac_flow.py'; args=''; name='dac_flow'; subject='Glider DAC flow';
        ;;
    "realtime")
        script='compare_server_availability.py'; args='--check realtime'; name='realtime'; subject='Glider realtime old vs new';
        ;;
    *)
        echo "ERROR: unknown CHECK '$check' (use dac or realtime)" >&2;
        echo -e "$USAGE" >&2;
        exit 1;
        ;;
esac

# Activate the conda environment
conda activate $conda_env;
# Exit if failed to activate python environment
[ "$?" -ne 0 ] && exit 1;

mkdir -p "$OUT_DIR/reports";
cd "$OUT_DIR" || exit 1;

echo "$(date +"%Y-%m-%d %H:%M:%S"): Running $check check";
python ${scripts_dir}/$script $args --html ${name}_report.html --archive-dir reports > ${name}_last.txt 2>&1;
status=$?;
echo "$(date +"%Y-%m-%d %H:%M:%S"): Finished $check check (exit $status)";

# Recovered deployments (dac check): check_dac_flow.py lists the newly recovered ones in recovered_today.txt; make a
# one-page summary for each in OUT_DIR/glider_deployment_summary/, copy it to web_dir/glider_deployment_summary/ when
# there's a web folder, and list it in the email (with its link)
if [ "$check" == "dac" ] && [ "$status" -eq 0 ] && [ -s recovered_today.txt ]; then
    mkdir -p glider_deployment_summary;
    echo "" >> ${name}_last.txt;
    echo "Deployment summaries:" >> ${name}_last.txt;
    for dep in $(cat recovered_today.txt); do
        echo "$(date +"%Y-%m-%d %H:%M:%S"): Making deployment summary for $dep";
        pdf=glider_deployment_summary/${dep}_summary.pdf;
        if PYTHONPATH="${erddapgraph_dir}:${PYTHONPATH}" python ${scripts_dir}/recovered_deployment_summary.py \
               $dep -o glider_deployment_summary >> glider_deployment_summary/summary.log 2>&1; then
            echo "  made $OUT_DIR/$pdf" >> ${name}_last.txt;
            if [ -n "$web_dir" ]; then
                if mkdir -p "$web_dir/glider_deployment_summary" && chmod a+rx "$web_dir/glider_deployment_summary" \
                       && cp $pdf "$web_dir/glider_deployment_summary/" && chmod a+r "$web_dir/$pdf"; then
                    echo "  view: $web_url/$pdf" >> ${name}_last.txt;
                    echo "$(date +"%Y-%m-%d %H:%M:%S"): Copied $pdf to $web_dir/glider_deployment_summary";
                else
                    echo "  (could not copy it to the web folder $web_dir/glider_deployment_summary)" >> ${name}_last.txt;
                    echo "$(date +"%Y-%m-%d %H:%M:%S"): Could not copy $pdf to $web_dir" >&2;
                fi
            fi
        else
            echo "  FAILED for $dep - see $OUT_DIR/glider_deployment_summary/summary.log" >> ${name}_last.txt;
        fi
    done
fi

# Copy the report to the web folder (same name each run, so the link always shows the latest) and put the link at
# the top of the email body. Only a fresh report is copied - if the check failed, the web copy is left as it was.
body=${name}_last.txt;
if [ -n "$web_dir" ]; then
    body=${name}_email.txt;
    if [ "$status" -ne 0 ]; then
        echo "(the check failed - web copy NOT updated, so the link would show an older report; see the error below)" > $body;
        echo "$(date +"%Y-%m-%d %H:%M:%S"): Check failed - web copy not updated";
    elif mkdir -p "$web_dir" && cp ${name}_report.html "$web_dir/" && chmod a+r "$web_dir/${name}_report.html"; then
        echo "View in browser: $web_url/${name}_report.html" > $body;
        echo "$(date +"%Y-%m-%d %H:%M:%S"): Copied report to $web_dir";
    else
        echo "(could not copy the report to the web folder $web_dir - the link would show an older report; the attachment is current)" > $body;
        echo "$(date +"%Y-%m-%d %H:%M:%S"): Could not copy report to $web_dir" >&2;
    fi
    echo "" >> $body;
    cat ${name}_last.txt >> $body;
fi

# Email even if the check failed - the body then holds the error
sh ${sh_dir}/mail_report.sh "$subject $(date +%F)" "$email" $body ${name}_report.html;
echo "$(date +"%Y-%m-%d %H:%M:%S"): Emailed $email";
echo " "
