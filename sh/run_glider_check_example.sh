#! /bin/bash
# written by J Engdahl on November 5 2026

. ~/.bashrc;

app=$(basename $0);

# Default values - fill these in on your copy
REPO_DIR=/PATH/TO/REPO;                        # this repo (scripts/ and sh/)
OUT_DIR=/PATH/TO/OUTPUT/DIR;                   # reports, text summaries and dated copies (reports/) go here
conda_env='glider-bonus-content';
email='email1@domain.edu, email2@domain.com';
web_dir='';                                    # optional, e.g. /PATH/TO/WEB/DIR - '' = no web copy
web_url='';                                    # URL web_dir is served at, e.g. https://URL/TO/WEB/DIR

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
        dac       daily DAC data-flow check (check_dac_flow.py) [Default]
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
python ${REPO_DIR}/scripts/$script $args --html ${name}_report.html --archive-dir reports > ${name}_last.txt 2>&1;
status=$?;
echo "$(date +"%Y-%m-%d %H:%M:%S"): Finished $check check (exit $status)";

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
sh ${REPO_DIR}/sh/mail_report.sh "$subject $(date +%F)" "$email" $body ${name}_report.html;
echo "$(date +"%Y-%m-%d %H:%M:%S"): Emailed $email";
echo " "
