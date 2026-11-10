#!/bin/sh
# Email a report with the HTML attached, through the server's sendmail (the same path cron's MAILTO uses).
#
#   sh mail_report.sh "<subject>" <to> <body.txt> <report.html>
#
# Normally called by run_glider_check_example.sh (your filled-in copy); on its own, e.g.:
#   sh mail_report.sh "Glider DAC flow $(date +%F)" you@example.edu dac_flow_last.txt dac_flow_report.html
#
# DRY_RUN=1 prints the message instead of sending it.

SUBJECT="$1"; TO="$2"; BODY="$3"; ATTACH="$4"
SENDMAIL=${SENDMAIL:-/usr/sbin/sendmail}
B="report-$(date +%s)-$$"

if [ -z "$SUBJECT" ] || [ -z "$TO" ] || [ -z "$BODY" ]; then
    echo "usage: sh mail_report.sh \"<subject>\" <to> <body.txt> [report.html]" >&2; exit 2
fi

build() {
    echo "To: $TO"
    echo "Subject: $SUBJECT"
    echo "MIME-Version: 1.0"
    echo "Content-Type: multipart/mixed; boundary=\"$B\""
    echo
    echo "--$B"
    echo "Content-Type: text/plain; charset=utf-8"
    echo
    if [ -f "$BODY" ]; then cat "$BODY"; else echo "(no output file $BODY - the script may not have run)"; fi
    if [ -n "$ATTACH" ] && [ -f "$ATTACH" ]; then
        name=$(basename "$ATTACH")
        echo
        echo "--$B"
        echo "Content-Type: text/html; charset=utf-8; name=\"$name\""
        echo "Content-Transfer-Encoding: base64"
        echo "Content-Disposition: attachment; filename=\"$name\""
        echo
        base64 "$ATTACH"
    fi
    echo
    echo "--$B--"
}

if [ -n "$DRY_RUN" ]; then build; else build | "$SENDMAIL" -t; fi
