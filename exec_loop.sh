#!/usr/bin/env bash
# JobClaw — Continuous loop. Runs pipeline every 60 minutes.
#
# Usage:
#   chmod +x exec_loop.sh
#   ./exec_loop.sh                              # foreground
#   nohup ./exec_loop.sh > data/loop.log 2>&1 & # background
#
# Stop:
#   kill $(cat .jobclaw.pid)

cd "$(dirname "$0")"
echo $$ > .jobclaw.pid
INTERVAL=3600

echo "🦀 JobClaw loop started (PID: $$, interval: ${INTERVAL}s)"

while true; do
    echo "──── Run: $(date) ────"
    python3 run.py
    echo "💤 Next run in $(($INTERVAL / 60)) minutes..."
    sleep $INTERVAL
done
