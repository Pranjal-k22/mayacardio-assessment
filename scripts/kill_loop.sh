#!/usr/bin/env bash
# kill -9 the daemon at random moments, then verify:
#   1. systemd restarts it automatically (Restart=always)
#   2. SQLite WAL / btree integrity is intact (PRAGMA integrity_check == ok)
#   3. Committed sample count never goes backwards (zero silent regression)
#
# Usage:
#   sudo ./scripts/kill_loop.sh               (defaults: 10 kills)
#   sudo KILLS=20 GROUND_TRUTH_CMD="check_ground_truth ..." ./scripts/kill_loop.sh
# Requirements: pipeline.service running under systemd, mock running.
set -uo pipefail

[ "$(id -u)" -eq 0 ] || { echo "Run with sudo."; exit 1; }

REPO_DIR="${REPO_DIR:-/home/${SUDO_USER:-pi}/mayacardio}"
UNIT="${UNIT:-pipeline.service}"
DB_PATH="${DB_PATH:-$REPO_DIR/pipeline.db}"
STATUS_CMD="${STATUS_CMD:-$REPO_DIR/pipeline status}"
KILLS="${KILLS:-10}"
MIN_WAIT="${MIN_WAIT:-5}"; MAX_WAIT="${MAX_WAIT:-45}"
RECOVER_LIMIT="${RECOVER_LIMIT:-15}"                    # seconds allowed for daemon to recover
GROUND_TRUTH_CMD="${GROUND_TRUTH_CMD:-}"                # optional: official check_ground_truth command line
OUT="${OUT:-killloop_$(date +%Y%m%d_%H%M%S).csv}"

now()  { awk '{print $1}' /proc/uptime; }
pid()  { systemctl show -p MainPID --value "$UNIT" 2>/dev/null || echo ""; }
status_json() { (cd "$REPO_DIR" && $STATUS_CMD 2>/dev/null | head -n1); }
saved() { status_json | jq -r '(.samples_saved // .saved) | if type=="object" then (map(.)|add) else . end // empty' 2>/dev/null; }

echo "kill,old_pid,new_pid,recover_s,integrity,saved_before,saved_after,verdict" | tee "$OUT"
fails=0
for i in $(seq 1 "$KILLS"); do
  wait_s=$((RANDOM % (MAX_WAIT - MIN_WAIT + 1) + MIN_WAIT))
  echo "--- Kill $i / $KILLS: waiting ${wait_s}s before kill..."
  sleep "$wait_s"

  before="$(saved)"; old="$(pid)"
  if [ -z "$old" ] || [ "$old" = "0" ]; then
    echo "$i,NA,NA,NA,NA,NA,NA,FAIL_daemon_not_running" | tee -a "$OUT"
    fails=$((fails+1))
    continue
  fi

  echo "  Terminating MainPID $old with SIGKILL (-9)..."
  kill -9 "$old"
  t0=$(now)

  # Wait for systemd to bring up a NEW process that responds to status
  new=""; rec="TIMEOUT"
  while :; do
    p="$(pid)"
    if [ -n "$p" ] && [ "$p" != "0" ] && [ "$p" != "$old" ] && [ -n "$(saved)" ]; then
      new="$p"
      rec=$(awk -v a="$(now)" -v b="$t0" 'BEGIN{printf "%.1f", a-b}')
      break
    fi
    if awk -v a="$(now)" -v b="$t0" -v l="$RECOVER_LIMIT" 'BEGIN{exit !(a-b>l)}'; then
      break
    fi
    sleep 0.3
  done

  integ="$(sqlite3 "file:$DB_PATH?mode=ro" 'PRAGMA integrity_check;' 2>&1 | head -n1)"
  sleep 3                                  # allow daemon to commit subsequent samples
  after="$(saved)"

  verdict=PASS
  [ "$rec" = "TIMEOUT" ] && verdict=FAIL
  [ "$integ" = "ok" ] || verdict=FAIL
  if [ -n "$before" ] && [ -n "$after" ] && [ "$after" -lt "$before" ]; then
    verdict=FAIL   # committed data must never regress
  fi
  [ "$verdict" = "FAIL" ] && fails=$((fails+1))
  echo "  Result: old_pid=$old new_pid=$new recovered=${rec}s integrity=$integ saved: $before -> $after ($verdict)"
  echo "$i,$old,$new,$rec,$integ,${before:-NA},${after:-NA},$verdict" | tee -a "$OUT"
done

echo
echo "=========================================================="
echo "Kill Loop Summary: Kills=$KILLS, Failed=$fails"
echo "Evidence Log: $OUT"
echo "=========================================================="

if [ -n "$GROUND_TRUTH_CMD" ]; then
  echo "== Running official ground-truth check..."
  eval "$GROUND_TRUTH_CMD" || fails=$((fails+1))
else
  echo "NOTE: Set GROUND_TRUTH_CMD to run official ground-truth verification on exported telemetry."
fi

[ "$fails" -eq 0 ]
