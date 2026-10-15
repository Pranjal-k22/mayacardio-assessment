#!/usr/bin/env bash
# Post-reboot power-pull verification script for MayaCardio.
#
# Execute this script immediately after each physical power cut and subsequent reboot.
# It automatically:
#   1. Detects boot uptime to confirm a real reboot took place.
#   2. Runs SQLite PRAGMA integrity_check and quick_check on the database.
#   3. Queries committed sample counts per sensor and asserts no backward regression.
#   4. Executes the MayaCardio audit suite on the active session.
#   5. Optionally executes official ground-truth verification on exported telemetry.
#   6. Appends structured evidence to powercut_log.csv.
#
# Usage:
#   sudo ./scripts/powercut_check.sh
#   sudo PULL=3 ./scripts/powercut_check.sh
#   sudo GROUND_TRUTH_CMD="check_ground_truth" ./scripts/powercut_check.sh
set -uo pipefail

[ "$(id -u)" -eq 0 ] || { echo "Run with sudo."; exit 1; }

REPO_DIR="${REPO_DIR:-/home/${SUDO_USER:-pi}/mayacardio}"
DB_PATH="${DB_PATH:-$REPO_DIR/pipeline.db}"
STATUS_CMD="${STATUS_CMD:-$REPO_DIR/pipeline status}"
AUDIT_CMD="${AUDIT_CMD:-$REPO_DIR/pipeline audit}"
EXPORT_CMD="${EXPORT_CMD:-$REPO_DIR/pipeline export}"
GROUND_TRUTH_CMD="${GROUND_TRUTH_CMD:-}"
LOG_FILE="${LOG_FILE:-powercut_log.csv}"

# Ensure database exists
if [ ! -f "$DB_PATH" ]; then
  echo "ERROR: SQLite database not found at '$DB_PATH'."
  exit 1
fi

# 1. Determine pull cycle number
if [ -n "${PULL:-}" ]; then
  CYCLE="$PULL"
elif [ -f "$LOG_FILE" ]; then
  LAST_CYCLE=$(awk -F',' 'NR>1 {c=$1} END {print c+0}' "$LOG_FILE")
  CYCLE=$((LAST_CYCLE + 1))
else
  CYCLE=1
fi

# 2. Monotonic uptime and timestamp
UPTIME_S=$(awk '{print int($1)}' /proc/uptime)
TIMESTAMP_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo "=========================================================="
echo "=== MAYACARDIO PHYSICAL POWER-PULL CHECK — CYCLE $CYCLE / 10 ==="
echo "=========================================================="
echo "Timestamp:    $TIMESTAMP_ISO"
echo "Reboot Uptime: ${UPTIME_S}s"
echo "Database:     $DB_PATH"

if [ "$UPTIME_S" -gt 1800 ]; then
  echo "WARNING: System uptime is > 30 minutes (${UPTIME_S}s). Was the power physically pulled?"
fi

# 3. SQLite Database Integrity Checks
echo "--> 1. Running SQLite PRAGMA integrity_check & quick_check..."
INTEG=$(sqlite3 "file:$DB_PATH?mode=ro" 'PRAGMA integrity_check;' 2>&1 | head -n1)
QUICK=$(sqlite3 "file:$DB_PATH?mode=ro" 'PRAGMA quick_check;' 2>&1 | head -n1)

echo "    integrity_check: $INTEG"
echo "    quick_check:     $QUICK"

# 4. Committed Sample Counts
echo "--> 2. Querying committed sample counts..."
S1_COUNT=$(sqlite3 "file:$DB_PATH?mode=ro" "SELECT count(*) FROM samples WHERE sensor_id = 1;" 2>/dev/null || echo 0)
S2_COUNT=$(sqlite3 "file:$DB_PATH?mode=ro" "SELECT count(*) FROM samples WHERE sensor_id = 2;" 2>/dev/null || echo 0)
TOTAL_COUNT=$((S1_COUNT + S2_COUNT))
MAX_S1_IDX=$(sqlite3 "file:$DB_PATH?mode=ro" "SELECT COALESCE(max(sample_idx), -1) FROM samples WHERE sensor_id = 1;" 2>/dev/null || echo -1)
MAX_S2_IDX=$(sqlite3 "file:$DB_PATH?mode=ro" "SELECT COALESCE(max(sample_idx), -1) FROM samples WHERE sensor_id = 2;" 2>/dev/null || echo -1)

echo "    Sensor 1 samples: $S1_COUNT (max index: $MAX_S1_IDX)"
echo "    Sensor 2 samples: $S2_COUNT (max index: $MAX_S2_IDX)"
echo "    Total samples:    $TOTAL_COUNT"

# Verify non-regression against previous cycle
PREV_TOTAL=0
if [ -f "$LOG_FILE" ]; then
  PREV_TOTAL=$(awk -F',' -v target="$((CYCLE - 1))" '$1 == target {print $7+0}' "$LOG_FILE" | head -n1)
fi

REGRESSION_FAIL=0
if [ -n "$PREV_TOTAL" ] && [ "$PREV_TOTAL" -gt 0 ]; then
  if [ "$TOTAL_COUNT" -lt "$PREV_TOTAL" ]; then
    echo "    FAIL: Sample count regressed! Previous: $PREV_TOTAL, Current: $TOTAL_COUNT"
    REGRESSION_FAIL=1
  else
    echo "    Verified monotonic count progression: $PREV_TOTAL -> $TOTAL_COUNT"
  fi
fi

# 5. Pipeline Audit Verification
echo "--> 3. Executing pipeline audit on latest session..."
SESSION_ID=$(sqlite3 "file:$DB_PATH?mode=ro" "SELECT session_id FROM sessions ORDER BY created_at DESC LIMIT 1;" 2>/dev/null || echo "")

AUDIT_VERDICT="SKIPPED_NO_SESSION"
if [ -n "$SESSION_ID" ]; then
  echo "    Active Session ID: $SESSION_ID"
  AUDIT_OUTPUT=$(cd "$REPO_DIR" && $AUDIT_CMD --session "$SESSION_ID" 2>&1) || true
  if echo "$AUDIT_OUTPUT" | grep -q '"valid":true'; then
    AUDIT_VERDICT="PASS"
    echo "    Pipeline Audit: PASS (Zero gaps, zero collisions)"
  else
    AUDIT_VERDICT="FAIL"
    echo "    Pipeline Audit: FAIL"
    echo "$AUDIT_OUTPUT" | head -n 10
  fi
fi

# 6. Official Ground-Truth Verification (if command provided)
GT_VERDICT="NOT_CONFIGURED"
if [ -n "$GROUND_TRUTH_CMD" ] && [ -n "$SESSION_ID" ]; then
  echo "--> 4. Running official ground-truth check..."
  TMP_EXPORT="/tmp/powercut_export_${SESSION_ID}.json"
  if (cd "$REPO_DIR" && $EXPORT_CMD --session "$SESSION_ID" --from device > "$TMP_EXPORT" 2>/dev/null); then
    if eval "$GROUND_TRUTH_CMD $TMP_EXPORT"; then
      GT_VERDICT="PASS"
      echo "    Ground Truth Check: PASS"
    else
      GT_VERDICT="FAIL"
      echo "    Ground Truth Check: FAIL"
    fi
    rm -f "$TMP_EXPORT"
  else
    GT_VERDICT="EXPORT_FAILED"
  fi
fi

# 7. Overall Verdict Calculation
VERDICT="PASS"
if [ "$INTEG" != "ok" ] || [ "$QUICK" != "ok" ]; then
  VERDICT="FAIL_CORRUPTION"
elif [ "$REGRESSION_FAIL" -eq 1 ]; then
  VERDICT="FAIL_REGRESSION"
elif [ "$AUDIT_VERDICT" = "FAIL" ]; then
  VERDICT="FAIL_AUDIT"
elif [ "$GT_VERDICT" = "FAIL" ]; then
  VERDICT="FAIL_GROUND_TRUTH"
fi

# 8. Append to CSV
if [ ! -f "$LOG_FILE" ]; then
  echo "pull_cycle,timestamp_iso,uptime_s,integrity_check,s1_samples,s2_samples,total_samples,audit_status,ground_truth,verdict" > "$LOG_FILE"
fi

echo "$CYCLE,$TIMESTAMP_ISO,$UPTIME_S,$INTEG,$S1_COUNT,$S2_COUNT,$TOTAL_COUNT,$AUDIT_VERDICT,$GT_VERDICT,$VERDICT" >> "$LOG_FILE"

echo "----------------------------------------------------------"
echo "Pull $CYCLE / 10 Verdict: $VERDICT"
echo "Results logged to $LOG_FILE"
echo "=========================================================="

[ "$VERDICT" = "PASS" ]
