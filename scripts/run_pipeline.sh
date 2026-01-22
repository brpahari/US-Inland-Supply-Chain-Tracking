#!/usr/bin/env bash
set -euo pipefail

rm -f pipeline_status.env
mkdir -p data/history

RIVER_RC=0
BARGE_RC=0
RAIL_RC=0
RISK_RC=0

echo "Running river_monitor"
python river_monitor.py || RIVER_RC=$?

if [ "$RIVER_RC" -ne 0 ]; then
  echo "River failed"
  echo "RIVER_RC=$RIVER_RC" >> pipeline_status.env
  exit "$RIVER_RC"
fi

echo "Running barge_monitor"
python barge_monitor.py || BARGE_RC=$?

if [ "$BARGE_RC" -ne 0 ]; then
  echo "Barge failed"
  echo "BARGE_RC=$BARGE_RC" >> pipeline_status.env
  exit "$BARGE_RC"
fi

echo "Running rail_monitor"
python rail_monitor.py || RAIL_RC=$?

echo "Running generate_risk"
python generate_risk.py || RISK_RC=$?

echo "RIVER_RC=$RIVER_RC" >> pipeline_status.env
echo "BARGE_RC=$BARGE_RC" >> pipeline_status.env
echo "RAIL_RC=$RAIL_RC" >> pipeline_status.env
echo "RISK_RC=$RISK_RC" >> pipeline_status.env

echo "river rc $RIVER_RC"
echo "barge rc $BARGE_RC"
echo "rail rc $RAIL_RC"
echo "risk rc $RISK_RC"

if [ "$RISK_RC" -ne 0 ]; then
  echo "Risk failed"
  exit "$RISK_RC"
fi

if [ "$RAIL_RC" -ne 0 ]; then
  echo "Rail format watchdog triggered"
  exit 1
fi

exit 0
