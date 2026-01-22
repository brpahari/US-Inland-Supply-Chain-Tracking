#!/usr/bin/env bash
set -u

mkdir -p data data/history
rm -f pipeline_status.env

echo "Running river_monitor"
python river_monitor.py
RIVER_RC=$?
echo "RIVER_RC=$RIVER_RC" >> pipeline_status.env
if [ "$RIVER_RC" -ne 0 ]; then
  echo "River failed"
  exit "$RIVER_RC"
fi

echo "Running barge_monitor"
python barge_monitor.py
BARGE_RC=$?
echo "BARGE_RC=$BARGE_RC" >> pipeline_status.env
if [ "$BARGE_RC" -ne 0 ]; then
  echo "Barge failed"
  exit "$BARGE_RC"
fi

echo "Running rail_monitor"
set +e
python rail_monitor.py
RAIL_RC=$?
set -e
echo "RAIL_RC=$RAIL_RC" >> pipeline_status.env

echo "Running generate_risk"
python generate_risk.py
RISK_RC=$?
echo "RISK_RC=$RISK_RC" >> pipeline_status.env
if [ "$RISK_RC" -ne 0 ]; then
  echo "Risk failed"
  exit "$RISK_RC"
fi

exit 0
