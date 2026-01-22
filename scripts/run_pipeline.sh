#!/usr/bin/env bash
set -u

mkdir -p data data/history

echo "Running river_monitor"
python river_monitor.py
RIVER_RC=$?

echo "Running barge_monitor"
python barge_monitor.py
BARGE_RC=$?

echo "Running rail_monitor"
set +e
python rail_monitor.py
RAIL_RC=$?
set -e

echo "Running generate_risk"
python generate_risk.py
RISK_RC=$?

echo "RIVER_RC=$RIVER_RC" >> pipeline_status.env
echo "BARGE_RC=$BARGE_RC" >> pipeline_status.env
echo "RAIL_RC=$RAIL_RC" >> pipeline_status.env
echo "RISK_RC=$RISK_RC" >> pipeline_status.env

if [ "$RIVER_RC" -ne 0 ]; then
  echo "River failed"
  exit "$RIVER_RC"
fi

if [ "$BARGE_RC" -ne 0 ]; then
  echo "Barge failed"
  exit "$BARGE_RC"
fi

if [ "$RISK_RC" -ne 0 ]; then
  echo "Risk failed"
  exit "$RISK_RC"
fi

echo "Pipeline finished with rail rc $RAIL_RC"
exit 0
