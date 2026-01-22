set -u

RIVER_RC=0
BARGE_RC=0
RAIL_RC=0
RISK_RC=0

echo "Running river_monitor"
python river_monitor.py || RIVER_RC=$?

echo "Running barge_monitor"
python barge_monitor.py || BARGE_RC=$?

echo "Running rail_monitor"
python rail_monitor.py || RAIL_RC=$?

echo "Running generate_risk"
python generate_risk.py || RISK_RC=$?

echo "river rc $RIVER_RC"
echo "barge rc $BARGE_RC"
echo "rail rc $RAIL_RC"
echo "risk rc $RISK_RC"

# Commit updates no matter what
exit_code=0

# Decide when to fail the workflow
if [ "$RAIL_RC" -ne 0 ]; then
  echo "Rail format watchdog triggered"
  exit_code=1
fi

exit "$exit_code"
