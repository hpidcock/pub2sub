#!/bin/bash

./publisher & pid=$!
PID_LIST+=" $pid";

./distributor & pid=$!
PID_LIST+=" $pid";

./planner & pid=$!
PID_LIST+=" $pid";

./executor & pid=$!
PID_LIST+=" $pid";

./subscriber & pid=$!
PID_LIST+=" $pid";

trap "kill $PID_LIST" SIGINT

echo "Parallel processes have started";

wait $PID_LIST

echo
echo "All processes have completed";