#!/bin/bash
mkdir -p test_log
RUNS=150
for ((i=1;i<=$RUNS;i++))
do
    make project2b > ./testlog/log-$i.log
		fail=`fgrep -r "FAIL" ./testlog/log-$i.log | wc -l`
    if [ ${fail} -ne 0 ]; then
        echo -e "\033[31mFail ${i}/${RUNS}\033[0m"
    else
        echo -e "\033[32mDone ${i}/${RUNS}\033[0m"
    fi
done