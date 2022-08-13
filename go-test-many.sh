#!/bin/bash
mkdir -p test-log
RUNS=100
for ((i=1;i<=$RUNS;i++))
do
    make project2 > ./test-log/log-$i.log
		fail=`fgrep -r "FAIL" ./test-log/log-$i.log | wc -l`
    if [ ${fail} -ne 0 ]; then
        echo -e "\033[31mFail ${i}/${RUNS}\033[0m"
    else
        echo -e "\033[32mDone ${i}/${RUNS}\033[0m"
    fi
done