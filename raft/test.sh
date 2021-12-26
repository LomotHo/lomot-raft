#!/bin/bash

TEST_PROJECT=$1
for ((i = 1; i <= 100; i++)); do
  echo $i
  GO_DEBUG=1 go test -run $TEST_PROJECT -race >>../.tmp/raft.log
  # go test -run $TEST_PROJECT -race >>../.tmp/raft.log
done

# path=.tmp/res
# B_test_list=(TestBasicAgree2B TestRPCBytes2B TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B)
# C_test_list=(TestPersist12C TestPersist22C TestPersist32C TestFigure82C TestUnreliableAgree2C TestFigure8Unreliable2C internalChurn TestReliableChurn2C TestUnreliableChurn2C)
# if test $1 = "2B" ;then
# 	test_list=(${B_test_list[@]})
# elif test $1 = "2C" ;then
# 	test_list=(${C_test_list[@]})
# else
# 	test_list=$1
# fi
# echo ${test_list[@]}
# rm -rf ${path}*
# for ((i=1;i<=$2;i++))
# do
# 	for test_name  in ${test_list[@]}
# 	do
# 	sleep 0.5
# 	go test -run ${test_name} -race >> ${path}${test_name}${i}
# 	grep "FAIL" ${path}${test_name}${i} > /dev/null
# 	if [ $? -eq 0 ];then
# 		echo "${test_name}${i}fail"
# 	else
# 		echo "${test_name}${i}pass"
# 		rm ${path}${test_name}${i}
# 	fi
# 	done
# done
