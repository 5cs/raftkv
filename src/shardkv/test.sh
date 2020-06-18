#!/bin/bash

TEST_CASES="TestStaticShards
TestJoinLeave
TestSnapshot
TestMissChange
TestConcurrent1
TestConcurrent2
TestUnreliable1
TestUnreliable2
TestUnreliable3
TestChallenge1Delete
TestChallenge1Concurrent
TestChallenge2Unaffected
TestChallenge2Partial"

TEST_CASES="TestStaticShards
TestJoinLeave
TestSnapshot"


for test_case in $TEST_CASES; do
  echo $test_case
  output_dir=./out1/$test_case
  if [[ ! -e $output_dir ]]; then
    mkdir -p $output_dir
  fi
  for i in `seq 1 100`; do
    echo $test_case $i
    go test -run $test_case 2>$output_dir/$i
    if [[ $? -eq 0 ]]; then
      rm $output_dir/$i
    fi
  done
done
