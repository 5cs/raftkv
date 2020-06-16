#!/bin/sh

TEST_CASES="TestBasic3A
TestConcurrent3A
TestUnreliable3A
TestUnreliableOneKey3A
TestOnePartition3A
TestManyPartitionsOneClient3A
TestManyPartitionsManyClients3A
TestPersistOneClient3A
TestPersistConcurrent3A
TestPersistConcurrentUnreliable3A
TestPersistPartition3A
TestPersistPartitionUnreliable3A
TestPersistPartitionUnreliableLinearizable3A
TestSnapshotRPC3B
TestSnapshotSize3B
TestSnapshotRecover3B
TestSnapshotRecoverManyClients3B
TestSnapshotUnreliable3B
TestSnapshotUnreliableRecover3B
TestSnapshotUnreliableRecoverConcurrentPartition3B
TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B"


for test_case in $TEST_CASES; do
  echo $test_case
  output_dir=./out1/$test_case
  if [ ! -e $output_dir ]; then
    mkdir -p $output_dir
  fi
  for i in `seq 1 100`; do
    echo $test_case $i
    go test -run $test_case 2>$output_dir/$i
    if [ $? -eq 0 ]; then
      rm $output_dir/$i
    fi
  done
done
