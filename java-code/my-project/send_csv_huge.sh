#!/bin/bash

baseTestPath="../../huge_test"

# Send data to containers
docker exec namenode rm -rf /tmp/test
docker exec namenode mkdir -p /tmp/test

docker cp ${baseTestPath}/huge_file_001.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/huge_file_002.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/huge_file_003.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/huge_file_004.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/huge_file_005.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/huge_file_006.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/huge_file_007.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/huge_file_008.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/huge_file_009.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/huge_file_010.csv namenode:/tmp/test/.

# Put data on HDFS
docker exec namenode hdfs dfs -rm -r -skipTrash /user/category/input
docker exec namenode hdfs dfs -rm -r -skipTrash /user/category/result

docker exec namenode hdfs dfs -mkdir -p /user/category/input

docker exec namenode hdfs dfs -put /tmp/test/. /user/category/input 
