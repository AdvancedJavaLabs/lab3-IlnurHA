#!/bin/bash

baseTestPath="../../"

# Send data to containers
docker exec namenode rm -rf /tmp/test
docker exec namenode mkdir -p /tmp/test

docker cp ${baseTestPath}/0.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/1.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/2.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/3.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/4.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/5.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/6.csv namenode:/tmp/test/.
docker cp ${baseTestPath}/7.csv namenode:/tmp/test/.

# Put data on HDFS
docker exec namenode hdfs dfs -rm -r -skipTrash /user/category/input
docker exec namenode hdfs dfs -rm -r -skipTrash /user/category/result

docker exec namenode hdfs dfs -mkdir -p /user/category/input

docker exec namenode hdfs dfs -put /tmp/test/. /user/category/input 
