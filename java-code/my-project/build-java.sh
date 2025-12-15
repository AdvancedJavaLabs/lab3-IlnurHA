#!/bin/bash

# Stop service
docker compose down hadoop-app
docker rmi my-project-hadoop-app:latest

# Build new package
mvn clean
mvn clean package
echo $(bash -c "jar tf target/*.jar | grep CategoryDriver")

# Test from homework
# Update data
bash send_csv.sh

# Start hadoop-app
docker compose up hadoop-app

# Get result
docker exec namenode hdfs dfs -getmerge /user/category/result/* /tmp/result.csv
docker cp namenode:/tmp/result.csv ./../../hw_result.csv
docker exec namenode rm /tmp/result.csv

echo "Result"
echo "Category,Price,Quantity"
cat ./../../hw_result.csv

# Test on huge data
# Update data
bash send_csv_huge.sh

# Start hadoop-app
docker compose up hadoop-app

# Get result
docker exec namenode hdfs dfs -getmerge /user/category/result/* /tmp/result.csv
docker cp namenode:/tmp/result.csv ./../../huge_test_result.csv
docker exec namenode rm /tmp/result.csv

echo "Result"
echo "Category,Price,Quantity"
cat ./../../huge_test_result.csv