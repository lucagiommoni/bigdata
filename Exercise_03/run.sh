#!/bin/bash

re='^[0-9]+$'
REDUCER_NUM="test"

echo "Insert exercise number:"
read EX_NUM

while ! [[ $REDUCER_NUM =~ $re ]]; do
	echo "Insert number of reducer:"
	read REDUCER_NUM
done

# Remove directory relative to the exercise
hdfs dfs -rm -r -f /user/$(whoami)/ex${EX_NUM}

# Crete the directory
hdfs dfs -mkdir -p /user/$(whoami)/ex${EX_NUM}

# Copy into the directory the directory with data files
hdfs dfs -put ex${EX_NUM}/data /user/$(whoami)/ex${EX_NUM}

# Execute jar
hadoop jar ex${EX_NUM}/ex${EX_NUM}*.jar it.polito.bigdata.hadoop.ex${EX_NUM}.MyDriver $REDUCER_NUM /user/$(whoami)/ex${EX_NUM}/data /user/$(whoami)/ex${EX_NUM}/output

echo
echo
echo "OUTPUT FILE"
hdfs dfs -ls -h /user/$(whoami)/ex${EX_NUM}/output
echo
echo "OUTPUT CONTENT"
hdfs dfs -cat /user/$(whoami)/ex${EX_NUM}/output/*
