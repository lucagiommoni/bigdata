#!/bin/bash

re='^[0-9]+$'
REDUCER_NUM="test"
EX_NUM="test"
USER=$(whoami)

echo
echo

while [[ true ]]; do
	echo "Insert exercise number:"
	read EX_NUM
	if [[ ! -d ex${EX_NUM} ]]; then
		echo "No directory found related to exercise ${EX_NUM}"
	else
		break
	fi
done

while [[ true ]]; do
	echo "Insert exercise version:"
	read EX_VER
	if [[ ! -f ex${EX_NUM}/ex${EX_NUM}-${EX_VER}.jar ]]; then
		echo "JAR file 'ex${EX_NUM}/ex${EX_NUM}-${EX_VER}.jar' not found!"
		echo "Choose one between"
		ls ex${EX_NUM}/*.jar
	else
		break
	fi
done

while ! [[ $REDUCER_NUM =~ $re ]]; do
	echo "Insert number of reducer:"
	read REDUCER_NUM
done

echo
echo

# Remove directory relative to the exercise
hdfs dfs -rm -r -f /user/${USER}/*

# Copy into the directory the directory with data files
hdfs dfs -put ex${EX_NUM}/data /user/${USER}

# Execute jar
hadoop jar ex${EX_NUM}/ex${EX_NUM}-${EX_VER}.jar it.polito.bigdata.hadoop.ex${EX_NUM}.MyDriver ${REDUCER_NUM} /user/${USER}/data /user/${USER}/output

if [[ $? -ne 0 ]]; then
	echo
	echo
	echo "Some error occurs: No clean operation will be done in order to make further analysis!"
	exit 1
fi

echo
echo
echo "OUTPUT FILE"
hdfs dfs -ls -h /user/${USER}/output
echo
echo "OUTPUT CONTENT"
hdfs dfs -cat /user/${USER}/output/part-r-*
echo
echo
# Remove directory relative to the exercise
hdfs dfs -rm -r -f /user/${USER}/*

echo
echo
echo
