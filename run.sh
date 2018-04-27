#!/bin/bash

re_num='^[0-9]+$'
re_yn='^[yn]$'

REDUCER_NUM=""
ISCOMBINER=""
EX_NUM=""
EX_VER=""

USER=$(whoami)

echo
echo

while [[ true ]]; do
	echo "Insert exercise number:"
	read EX_NUM
	if [[ -d ex${EX_NUM} ]]; then
		break
	fi
		echo "No directory found related to exercise ${EX_NUM}"
done

while [[ true ]]; do
	echo "Insert exercise version (press enter for default=1):"
	read EX_VER
	if [[ -z "$EX_VER" ]]; then
		EX_VER=1
		echo "*** default value $EX_VER choosen!"
		echo
		break;
	elif [[ ! -f ex${EX_NUM}/ex${EX_NUM}-${EX_VER}.jar ]]; then
		echo "JAR file 'ex${EX_NUM}/ex${EX_NUM}-${EX_VER}.jar' not found!"
		echo "Choose one between"
		ls ex${EX_NUM}/*.jar
	else
		break
	fi
done

while [[ true ]]; do
	echo "Insert number of reducer (press enter for default=1):"
	read REDUCER_NUM
	if [[ -z "$REDUCER_NUM" ]]; then
		REDUCER_NUM=1
		echo "*** default value $REDUCER_NUM choosen!"
		echo
		break;
	elif [[ $REDUCER_NUM =~ $re_num ]]; then
		break;
	fi
done

while [[ true ]]; do
	echo "Use of combiner (y|n - default=n):"
	read ISCOMBINER
	if [[ -z "$ISCOMBINER" ]]; then
		ISCOMBINER=n
		echo "*** default value $ISCOMBINER choosen!"
		echo
		break;
	elif [[ $ISCOMBINER =~ $re_yn ]]; then
		break;
	fi
done

while [[ true ]]; do
	echo "Insert the number of additional parameter (default=0):"
	read ADD_PAR_COUNT
	if [[ -z "$ADD_PAR_COUNT" ]]; then
		ADD_PAR_COUNT=0
		echo "*** default value $ISCOMBINER choosen!"
		echo
		break
	elif [[ $REDUCER_NUM =~ $re_num ]]; then
		break
	fi
done

if [[ $ADD_PAR_COUNT -ne 0 ]]; then
	for (( i = 0; i < $ADD_PAR_COUNT; i++ )); do
		echo "Insert the value of parameter:"
		read TMP
		while [[ -z "$TMP" ]]; do
			echo "Empty parameter entered, please enter non empty value:"
			read TMP
		done
		ADD_PAR="$ADD_PAR $TMP"
	done
fi

echo
echo

# Remove directory relative to the exercise
hdfs dfs -rm -r -f /user/${USER}/*

# Copy into the directory the directory with data files
hdfs dfs -put ex${EX_NUM}/data /user/${USER}

# Execute jar
hadoop jar ex${EX_NUM}/ex${EX_NUM}-${EX_VER}.jar it.polito.bigdata.hadoop.ex${EX_NUM}.MyDriver "/user/${USER}/data" "/user/${USER}/output" $REDUCER_NUM $ISCOMBINER $ADD_PAR

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
