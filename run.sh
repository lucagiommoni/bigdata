#!/bin/bash

re_num='^[0-9]+$'
re_yn='^[YNyn]$'

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
		echo "*** default value $EX_VER chosen!"
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
		echo "*** default value $REDUCER_NUM chosen!"
		echo
		break;
	elif [[ $REDUCER_NUM =~ $re_num ]]; then
		break;
	fi
done

# while [[ true ]]; do
# 	echo "Use of combiner (y|n - default=n):"
# 	read ISCOMBINER
# 	if [[ -z "$ISCOMBINER" ]]; then
# 		ISCOMBINER=n
# 		echo "*** default value $ISCOMBINER chosen!"
# 		echo
# 		break;
# 	elif [[ $ISCOMBINER =~ $re_yn ]]; then
# 		break;
# 	fi
# done

while [[ true ]]; do
	echo "Insert the number of additional parameter (default=0):"
	read ADD_PAR_COUNT
	if [[ -z "$ADD_PAR_COUNT" ]]; then
		ADD_PAR_COUNT=0
		echo "*** default value $ADD_PAR_COUNT chosen!"
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
	echo
fi

while [[ true ]]; do
	echo "Multiple Inputs? (y|n - default=n):"
	read MULINP
	if [[ -z "$MULINP" ]]; then
		MULINP=n
		echo "*** default value $MULINP chosen!"
		echo
		break;
	elif [[ $MULINP =~ $re_yn ]]; then
		break;
	fi
done

echo

if [[ "${MULINP,,}" == "y" ]]; then
	while [[ true ]]; do
		echo "Insert input path:"
		read TMPINPUT
		if [[ -z $TMPINPUT ]]; then
			if [[ -z $INPUT ]]; then
				echo "Insert at least one input"
				continue
			else
				break
			fi
		fi
		INPUT="$INPUT $TMPINPUT"
	done
	INPUT="$(echo -e "${INPUT}" | sed -e 's/^[[:space:]]*//')"
	INPUT="$(echo -e "${INPUT}" | sed -e 's/[[:space:]]*$//')"
else
	INPUT="/user/${USER}/data"
fi

echo "This is the multiple input"
echo "$INPUT"
echo "It's right? (y)"
while [[ true ]]; do
	read ANSWER
	if [[ -z $ANSWER ]] || [[ "${ANSWER,,}" == "y" ]]; then
		break
	else
		echo "To continue insert 'y'"
	fi
done
echo
echo "Starting..."
echo
echo "hdfs dfs -rm -r -f /user/${USER}/*"
echo
# Remove directory relative to the exercise
hdfs dfs -rm -r -f /user/${USER}/*
echo
echo "hdfs dfs -put ex${EX_NUM}/data /user/${USER}"
echo
# Copy into the directory the directory with data files
hdfs dfs -put ex${EX_NUM}/data /user/${USER}
echo
# echo "hadoop jar ex${EX_NUM}/ex${EX_NUM}-${EX_VER}.jar it.polito.bigdata.hadoop.ex${EX_NUM}.MyDriver $REDUCER_NUM $ISCOMBINER "/user/${USER}/output" "$INPUT" $ADD_PAR"
ACT="hadoop jar ex${EX_NUM}/ex${EX_NUM}-${EX_VER}.jar it.polito.bigdata.hadoop.ex${EX_NUM}.MyDriver $REDUCER_NUM /user/${USER}/output $INPUT $ADD_PAR"
echo "$ACT"
echo
# Execute jar
# hadoop jar ex${EX_NUM}/ex${EX_NUM}-${EX_VER}.jar it.polito.bigdata.hadoop.ex${EX_NUM}.MyDriver $REDUCER_NUM $ISCOMBINER "/user/${USER}/output" "$INPUT" $ADD_PAR
RES=$( $ACT )

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
