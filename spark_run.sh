#!/bin/bash

re_num='^[0-9]+$'
re_yn='^[YNyn]$'
re_dep='^[12]$'

REDUCER_NUM=""
ISCOMBINER=""
EX_NUM=""
EX_VER=""

USER=$(whoami)

INPUT="/user/${USER}/data/input.txt"
OUTPUT="/user/${USER}/output"

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

LOG="ex${EX_NUM}/log.txt"
touch $LOG

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
	echo "Deploy Mode: (def=1)"
	echo "1: Client"
	echo "2: Cluster"
	read DEPMODE
	if [[ -z "$DEPMODE" ]]; then
		DEPMODE="client"
		echo "Deploy = $DEPMODE"
		echo
		break
	elif [[ $DEPMODE == "1" ]]; then
		DEPMODE="client"
		echo "Deploy = $DEPMODE"
		echo
		break
	elif [[ $DEPMODE == "2" ]]; then
		DEPMODE="cluster"
		echo "Deploy = $DEPMODE"
		echo
		break
	else
		echo "Enter only 1 or 2.."
	fi
done

while [[ true ]]; do
	echo "Master: (def=1)"
	echo "1: Yarn"
	echo "2: Local"
	read MASTER
	if [[ -z "$MASTER" ]]; then
		MASTER="yarn"
		echo "Master = $MASTER"
		echo
		break
	elif [[ $MASTER == "1" ]]; then
		MASTER="yarn"
		echo "Master = $MASTER"
		echo
		break
	elif [[ $MASTER == "2" ]]; then
		MASTER="local"
		echo "Master = $MASTER"
		echo
		break
	else
		echo "Enter only 1 or 2.."
	fi
done

echo "Insert input path: (def=$INPUT)"
read TMP

if [[ -n "$TMP" ]]; then
	INPUT="$TMP"
fi

echo "Input = $INPUT"
echo

echo "Insert output: (def = $OUTPUT)"
read TMP

if [[ -n "$TMP" ]]; then
	OUTPUT="$TMP"
fi

echo "Output = $OUTPUT"
echo

echo "Starting..."

# Remove directory relative to the exercise
echo
ACT="hdfs dfs -rm -r -f /user/${USER}/*"
echo $ACT
echo
RES=$( $ACT >> $LOG )

# Copy into the directory the directory with data files
echo
ACT="hdfs dfs -put ex${EX_NUM}/data /user/${USER}"
echo $ACT
echo
RES=$( $ACT >> $LOG )

# Run application
echo
ACT="spark2-submit --class it.polito.bigdata.spark.ex${EX_NUM}.MyDriver --deploy-mode $DEPMODE --master $MASTER ex${EX_NUM}/ex${EX_NUM}-${EX_VER}.jar $INPUT $OUTPUT"
echo "$ACT"
echo
RES=$( $ACT >> $LOG )

if [[ $? -ne 0 ]]; then
	echo
	echo
	echo "Some error occurs: No clean operation will be done in order to make further analysis!"
	exit 1
fi

echo
echo
echo "LOG"
cat $LOG
echo
echo "---------------"
echo
echo "OUTPUT FILEs"
hdfs dfs -ls -h $OUTPUT
echo
echo "OUTPUT CONTENT"
echo
for filename in $( hdfs dfs -ls -C ${OUTPUT}/*); do
	echo $filename
	echo "---------------"
	hdfs dfs -cat $filename
	echo
done

# Remove directory relative to the exercise
hdfs dfs -rm -r -f /user/${USER}/*

echo
echo
echo
