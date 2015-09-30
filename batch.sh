#!/bin/zsh

RUNS=$1
echo Runs: $RUNS
WAIT=$2
echo Wait: $WAIT
FILE=$3
echo File: $FILE
HOST=$4
echo Host: $HOST
PORT=$5
echo Port: $PORT

for i in {1..$RUNS}
do
	echo Run: $i
	cat $FILE | netcat -c $HOST $PORT
	sleep $WAIT
done