#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export CLASSPATH=`hadoop classpath`

echo -n "Enter Task File:"
read RUN_FILE

echo --- Deleting
rm $RUN_FILE.jar
rm $RUN_FILE*.class

echo --- Compiling
$JAVA_HOME/bin/javac $RUN_FILE.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf $RUN_FILE.jar $RUN_FILE*.class

echo --- Running
INPUT=/a2_inputs/in3.txt
OUTPUT=/user/${USER}/a2_starter_code_output/

hdfs dfs -rm -R $OUTPUT
time yarn jar $RUN_FILE.jar $RUN_FILE $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
