#!/bin/sh

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export SCALA_HOME=/usr
export CLASSPATH=".:/opt/spark-latest/jars/*"

echo -n "Enter Task File:"
read RUN_FILE

echo --- Deleting
rm $RUN_FILE.jar
rm $RUN_FILE*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g $RUN_FILE.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf $RUN_FILE.jar $RUN_FILE*.class

echo --- Running
INPUT=/a2_inputs/in3.txt
OUTPUT=/user/${USER}/a2_starter_code_output/

hdfs dfs -rm -R $OUTPUT
time spark-submit --master yarn --class $RUN_FILE $RUN_FILE.jar $INPUT $OUTPUT

hdfs dfs -ls $OUTPUT
