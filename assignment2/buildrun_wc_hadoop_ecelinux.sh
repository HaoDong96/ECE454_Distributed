#!/bin/sh


export JAVA_HOME
export HADOOP_HOME
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`

export JavaFile=Task4

echo --- Deleting
rm $JavaFile.jar
rm $JavaFile*.class

echo --- Compiling
$JAVA_HOME/bin/javac $JavaFile.java
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf $JavaFile.jar $JavaFile*.class

echo --- Running
INPUT=sample_input/smalldata.txt
OUTPUT=output_hadoop

rm -fr $OUTPUT
time $HADOOP_HOME/bin/hadoop jar $JavaFile.jar $JavaFile $INPUT $OUTPUT

cat $OUTPUT/*
