#!/bin/bash

USER=`whoami`

export HADOOP_HOME=/opt/mapr/hadoop/hadoop-0.20.2
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/Linux-amd64-64
export CLASSPATH=$HADOOP_HOME/lib/*:$HADOOP_HOME/*:/user/$USER/bank/OneR/*
export HADOOP_CLASSPATH=$CLASSPATH

javac -d classes BankORMapper.java
javac -d classes BankORReducer.java
jar -cvf BankOR.jar -C classes/ .
javac -classpath $CLASSPATH:BankOR.jar -d classes BankORDriver.java
jar -cvf BankOR.jar -C classes/ .

