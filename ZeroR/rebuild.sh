#!/bin/bash

USER=`whoami`

export HADOOP_HOME=/opt/mapr/hadoop/hadoop-0.20.2
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/Linux-amd64-64
export CLASSPATH=$HADOOP_HOME/lib/*:$HADOOP_HOME/*:/user/$USER/bank/ZeroR/*
export HADOOP_CLASSPATH=$CLASSPATH

javac -d classes BankZRMapper.java
javac -d classes BankZRReducer.java
jar -cvf BankZR.jar -C classes/ .
javac -classpath $CLASSPATH:BankZR.jar -d classes BankZRDriver.java
jar -cvf BankZR.jar -C classes/ .
javac -classpath $CLASSPATH:BankZR.jar -d classes BankZRTest.java
jar -cvf BankZR.jar -C classes/ .
