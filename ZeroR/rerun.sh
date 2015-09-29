#!/bin/bash

USER=`whoami`

export HADOOP_HOME=/opt/mapr/hadoop/hadoop-0.20.2
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/Linux-amd64-64
export CLASSPATH=$HADOOP_HOME/lib/*:$HADOOP_HOME/*:/user/$USER/bank/ZeroR/*
export HADOOP_CLASSPATH=$CLASSPATH

rm -rf /user/$USER/bank/ZeroR/OUT

ARGS=$1

hadoop jar BankZR.jar BankZR.BankZRDriver $ARGS /user/$USER/bank/ZeroR/DATA/bankTestSet.csv /user/$USER/bank/ZeroR/OUT
