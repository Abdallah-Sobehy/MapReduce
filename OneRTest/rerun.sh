#!/bin/bash

USER=`whoami`

export HADOOP_HOME=/opt/mapr/hadoop/hadoop-0.20.2
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/Linux-amd64-64
export CLASSPATH=$HADOOP_HOME/lib/*:$HADOOP_HOME/*:/user/$USER/bank/OneRTest/*
export HADOOP_CLASSPATH=$CLASSPATH

rm -rf /user/$USER/bank/OneRTest/OUT

ARGS=$1

hadoop jar BankOR.jar BankOR.BankORDriver $ARGS /user/$USER/bank/OneRTest/DATA/bankTestSet40.csv /user/$USER/bank/OneRTest/OUT
