#!/bin/bash

USER=`whoami`

export HADOOP_HOME=/opt/mapr/hadoop/hadoop-0.20.2
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/Linux-amd64-64
export CLASSPATH=$HADOOP_HOME/lib/*:$HADOOP_HOME/*:/user/$USER/bank/ZeroR/*
export HADOOP_CLASSPATH=$CLASSPATH

if [ $# -ne 2 ]
then
   echo "usage $0 <map|reduce> <testfile>"
   exit 1
fi

if [ ! -f "$2" ]
then
   echo "file $2 does not exist -- exiting"
   exit 1
fi

hadoop1 jar BankZR.jar BankZR.BankZRTest $1 $2
