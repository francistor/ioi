#!/bin/bash

KAFKA_HOME=$HOME/kafka_2.12-2.5.0

# Stop Kafka
kafkaPID=$(ps -def|grep java|grep kafka\.Kafka|awk '{print $2}')
if [ -n "$kafkaPID" ]
then
  echo killing kafka on $kafkaPID
  kill -s TERM $kafkaPID;
fi

# Stop zookeeper
zookeeperPID=$(ps -def|grep java|grep zookeeper.properties|awk '{print $2}')
if [ -n "$zookeeperPID" ]
then
  echo killing zookeeper on $zookeeperPID
  kill -s TERM $zookeeperPID
fi
