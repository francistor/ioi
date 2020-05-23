#!/bin/bash

rm -rf $HOME/logs
mkdir $HOME/logs > /dev/null 2>&1

KAFKA_HOME=$HOME/kafka_2.12-2.5.0

# Start Kafka
echo launching Zookeeper ...
nohup $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties > $HOME/logs/zookeeper.log 2>&1 &
sleep 3
echo launching Kafka ...
nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > $HOME/logs/kafka.log 2>&1 &
sleep 5

# Create topic
echo creating topic ...
$KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic cdr 2>&1 > /dev/null

# Set retention time
echo setting retention time for 'cdr' topic ...
$KAFKA_HOME/bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --alter --entity-name cdr --add-config retention.ms=60000

sleep 3

# Launch CDRReplay
$HOME/ioi/CDRReplay/target/universal/stage/bin/cdrreplay --fastForwardFactor 1 --dir /home/francisco/data --date 2019-12-04T00-00

echo done.
