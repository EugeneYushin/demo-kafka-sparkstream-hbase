# demo-kafka-sparkstream-hbase

Demo for loading data from Kafka into HBase table through SparkStreaming. Calculates MIN, MAX, AVG (SUM, CNT) on Minute bases.
Kafka topic: demo-stream-topic
HBase table: demo-log
HBase family: demo-ts-metrics

Input data example (paste exactly into Kafka Producer console):
* `2015-03-01 01:17:12.874,4`
* `2015-03-01 01:17:12.874,6`
* `2015-03-01 01:18:12.874,5`
* `2015-03-01 01:18:12.874,3`
* `2015-03-01 01:18:12.874,2`

Please, delete /tmp/(spark-\*|checpoint\*) data from Driver before execution.


# Kafka

Be sure your ZooKeeper instance is up and running, else start system zoo or embedded one from Kafka.
bin/zookeeper-server-start.sh config/zookeeper.properties

Start Kafka server:
bin/kafka-server-start.sh config/server.properties

Create topic for test purposes:
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic demo-stream-topic

You could find all topics running following command:
bin/kafka-topics.sh --list --zookeeper localhost:2181

We would use embedded console producer/consumer for fetching and debugging messages to know what is going on for simplifying. For more advance profiling there's more convenient to use customized JMeter generator to produce Kafka messages.
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic demo-stream-topic
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic demo-stream-topic --from-beginning


# HBase

Table created through Cloudera Hue gui (HBase Browser). RowKey is TimeStamp value trimmed to Minutes from Stream.


# Simplifications

* We don't use info already loaded into HBase table
* We don't apply changes provided by external applications on HBase table

It's possible to use one more DStream for reading from HBase and union KafkaStream with HBaseStream to fetch external HBase table modifications.
 