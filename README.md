# Queryable Window Example

This example implements a window operator that allows querying the current window state
using a secondary input stream. The regular input stream contains Long values, the window
operator basically does WordCount on these Long values, i.e. how often does a value occur
in the given time window.

If a query comes in on the second input stream (also a Long value) the current accumulated count
for that value is emitted from the window operator.

Instructions:
 - Change state backend in WindowJob.java:
   - this line: env.setStateBackend(new FsStateBackend("hdfs://aljoscha-bdutil-m:8020/flink-checkpoints"));
 - Setup Flink, ZooKeeper, Kafka
 - Then, start WindowJob and DataGenerator

Common parameters:
 - zookeeper: comma-separated list of the ZooKeeper nodes
 - brokers: comma-separated list of Kafka broker nodes

DataGenerator parameters:
 - sink: the topic to write to
 - num-keys: the amount of unique keys generated
 - sleep: the duration in milliseconds to sleep between element emission

WindowJob parameters:
 - source: source topic
 - query: query topic
 - sink: sink topic
 - window-size: size of the (processing-time) window in milliseconds (default: 10000)
 - checkpoint: checkpoint interval in milliseconds (default: 1000)
 - state-path: path (for example in HDFS) where checkpoints are stored

(In all these, adapt parameters to your environment.)

Setup Kafka Topics:

    kafka-topics.sh --create --zookeeper aljoscha-bdutil-w-5:2181 --partitions 5 --replication-factor 2 --topic regular-input
    kafka-topics.sh --create --zookeeper aljoscha-bdutil-w-5:2181 --partitions 5 --replication-factor 2 --topic query-input
    kafka-topics.sh --create --zookeeper aljoscha-bdutil-w-5:2181 --partitions 5 --replication-factor 2 --topic window-result

Run Flink Jobs:

    bin/flink run -p 5 -c com.dataartisans.querywindow.WindowJob ../query-window-0.1.jar --zookeeper aljoscha-bdutil-w-5:2181,aljoscha-bdutil-w-6:2181,aljoscha-bdutil-w-7:2181 --brokers aljoscha-bdutil-w-5:6667,aljoscha-bdutil-w-6:6667,aljoscha-bdutil-w-7:6667 --source regular-input --sink window-result --query query-input --window-size 600000 --checkpoint 10000 --state-path "hdfs://aljoscha-bdutil-m:8020/flink-checkpoints"

    bin/flink run -p 5 -c com.dataartisans.querywindow.DataGenerator ../query-window-0.1.jar --zookeeper aljoscha-bdutil-w-5:2181,aljoscha-bdutil-w-6:2181,aljoscha-bdutil-w-7:2181 --brokers aljoscha-bdutil-w-5:6667,aljoscha-bdutil-w-6:6667,aljoscha-bdutil-w-7:6667 --sink regular-input --sleep 0 --num-keys 64000000
