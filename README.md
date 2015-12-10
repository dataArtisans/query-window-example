# Queryable Window Example

This example implements a window operator that allows querying the current window state
using Akka messages. The regular input stream contains Long values, the window
operator basically does WordCount on these Long values, i.e. how often does a value occur
in the given time window. Each window operator starts an Akka actor which is responsible to answer
the state queries. In order to find the actors, each actor registers at ZooKeeper with their 
partition ID. Given the key of the queried state, it is possible to calculate to which partition
it belongs. Consequently, one can directly send the state request to the TaskManager which also 
keeps the respective state.

In order to query the state easily, the QueryActor was developed. The QueryActor awaits 
QueryState messages and responds with a QueryResponse. The query response can either be a 
StateFound message with the key and state value or a StateNotFound message. The latter message
indicates that the partition does not yet contain a state value for the given key. In case that the 
QueryActor cannot find the right TaskManager, the state query times out.

Instructions:
 - Change state backend in WindowJob.java:
   - this line: env.setStateBackend(new FsStateBackend("hdfs://aljoscha-bdutil-m:8020/flink-checkpoints"));
 - Setup Flink, ZooKeeper, Kafka
 - Then, start WindowJob and DataGenerator
 - In order to query the state, start AkkaStateQuery and type in the keys of the requested state 

Common parameters:
 - zookeeper: comma-separated list of the ZooKeeper nodes
 - brokers: comma-separated list of Kafka broker nodes

DataGenerator parameters:
 - sink: the topic to write to
 - num-keys: the amount of unique keys generated
 - sleep: the duration in milliseconds to sleep between element emission

WindowJob parameters:
 - source: source topic
 - sink: sink topic
 - zkPath: ZooKeeper path where the actors of the queryable window operator register themselves
 - window-size: size of the (event-time) window in milliseconds (default: 10000)
 - window-cleanup-delay: the amount of time to keep emitted windows in internal state (default: 2000)
 - checkpoint: checkpoint interval in milliseconds (default: 1000)
 - state-path: path (for example in HDFS) where checkpoints are stored
 
AkkaStateQuery parameters:
 - zkPath: ZooKeeper path where the actors of the queryable window operator register
 - lookupTimeout: Timeout for the ActorRef resolution
 - queryTimeout: Timeout for the queries from the QueryActor to the ResponseActor (running in the 
 queryable window operator)
 - queryAttempts: Number of query attempts before the state query fails.
 - maxTimeouts: Number of ask timeouts before the actor cache is refreshed (this causes the retrieval of akka URLs from the registry and their resolution to ActorRefs)

(In all these, adapt parameters to your environment.)

Setup Kafka Topics:

    kafka-topics.sh --create --zookeeper aljoscha-bdutil-w-5:2181 --partitions 4 --replication-factor 2 --topic regular-input
    kafka-topics.sh --create --zookeeper aljoscha-bdutil-w-5:2181 --partitions 4 --replication-factor 2 --topic window-result

Run Flink Jobs:

    bin/flink run -p 4 -c com.dataartisans.querywindow.WindowJob ../query-window-0.1.jar --zookeeper aljoscha-bdutil-w-5:2181,aljoscha-bdutil-w-6:2181,aljoscha-bdutil-w-7:2181 --brokers aljoscha-bdutil-w-5:6667,aljoscha-bdutil-w-6:6667,aljoscha-bdutil-w-7:6667 --source regular-input --sink window-result --zkPath /akkaQuery --window-size 600000 --checkpoint 10000 --state-path "hdfs://aljoscha-bdutil-m:8020/flink-checkpoints"

    bin/flink run -p 4 -c com.dataartisans.querywindow.DataGenerator ../query-window-0.1.jar --zookeeper aljoscha-bdutil-w-5:2181,aljoscha-bdutil-w-6:2181,aljoscha-bdutil-w-7:2181 --brokers aljoscha-bdutil-w-5:6667,aljoscha-bdutil-w-6:6667,aljoscha-bdutil-w-7:6667 --sink regular-input --sleep 0 --num-keys 64000000
    
Run AkkaStateQuery:

    java -jar target/state-query-0.1.jar --zookeeper node1:2181 --zkPath /akkaQuery --lookupTimeout "10 seconds" --queryTimeout "5 seconds" --queryAttempts 10 --maxTimeouts 8
    
The repl can be terminated by typing `stop` or `quit`.
