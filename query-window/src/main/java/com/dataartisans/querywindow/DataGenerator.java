/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.querywindow;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class DataGenerator {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);

		String zookeeper = params.get("zookeeper", "localhost:2181");
		String brokers = params.get("brokers", "localhost:9092");
		String sinkTopic = params.getRequired("sink");
		Long numKeys = params.getLong("num-keys", 10_000);
		Integer sleep = params.getInt("sleep", 10);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().enableObjectReuse();

		Properties props = new Properties();
		props.setProperty("zookeeper.connect", zookeeper);
		props.setProperty("bootstrap.servers", brokers);
		props.setProperty("group.id", "window-query-example");
		props.setProperty("auto.commit.enable", "false");
		props.setProperty("auto.offset.reset", "largest");

		DataStream<Long> result = env.addSource(new InfiniteLongSource(numKeys, sleep));

		result.addSink(new FlinkKafkaProducer<>(brokers, sinkTopic, new SimpleLongSchema()));

		env.execute("Query Window Data Generator");
	}

	public static class InfiniteLongSource extends RichParallelSourceFunction<Long> {
		private static final long serialVersionUID = 1L;

		private long numGroups;

		private int sleepInterval;

		private volatile boolean running = true;

		public InfiniteLongSource(long numGroups, int sleepInterval) {
			this.numGroups = numGroups;
			this.sleepInterval = sleepInterval;
		}

		@Override
		public void run(SourceContext<Long> out) throws Exception {

			long step = numGroups / getRuntimeContext().getNumberOfParallelSubtasks();

			long index = getRuntimeContext().getIndexOfThisSubtask() * step;
			while (running) {
				out.collect((index % numGroups));
				Thread.sleep(sleepInterval);
				index++;
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

}
