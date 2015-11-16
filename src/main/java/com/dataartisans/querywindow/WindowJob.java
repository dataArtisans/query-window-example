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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class WindowJob {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);

		String zookeeper = params.get("zookeeper", "localhost:2181");
		String brokers = params.get("brokers", "localhost:9092");
		String sourceTopic = params.getRequired("source");
		String queryTopic = params.getRequired("query");
		String sinkTopic = params.getRequired("sink");
		Long windowSize = params.getLong("window-size", 10_000);
		Long checkpointInterval = params.getLong("checkpoint", 1000);
		String statePath = params.getRequired("state-path");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(checkpointInterval);
		env.setStateBackend(new FsStateBackend(statePath));

		Properties props = new Properties();
		props.setProperty("zookeeper.connect", zookeeper);
		props.setProperty("bootstrap.servers", brokers);
		props.setProperty("group.id", "window-query-example");
		props.setProperty("auto.commit.enable", "false");
		props.setProperty("auto.offset.reset", "largest");

		DataStream<Long> inputStream = env
				.addSource(new FlinkKafkaConsumer<>(sourceTopic,
						new SimpleLongSchema(),
						props,
						FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER,
						FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL));

		KeyedStream<Long, Long> queryStream = env
				.addSource(new FlinkKafkaConsumer<>(queryTopic,
						new SimpleLongSchema(),
						props,
						FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER,
						FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL))
				.keyBy(new KeySelector<Long, Long>() {
					@Override
					public Long getKey(Long value) throws Exception {
						return value;
					}
				});


		KeyedStream<Tuple2<Long, Long>, Long> withOne = inputStream.map(new MapFunction<Long, Tuple2<Long, Long>>() {
			@Override
			public Tuple2<Long, Long> map(Long value) throws Exception {
					return Tuple2.of(value, 1L);
			}
		})
				.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
					@Override
					public Long getKey(Tuple2<Long, Long> value) throws Exception {
						return value.f0;
					}
				});

		TupleTypeInfo<Tuple2<Long, Long>> resultType =
				new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

		DataStream<WindowResult<Long, Tuple2<Long, Long>>> result = withOne.connect(queryStream)
				.transform("Query Window",
						new WindowResultType<>(resultType, BasicTypeInfo.LONG_TYPE_INFO),
						new QueryableWindowOperator(windowSize));

		result.addSink(new FlinkKafkaProducer<>(brokers, sinkTopic, new WindowResultLongLongSchema())).disableChaining();

		env.execute("Query Window Example");
	}

}
