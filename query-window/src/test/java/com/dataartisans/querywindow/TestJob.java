/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.querywindow;

import com.dataartisans.querycommon.RegistrationService;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestJob {
	public static JobGraph getTestJob(
		int parallelism,
		long windowSize,
		long cleanupDelay,
		RegistrationService registrationService) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		DataStream<Long> inputStream = env.addSource(new DataGenerator.InfiniteLongSource(10, 100));

		KeyedStream<Tuple2<Long, Long>, Long> keyedStream = inputStream.map(new MapFunction<Long, Tuple2<Long, Long>>() {
			@Override
			public Tuple2<Long, Long> map(Long value) throws Exception {
				return Tuple2.of(value, 1L);
			}
		}).keyBy(new KeySelector<Tuple2<Long,Long>, Long>() {
			@Override
			public Long getKey(Tuple2<Long, Long> value) throws Exception {
				return value.f0;
			}
		});

		TupleTypeInfo<Tuple2<Long, Long>> resultType =
			new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

		DataStream<Tuple2<Long, Long>> result = keyedStream
			.transform(
				"Query window",
				resultType,
				new QueryableWindowOperator(windowSize, cleanupDelay, registrationService));

		result.print();

		return env.getStreamGraph().getJobGraph();
	}
}
