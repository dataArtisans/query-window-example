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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import java.util.HashMap;
import java.util.Map;

class QueryableWindowOperator
		extends AbstractStreamOperator<WindowResult<Long, Tuple2<Long, Long>>>
		implements TwoInputStreamOperator<Tuple2<Long, Long>, Long, WindowResult<Long, Tuple2<Long, Long>>>, Triggerable {

	private final Long windowSize;
	private Map<Long, Long> state;

	public QueryableWindowOperator(Long windowSize) {
		this.windowSize = windowSize;
	}

	@Override
	public void open() throws Exception {
		super.open();
		state = new HashMap<>();
		registerTimer(System.currentTimeMillis() + windowSize, this);
	}

	@Override
	public void processElement1(StreamRecord<Tuple2<Long, Long>> streamRecord) throws Exception {
		Long key = streamRecord.getValue().f0;

		Long previous = state.get(key);
		if (previous == null) {
			state.put(key, 1L);
		} else {
			state.put(key, previous + 1L);
		}
	}

	@Override
	public void processElement2(StreamRecord<Long> streamRecord) throws Exception {
		Long key = streamRecord.getValue();

		Long result = state.get(key);
		if (result != null) {
			Tuple2<Long, Long> resultTuple = Tuple2.of(key, result);
			WindowResult<Long, Tuple2<Long, Long>> windowResult = WindowResult.queryResult(key, resultTuple);
			output.collect(new StreamRecord<>(windowResult, streamRecord.getTimestamp()));
		} else {
			output.collect(new StreamRecord<>(WindowResult.<Tuple2<Long, Long>, Long>queryResult(key, null), streamRecord.getTimestamp()));
		}
	}


	@Override
	public void trigger(long l) throws Exception {

		StreamRecord<Tuple2<Long, Long>> result = new StreamRecord<>(null, System.currentTimeMillis());

		for (Map.Entry<Long, Long> value: state.entrySet()) {
			Tuple2<Long, Long> resultTuple = Tuple2.of(value.getKey(), value.getValue());
			output.collect(result.replace(WindowResult.<Tuple2<Long, Long>, Long>regularResult(resultTuple)));
		}

		state.clear();

		registerTimer(System.currentTimeMillis() + windowSize, this);
	}

	@Override
	public void processWatermark1(Watermark watermark) throws Exception {

	}

	@Override
	public void processWatermark2(Watermark watermark) throws Exception {

	}

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		AbstractStateBackend.CheckpointStateOutputView out = getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

		int numKeys = state.size();
		out.writeInt(numKeys);

		for (Map.Entry<Long, Long> value: state.entrySet()) {
			out.writeLong(value.getKey());
			out.writeLong(value.getValue());
		}

		taskState.setOperatorState(out.closeAndGetHandle());
		return taskState;
	}

	@Override
	public void restoreState(StreamTaskState taskState) throws Exception {
		super.restoreState(taskState);


		@SuppressWarnings("unchecked")
		StateHandle<DataInputView> inputState = (StateHandle<DataInputView>) taskState.getOperatorState();
		DataInputView in = inputState.getState(getUserCodeClassloader());

		int numKeys = in.readInt();

		this.state = new HashMap<>();

		for (int i = 0; i < numKeys; i++) {
			long key = in.readLong();
			long value = in.readLong();
			state.put(key, value);
		}
	}
}
