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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.dataartisans.querycommon.QueryableKeyValueState;
import com.dataartisans.querycommon.RegistrationService;
import com.dataartisans.querycommon.WrongKeyPartitionException;
import com.dataartisans.querycommon.actors.ResponseActor;
import com.typesafe.config.Config;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Some;
import scala.concurrent.duration.FiniteDuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class QueryableWindowOperator
		extends AbstractStreamOperator<Tuple2<Long, Long>>
		implements OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>>, Triggerable, QueryableKeyValueState<Long, Long> {

	private static final Logger LOG = LoggerFactory.getLogger(QueryableWindowOperator.class);

	private final Long windowSize;
	private Map<Long, Long> state;

	private final FiniteDuration timeout = new FiniteDuration(20, TimeUnit.SECONDS);
	private final RegistrationService registrationService;

	private ActorSystem actorSystem;

	public QueryableWindowOperator(
			Long windowSize,
			RegistrationService registrationService) {
		this.windowSize = windowSize;
		this.registrationService = registrationService;
	}

	@Override
	public void open() throws Exception {
		super.open();

		LOG.info("Opening QueryableWindowOperator {}." , this);

		state = new HashMap<>();
		registerTimer(System.currentTimeMillis() + windowSize, this);

		registrationService.start();

		String hostname = registrationService.getConnectingHostname();

		Configuration config = new Configuration();
		Option<scala.Tuple2<String, Object>> remoting = new Some<>(new scala.Tuple2<String, Object>(hostname, 0));

		Config akkaConfig = AkkaUtils.getAkkaConfig(config, remoting);

		LOG.info("Start actory system.");
		actorSystem = ActorSystem.create("queryableWindow", akkaConfig);

		ActorRef responseActor = actorSystem.actorOf(Props.create(ResponseActor.class, this), "responseActor");

		String akkaURL = AkkaUtils.getAkkaURL(actorSystem, responseActor);

		registrationService.registerActor(getRuntimeContext().getIndexOfThisSubtask(), akkaURL);
	}

	@Override
	public void close() throws Exception {
		LOG.info("Closing QueyrableWindowOperator {}.", this);
		super.close();

		registrationService.stop();

		if (actorSystem != null) {
			actorSystem.shutdown();
			actorSystem.awaitTermination(timeout);

			actorSystem = null;
		}
	}

	@Override
	public void processElement(StreamRecord<Tuple2<Long, Long>> streamRecord) throws Exception {
		Long key = streamRecord.getValue().f0;

		Long previous = state.get(key);
		if (previous == null) {
			state.put(key, 1L);
		} else {
			state.put(key, previous + 1L);
		}
	}

	@Override
	public void processWatermark(Watermark watermark) throws Exception {}


	@Override
	public void trigger(long l) throws Exception {

		StreamRecord<Tuple2<Long, Long>> result = new StreamRecord<>(null, System.currentTimeMillis());

		for (Map.Entry<Long, Long> value: state.entrySet()) {
			Tuple2<Long, Long> resultTuple = Tuple2.of(value.getKey(), value.getValue());
			output.collect(result.replace(resultTuple));
		}

		state.clear();

		registerTimer(System.currentTimeMillis() + windowSize, this);
	}

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		StateBackend.CheckpointStateOutputView out = getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

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
	public void restoreState(StreamTaskState taskState, long recoveryTimestamp) throws Exception {
		super.restoreState(taskState, recoveryTimestamp);

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

	@Override
	public Long getValue(Long key) throws WrongKeyPartitionException {
		if (key.hashCode() % getRuntimeContext().getNumberOfParallelSubtasks() != getRuntimeContext().getIndexOfThisSubtask()) {
			throw new WrongKeyPartitionException("Key " + key + " is not part of the partition " +
					"of subtask " + getRuntimeContext().getIndexOfThisSubtask());
		}

		if (state != null) {
			Long result = state.get(key);

			return result == null ? 0 : result;
		} else {
			return null;
		}
	}

	@Override
	public String toString() {
		RuntimeContext ctx = getRuntimeContext();

		return ctx.getTaskName() + " (" + ctx.getIndexOfThisSubtask() + "/" + ctx.getNumberOfParallelSubtasks() + ")";
	}
}
