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

package com.dataartisans.querycommon.actors;

import akka.actor.Status;
import akka.actor.UntypedActor;
import com.dataartisans.querycommon.QueryableKeyValueState;
import com.dataartisans.querycommon.WrongKeyPartitionException;
import com.dataartisans.querycommon.messages.QueryState;
import com.dataartisans.querycommon.messages.StateFound;
import com.dataartisans.querycommon.messages.StateNotFound;

import java.io.Serializable;

public class ResponseActor<K extends Serializable, V extends Serializable> extends UntypedActor {

	private final QueryableKeyValueState<K, V> keyValueState;

	public ResponseActor(QueryableKeyValueState<K, V> keyValueState) {
		this.keyValueState = keyValueState;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof QueryState) {
			@SuppressWarnings("unchecked")
			QueryState<K> queryState = (QueryState<K>) message;

			try {
				V value = keyValueState.getValue(queryState.getKey());

				if (value == null) {
					sender().tell(new StateNotFound(queryState.getKey()), getSelf());
				} else {
					sender().tell(new StateFound<>(queryState.getKey(), value), getSelf());
				}
			} catch (WrongKeyPartitionException ex) {
				sender().tell(new Status.Failure(ex), getSelf());
			}
		}
	}
}
