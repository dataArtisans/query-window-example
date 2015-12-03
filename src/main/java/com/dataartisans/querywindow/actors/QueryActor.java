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

package com.dataartisans.querywindow.actors;

import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import akka.dispatch.Futures;
import akka.dispatch.Recover;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.dataartisans.querywindow.RetrievalService;
import com.dataartisans.querywindow.messages.QueryState;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class QueryActor<K extends Serializable> extends UntypedActor {
	private final RetrievalService<K> retrievalService;

	private final FiniteDuration askTimeout = new FiniteDuration(3, TimeUnit.SECONDS);
	private final int tries = 10;

	public QueryActor(RetrievalService<K> retrievalService) throws Exception {
		this.retrievalService = retrievalService;

		retrievalService.start();
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof QueryState) {
			QueryState<K> queryState = (QueryState<K>) message;

			Future<Object> futureResult = queryStateFutureWithFailover(tries, queryState);

			Patterns.pipe(futureResult, getContext().dispatcher()).to(getSender());
		}
	}

	public Future<Object> queryStateFuture(final QueryState<K> queryState) {
		String actorURL = retrievalService.retrieveActorURL(queryState.getKey());

		if (actorURL != null) {
			ActorSelection actorSelection = getContext().system().actorSelection(actorURL);

			Future<Object> futureResult = Patterns.ask(actorSelection, queryState, new Timeout(askTimeout));

			Future<Object> recoveredResult = futureResult.recoverWith(new Recover<Future<Object>>() {
				@Override
				public Future<Object> recover(Throwable failure) throws Throwable {
					retrievalService.refreshActorCache();
					return Futures.failed(failure);
				}
			}, getContext().dispatcher());

			return recoveredResult;
		} else {
			return Patterns.after(
				askTimeout,
				getContext().system().scheduler(),
				getContext().dispatcher(),
				new Callable<Future<Object>>() {
					@Override
					public Future<Object> call() throws Exception {
						retrievalService.refreshActorCache();
						return Futures.failed(new Exception("Could not retrieve actor for state with key " + queryState.getKey() + "."));
					}
				});
		}
	}

	public Future<Object> queryStateFutureWithFailover(final int tries, final QueryState<K> queryState) {
		return queryStateFuture(queryState).recoverWith(new Recover<Future<Object>>() {
			@Override
			public Future<Object> recover(Throwable failure) throws Throwable {
				if (tries > 0) {
					return queryStateFutureWithFailover(tries - 1, queryState);
				} else {
					return Futures.failed(failure);
				}
			}
		}, getContext().dispatcher());
	}
}
