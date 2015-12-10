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

import akka.actor.ActorNotFound;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import akka.dispatch.Futures;
import akka.dispatch.Recover;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.dataartisans.querycommon.RetrievalService;
import com.dataartisans.querycommon.WrongKeyPartitionException;
import com.dataartisans.querycommon.messages.QueryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class QueryActor<K extends Serializable> extends UntypedActor {
	private static final Logger LOG = LoggerFactory.getLogger(QueryActor.class);

	private final RetrievalService<K> retrievalService;

	private final FiniteDuration askTimeout;
	private final FiniteDuration lookupTimeout;
	private final int queryAttempts;

	private final Map<K, ActorRef> cache = new HashMap<>();
	private final Object cacheLock = new Object();

	public QueryActor(
			RetrievalService<K> retrievalService,
			FiniteDuration lookupTimeout,
			FiniteDuration queryTimeout,
			int queryAttempts) throws Exception {
		this.retrievalService = retrievalService;
		this.askTimeout = queryTimeout;
		this.lookupTimeout = lookupTimeout;
		this.queryAttempts = queryAttempts;

		retrievalService.start();
	}

	@Override
	public void postStop() throws Exception {
		if (retrievalService != null) {
			retrievalService.stop();
		}
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof QueryState) {
			@SuppressWarnings("unchecked")
			QueryState<K> queryState = (QueryState<K>) message;

			LOG.debug("Query state for key " + queryState.getKey() + ".");

			Future<Object> futureResult = queryStateFutureWithFailover(queryAttempts, queryState);

			Patterns.pipe(futureResult, getContext().dispatcher()).to(getSender());
		}
	}

	public void refreshCache() throws Exception {
		LOG.debug("Refresh local and retrieval service cache.");
		synchronized (cacheLock) {
			cache.clear();
		}

		retrievalService.refreshActorCache();
	}

	public ActorRef getActorRef(K key) throws Exception {
		synchronized (cacheLock) {
			ActorRef result = cache.get(key);

			if(result != null) {
				return result;
			}
		}

		LOG.debug("Retrieve actor URL from retrieval service.");
		String actorURL = retrievalService.retrieveActorURL(key);

		if (actorURL == null) {
			return null;
		} else {
			ActorSelection selection = getContext().system().actorSelection(actorURL);

			try {
				LOG.debug("Resolve actor URL to ActorRef.");
				ActorRef actorRef = Await.result(selection.resolveOne(lookupTimeout), lookupTimeout);

				synchronized (cacheLock) {
					cache.put(key, actorRef);
				}

				return actorRef;
			} catch (ActorNotFound e) {
				return null;
			}
		}
	}

	public Future<Object> queryStateFuture(final QueryState<K> queryState) {
		ActorRef actorRef;

		try {
			LOG.debug("Try to get ActorRef for key " + queryState.getKey());
			actorRef = getActorRef(queryState.getKey());
		} catch (final Exception e) {
			LOG.debug("ActorRef retrieval failed with " + e + ".");
			return Patterns.after(
					askTimeout,
					getContext().system().scheduler(),
					getContext().dispatcher(),
					new Callable<Future<Object>>() {
						@Override
						public Future<Object> call() throws Exception {
							refreshCache();
							return Futures.failed(new Exception("Error occurred while retrieving the ActorRef.", e));
						}
					}
			);
		}

		if (actorRef != null) {
			LOG.debug("Ask response actor for state for key " + queryState.getKey() + ".");
			Future<Object> futureResult = Patterns.ask(actorRef, queryState, new Timeout(askTimeout));

			@SuppressWarnings("unchecked")
			Future<Object> recoveredResult = futureResult.recoverWith(new Recover<Future<Object>>() {
				@Override
				public Future<Object> recover(final Throwable failure) throws Throwable {
					if (failure instanceof WrongKeyPartitionException) {
						LOG.debug("WrongKeyPartitionException");
						return Patterns.after(
								askTimeout,
								getContext().system().scheduler(),
								getContext().dispatcher(),
								new Callable<Future<Object>>() {
									@Override
									public Future<Object> call() throws Exception {
										refreshCache();
										return Futures.failed(failure);
									}
								});
					} else {
						LOG.debug("State query failed with " + failure + ".");
						refreshCache();
						return Futures.failed(failure);
					}

				}
			}, getContext().dispatcher());

			return recoveredResult;
		} else {
			LOG.debug("ActorRef was null. Wait for retry.");
			return Patterns.after(
				askTimeout,
				getContext().system().scheduler(),
				getContext().dispatcher(),
				new Callable<Future<Object>>() {
					@Override
					public Future<Object> call() throws Exception {
						refreshCache();
						return Futures.failed(new Exception("Could not retrieve actor for state with key " + queryState.getKey() + "."));
					}
				});
		}
	}

	public Future<Object> queryStateFutureWithFailover(final int tries, final QueryState<K> queryState) {
		@SuppressWarnings("unchecked")
		Future<Object> result = queryStateFuture(queryState).recoverWith(new Recover<Future<Object>>() {
			@Override
			public Future<Object> recover(Throwable failure) throws Throwable {
				if (tries > 0) {
					LOG.debug("Query state failed with " + failure + ". Try to recover. #" + tries + " left.");
					return queryStateFutureWithFailover(tries - 1, queryState);
				} else {
					return Futures.failed(failure);
				}
			}
		}, getContext().dispatcher());

		return result;
	}
}
