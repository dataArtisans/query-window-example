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

package com.dataartisans.statequery;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.dataartisans.querycommon.AkkaUtils;
import com.dataartisans.querycommon.RetrievalService;
import com.dataartisans.querycommon.actors.QueryActor;
import com.dataartisans.querycommon.messages.QueryState;
import com.dataartisans.querycommon.zookeeper.ZooKeeperConfiguration;
import com.dataartisans.querycommon.zookeeper.ZooKeeperRetrievalService;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.Scanner;

public class AkkaStateQuery {
	private static final Logger LOG = LoggerFactory.getLogger(AkkaStateQuery.class);

	public static void main(String[] args) throws Exception {
		LOG.info("Starting AkkaStateQuery. You can send queries by entering them via the command line.");

		OptionParser parser = new OptionParser();

		OptionSpec<String> zookeeperOption = parser
				.accepts("zookeeper")
				.withRequiredArg()
				.ofType(String.class)
				.defaultsTo("localhost:2181");

		OptionSpec<String> zkPathOption = parser
				.accepts("zkPath")
				.withRequiredArg()
				.ofType(String.class)
				.defaultsTo("/akkaQuery");

		OptionSpec<String> lookupTimeoutOption = parser
				.accepts("lookupTimeout")
				.withRequiredArg()
				.ofType(String.class)
				.defaultsTo("10 seconds");

		OptionSpec<String> queryTimeoutOption = parser
				.accepts("queryTimeout")
				.withRequiredArg()
				.ofType(String.class)
				.defaultsTo("1 seconds");

		OptionSpec<Integer> queryAttemptsOption = parser
				.accepts("queryAttempts")
				.withRequiredArg()
				.ofType(Integer.class)
				.defaultsTo(10);

		OptionSpec<Integer> maxTimeoutsUntilRefreshOption = parser
				.accepts("maxTimeouts")
				.withRequiredArg()
				.ofType(Integer.class)
				.defaultsTo(3);

		OptionSet options = parser.parse(args);

		String zookeeper = zookeeperOption.value(options);
		String zkPath = zkPathOption.value(options);
		String lookupTimeoutStr = lookupTimeoutOption.value(options);
		String queryTimeoutStr = queryTimeoutOption.value(options);
		int queryAttempts = queryAttemptsOption.value(options);
		int maxTimeoutsUntilRefresh = maxTimeoutsUntilRefreshOption.value(options);

		FiniteDuration lookupTimeout;
		FiniteDuration queryTimeout;
		FiniteDuration askTimeout;

		Duration lookupDuration = FiniteDuration.create(lookupTimeoutStr);

		if (lookupDuration instanceof FiniteDuration) {
			lookupTimeout = (FiniteDuration) lookupDuration;
		} else {
			throw new Exception("Lookup timeout has to be finite.");
		}

		Duration duration = FiniteDuration.create(queryTimeoutStr);

		if (duration instanceof FiniteDuration) {
			queryTimeout = (FiniteDuration) duration;
		} else {
			throw new Exception("Query timeout has to be finite.");
		}

		askTimeout = queryTimeout.mul(queryAttempts);

		ZooKeeperConfiguration zooKeeperConfiguration = new ZooKeeperConfiguration(zkPath, zookeeper);

		RetrievalService<Long> retrievalService = new ZooKeeperRetrievalService<>(zooKeeperConfiguration);


		ActorSystem actorSystem = ActorSystem.create("AkkaStateQuery", AkkaUtils.getDefaultAkkaConfig("", 0));

		ActorRef queryActor = actorSystem.actorOf(
			Props.create(
				QueryActor.class,
				retrievalService,
				lookupTimeout,
				queryTimeout,
				queryAttempts,
				maxTimeoutsUntilRefresh),
			"queryActor");

		boolean continueQuery = true;
		Scanner scanner = new Scanner(System.in);

		while (continueQuery) {
			String line = scanner.next().toLowerCase();

			if (line.equals("stop") || line.equals("quit")) {
				continueQuery = false;
			} else {
				try {
					long state = Long.parseLong(line);

					Future<Object> futureResult = Patterns.ask(
						queryActor,
						new QueryState<>(state),
						new Timeout(askTimeout));

					Object result = Await.result(futureResult, askTimeout);

					System.out.println(result);
				} catch (NumberFormatException ex) {
					System.out.println("Could not parse the input " + line + " as a long. " +
						"You can stop the akka query by typing \"stop\" or \"quit\".");
				} catch (Exception e) {
					System.out.println("Could not retrieve state for the requested key.");
					e.printStackTrace();
				}
			}
		}
	}
}
