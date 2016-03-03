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
import akka.pattern.Patterns;
import akka.testkit.JavaTestKit;
import akka.util.Timeout;
import com.dataartisans.querycommon.RegistrationService;
import com.dataartisans.querycommon.RetrievalService;
import com.dataartisans.querycommon.actors.QueryActor;
import com.dataartisans.querycommon.messages.QueryState;
import com.dataartisans.querycommon.zookeeper.ZooKeeperConfiguration;
import com.dataartisans.querycommon.zookeeper.ZooKeeperRegistrationService;
import com.dataartisans.querycommon.zookeeper.ZooKeeperRetrievalService;
import com.typesafe.config.Config;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testutils.JobManagerActorTestUtils;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;
import scala.Some;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TestQueryableWindowOperator {

	private static ActorSystem actorSystem;
	private static TestingServer zkServer;
	private static ForkableFlinkMiniCluster cluster;
	private static Configuration config = new Configuration();

	private static int numberTaskManager = 2;
	private static int numberSlots = 2;
	private static int parallelism = numberTaskManager * numberSlots;
	private static long windowSize = 1000;
	private static long cleanupDelay = 100;

	private static int queryAttempts = 10;
	private static int maxTimeoutsUntilRefresh = 3;
	private static FiniteDuration queryTimeout = new FiniteDuration(1, TimeUnit.SECONDS);
	private static FiniteDuration lookupTimeout = new FiniteDuration(10, TimeUnit.SECONDS);

	private static FiniteDuration timeout = new FiniteDuration(20, TimeUnit.SECONDS);

	@BeforeClass
	public static void setup() throws Exception {
		Option<Tuple2<String, Object>> remoting = new Some<>(new scala.Tuple2<String, Object>("", 0));

		Config akkaConfig = AkkaUtils.getAkkaConfig(config, remoting);
		actorSystem = ActorSystem.create("TestingActorSystem", akkaConfig);

		zkServer = new TestingServer(true);

		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numberTaskManager);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numberSlots);

		cluster = TestBaseUtils.startCluster(config, false);
	}

	@AfterClass
	public static void teardown() throws Exception {
		if (actorSystem != null ) {
			JavaTestKit.shutdownActorSystem(actorSystem);
			actorSystem = null;
		}

		if (zkServer != null) {
			zkServer.close();
			zkServer = null;
		}

		if (cluster != null) {
			TestBaseUtils.stopCluster(cluster, timeout);
		}
	}

	@Test
	public void testQuerywableWindowOperator() throws Exception {
		ActorGateway leader = cluster.getLeaderGateway(timeout);

		ZooKeeperConfiguration zooKeeperConfiguration = new ZooKeeperConfiguration(
			"/test",
			zkServer.getConnectString());

		RegistrationService registrationService = new ZooKeeperRegistrationService(zooKeeperConfiguration);
		RetrievalService<Long> retrievalService = new ZooKeeperRetrievalService<>(zooKeeperConfiguration);

		JobGraph job = TestJob.getTestJob(parallelism, windowSize, cleanupDelay, registrationService);

		leader.tell(new JobManagerMessages.SubmitJob(job, ListeningBehaviour.DETACHED));

		ActorRef queryActor = actorSystem.actorOf(
				Props.create(
						QueryActor.class,
						retrievalService,
						lookupTimeout,
						queryTimeout,
						queryAttempts,
						maxTimeoutsUntilRefresh),
				"QueryActor");

		Random rnd = new Random();

		boolean continueQuery = true;
		int counter = 0;

		while (continueQuery) {
			long state = rnd.nextInt(10);

			// TODO: integrate window here
			Future<Object> futureResult = Patterns.ask(queryActor, new QueryState<Long>(-9223372036854775808L, state), new Timeout(timeout));

			Object result = Await.result(futureResult, timeout);

			System.out.println(result);

			counter++;

			continueQuery = counter < 10000;
		}

		leader.tell(new JobManagerMessages.CancelJob(job.getJobID()));

		JobManagerActorTestUtils.waitForJobStatus(
			job.getJobID(),
			JobStatus.CANCELED,
			leader,
			timeout
		);
	}
}
