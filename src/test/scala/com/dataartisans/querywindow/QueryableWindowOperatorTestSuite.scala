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

package com.dataartisans.querywindow

import akka.actor.{Props, ActorSystem}
import akka.pattern.ask
import akka.testkit.TestKit
import com.dataartisans.querywindow.QueryMessages.QueryState
import org.apache.curator.test.TestingServer
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.akka.{AkkaUtils, ListeningBehaviour}
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.runtime.messages.JobManagerMessages.{CancelJob, SubmitJob}
import org.apache.flink.runtime.testingUtils.TestingUtils
import org.apache.flink.runtime.testutils.JobManagerActorTestUtils
import org.apache.flink.test.util.TestBaseUtils
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSuiteLike}
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class QueryableWindowOperatorTestSuite(_system: ActorSystem)
  extends TestKit(_system)
  with FunSuiteLike
  with Matchers
  with BeforeAndAfterAll {

  val timeout = 20 seconds

  val numberTaskManager = 2
  val numberSlots = 2
  val parallelism = numberTaskManager * numberSlots

  val config: Configuration = new Configuration

  config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numberTaskManager)
  config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numberSlots)

  val cluster = TestBaseUtils.startCluster(config, StreamingMode.STREAMING, false)

  val zkServer = new TestingServer(true)

  def this() = this(ActorSystem("TestingActorSystem", AkkaUtils.getDefaultAkkaConfig))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)

    TestBaseUtils.stopCluster(cluster, timeout)
  }

  test("QueryableWindow test") {
    val windowSize = 1000

    val leader = cluster.getLeaderGateway(timeout)

    val zooKeeperConfig = ZooKeeperConfiguration(rootPath = "/test", zkQuorum = zkServer.getConnectString())

    val registrationService = new ZooKeeperRegistrationService(zooKeeperConfig)
    val retrievalService = new ZooKeeperRetrievalService[Long](zooKeeperConfig)
    retrievalService.start()

    val job = TestJob.getTestJob(parallelism, windowSize, registrationService)

    leader.tell(SubmitJob(job, ListeningBehaviour.DETACHED))

    val queryActor = system.actorOf(QueryActor.props[Long](retrievalService))

    while (true) {
      val state = Random.nextInt(10)
      val futureResult = (queryActor ? QueryState[Long](state))(timeout)

      val result = Await.result(futureResult, timeout)

      println(result)
    }



    leader.tell(CancelJob(job.getJobID()))

    JobManagerActorTestUtils.waitForJobStatus(
      job.getJobID,
      JobStatus.CANCELED,
      leader,
      TestingUtils.TESTING_DURATION)
  }
}
