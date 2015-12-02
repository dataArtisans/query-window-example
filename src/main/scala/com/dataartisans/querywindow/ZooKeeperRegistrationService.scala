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

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.{KeeperException, CreateMode}

class ZooKeeperRegistrationService(zooKeeperConfig: ZooKeeperConfiguration)
  extends RegistrationService
  with Serializable {
  var clientOption: Option[CuratorFramework] = None

  import ZooKeeperUtils.startCuratorFramework

  override def start(): Unit = {
    import zooKeeperConfig._

    clientOption = startCuratorFramework(
      rootPath,
      zkQuorum,
      sessionTimeout,
      connectionTimeout,
      retryWait,
      maxRetryAttempts
    )
  }

  override def registerActor(partition: Int, actorURL: String): Unit = {
    clientOption match {
      case Some(client) =>
        var success = false

        while(!success) {
          try {
            if (client.checkExists().forPath("/" + partition) != null) {
              client.setData().forPath("/" + partition, actorURL.getBytes)

              success = true
            } else {
              client
              .create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.EPHEMERAL)
              .forPath("/" + partition, actorURL.getBytes())

              success = true
            }
          } catch {
            case _: KeeperException.NoNodeException | _: KeeperException.NodeExistsException =>
              // try again
          }
        }
      case None =>
        throw new RuntimeException("CuratorFramework client has not been initialized.")
    }
  }
}
