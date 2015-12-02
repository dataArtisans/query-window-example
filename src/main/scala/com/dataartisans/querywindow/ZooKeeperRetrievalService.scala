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
import org.apache.zookeeper.KeeperException

class ZooKeeperRetrievalService[K](zooKeeperConfig: ZooKeeperConfiguration) extends RetrievalService[K] {
  var clientOption: Option[CuratorFramework] = None
  var actorMap: Option[Map[Int, String]] = None

  override def start(): Unit = {
    import zooKeeperConfig._
    clientOption = ZooKeeperUtils.startCuratorFramework(
      rootPath,
      zkQuorum,
      sessionTimeout,
      connectionTimeout,
      retryWait,
      maxRetryAttempts)

    refreshActorCache()
  }

  override def retrieveActorURL(key: K): Option[String] = {
    actorMap.flatMap {
      map =>
        val partition = key.hashCode() % map.size
        map.get(partition)
    }
  }

  override def refreshActorCache(): Unit = {
    import scala.collection.JavaConverters._

    clientOption match {
      case Some(client) =>
        val children = client.getChildren.forPath("/")

        val result = children.asScala.flatMap {
          child =>
            try {
              val data = client.getData.forPath("/" + child)

              Some(child.toInt -> new String(data))
            } catch {
              case _: KeeperException.NoNodeException => None
            }
        }.toMap

        if (result.size > 0) {
          actorMap = Some(result)
        } else {
          actorMap = None
        }
      case None =>
        throw new RuntimeException("The CuratorFramework client has not been properly initialized.")
    }
  }
}
