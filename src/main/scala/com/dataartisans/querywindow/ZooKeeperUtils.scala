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

import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.ExponentialBackoffRetry

object ZooKeeperUtils {
  def startCuratorFramework(
    root: String,
    zkQuorum: String,
    sessionTimeout: Int,
    connectionTimeout: Int,
    retryWait: Int,
    maxRetryAttempts: Int)
  : Option[CuratorFramework] = {

    val result = CuratorFrameworkFactory.builder()
    .connectString(zkQuorum)
    .sessionTimeoutMs(sessionTimeout)
    .connectionTimeoutMs(connectionTimeout)
    .retryPolicy(new ExponentialBackoffRetry(retryWait, maxRetryAttempts))
    // Curator prepends a '/' manually and throws an Exception if the
    // namespace starts with a '/'.
    .namespace(if (root.startsWith("/")) root.substring(1) else root)
    .build();

    result.start();

    Some(result)
  }
}
