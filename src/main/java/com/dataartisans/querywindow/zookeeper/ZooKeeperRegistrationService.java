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

package com.dataartisans.querywindow.zookeeper;

import com.dataartisans.querywindow.RegistrationService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.Serializable;

public class ZooKeeperRegistrationService implements RegistrationService, Serializable {

	private final ZooKeeperConfiguration configuration;
	private transient CuratorFramework client;

	public ZooKeeperRegistrationService(ZooKeeperConfiguration configuration) {
		this.configuration = configuration;
	}

	@Override
	public void start() {
		client = ZooKeeperUtils.startCuratorFramework(
			configuration.getRootPath(),
			configuration.getZkQuorum(),
			configuration.getSessionTimeout(),
			configuration.getConnectionTimeout(),
			configuration.getRetryWait(),
			configuration.getMaxRetryAttempts());
	}

	@Override
	public void stop() {
		if (client != null) {
			client.close();
			client = null;
		}
	}

	@Override
	public void registerActor(int partition, String actorURL) throws Exception {
		if (client != null) {
			boolean success = false;

			while (!success) {
				try {
					if (client.checkExists().forPath("/" + partition) != null) {
						client.setData().forPath("/" + partition, actorURL.getBytes());

						success = true;
					} else {
						client
							.create()
							.creatingParentsIfNeeded()
							.withMode(CreateMode.EPHEMERAL)
							.forPath("/" + partition, actorURL.getBytes());

						success = true;
					}
				} catch (KeeperException.NoNodeException ex) {
					// ignore and try again
				} catch (KeeperException.NodeExistsException ex) {
					// ignore and try again
				}
			}
		} else {
			throw new RuntimeException("CuratorFramework client has not been initialized.");
		}
	}
}
