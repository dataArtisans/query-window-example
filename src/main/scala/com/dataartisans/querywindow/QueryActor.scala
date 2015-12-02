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

import akka.pattern.ask
import akka.pattern.pipe
import akka.actor.{Props, Actor}
import com.dataartisans.querywindow.QueryMessages.{QueryResponse, QueryState}

import scala.concurrent.Future
import scala.concurrent.duration._

class QueryActor[K](val retrievalService: RetrievalService[K]) extends Actor {

  implicit val ec = context.dispatcher

  val timeout = 3 seconds
  val maxRetries = 5

  retrievalService.start()

  override def receive: Receive = {
    case QueryState(key: K) =>
      val result = retry(maxRetries)(queryState(key))

      result.pipeTo(sender())
  }


  def queryState(key: K): Future[QueryResponse[K, _]] = {
    val actorURLOption = retrievalService.retrieveActorURL(key)

    actorURLOption match {
      case Some(actorURL) =>
        println("Resolved to actorURL: " + actorURL)
        println("Query for key: " + key)
        (context.system.actorSelection(actorURL) ? QueryState[K](key))(timeout).mapTo[QueryResponse[K, _]].recoverWith{
          case e =>
            Thread.sleep(100)
            retrievalService.refreshActorCache()
            Future.failed(e)
        }
      case None =>
        Future {
          Thread.sleep(100)
          retrievalService.refreshActorCache()
          throw new RuntimeException("Could not retrieve actor for state with " +
          "key " + key + ".")
        }
    }
  }

  def retry(tries: Int)(future : => Future[QueryResponse[K, _]]): Future[QueryResponse[K, _]] = {
    future.recoverWith{
      case e if (tries > 0) =>
        retry(tries - 1)(future)
      case e => Future.failed(e)
    }
  }
}

object QueryActor {
  def props[K](retrievalService: RetrievalService[K]): Props = {
    Props(new QueryActor[K](retrievalService))
  }
}
