/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rpc

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.RpcUtils

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * 远程[[RpcEndpoint]]的引用。[[RpcEndpointRef]]是线程安全的。
  *
  * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
  */
private[spark] abstract class RpcEndpointRef(conf: SparkConf)
    extends Serializable with Logging {

    private[this] val maxRetries = RpcUtils.numRetries(conf)
    private[this] val retryWaitMs = RpcUtils.retryWaitMs(conf)
    private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

    /**
      * return the address for the [[RpcEndpointRef]]
      */
    def address: RpcAddress

    def name: String

    /**
      * 发送单向异步消息。即发即弃语义。
      *
      * Sends a one-way asynchronous message. Fire-and-forget semantics.
      */
    def send(message: Any): Unit

    /**
      * 向相应的[[RpcEndpoint.receiveAndReply）]]发送消息，并返回[[Future]]以在指定的超时内接收回复。
      *
      * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
      * receive the reply within the specified timeout.
      *
      * This method only sends the message once and never retries.
      */
    def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

    /**
      * 向相应的[[RpcEndpoint.receiveAndReply）]]发送消息，并向返回[[Future]]在默认超时内接收回复。
      *
      * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
      * receive the reply within a default timeout.
      *
      * This method only sends the message once and never retries.
      */
    def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

    /**
      * 向相应的[[RpcEndpoint.receiveAndReply]]发送消息，并在默认超时，如果失败则抛出异常。
      *
      * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
      * default timeout, throw an exception if this fails.
      *
      * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
      * loop of [[RpcEndpoint]].
      *
      * @param message the message to send
      * @tparam T type of the reply message
      * @return the reply message from the corresponding [[RpcEndpoint]]
      */
    def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)

    /**
      * 向相应的[[RpcEndpoint.receiveAndReply]]发送消息，并在指定的超时，如果失败则引发异常。
      *
      * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
      * specified timeout, throw an exception if this fails.
      *
      * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
      * loop of [[RpcEndpoint]].
      *
      * @param message the message to send
      * @param timeout the timeout duration
      * @tparam T type of the reply message
      * @return the reply message from the corresponding [[RpcEndpoint]]
      */
    def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
        val future = ask[T](message, timeout)
        timeout.awaitResult(future)
    }

}
