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

package org.apache.spark.rpc.netty

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

/**
  * 消息调度器，负责将RPC消息路由到适当的端点。
  *
  * A message dispatcher, responsible for routing RPC messages to the appropriate endpoint(s).
  */
private[netty] class Dispatcher(nettyEnv: NettyRpcEnv) extends Logging {

    private class EndpointData(
                                  val name: String,
                                  val endpoint: RpcEndpoint,
                                  val ref: NettyRpcEndpointRef) {
        val inbox = new Inbox(ref, endpoint)
    }

    private val endpoints: ConcurrentMap[String, EndpointData] =
        new ConcurrentHashMap[String, EndpointData]
    private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] =
        new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

    // Track the receivers whose inboxes may contain messages.
    private val receivers = new LinkedBlockingQueue[EndpointData]

    /**
      * 如果调度程序已停止，则为True。一旦停止，所有发布的消息将立即被弹出。
      *
      * True if the dispatcher has been stopped. Once stopped, all messages posted will be bounced
      * immediately.
      */
    @GuardedBy("this")
    private var stopped = false

    def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
        val addr = RpcEndpointAddress(nettyEnv.address, name)
        val endpointRef = new NettyRpcEndpointRef(nettyEnv.conf, addr, nettyEnv)
        synchronized {
            if (stopped) {
                throw new IllegalStateException("RpcEnv has been stopped")
            }
            if (endpoints.putIfAbsent(name, new EndpointData(name, endpoint, endpointRef)) != null) {
                throw new IllegalArgumentException(s"There is already an RpcEndpoint called $name")
            }
            val data = endpoints.get(name)
            endpointRefs.put(data.endpoint, data.ref)
            receivers.offer(data) // for the OnStart message
        }
        endpointRef
    }

    def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

    def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

    // Should be idempotent
    private def unregisterRpcEndpoint(name: String): Unit = {
        val data = endpoints.remove(name)
        if (data != null) {
            data.inbox.stop()
            receivers.offer(data) // for the OnStop message
        }
        // Don't clean `endpointRefs` here because it's possible that some messages are being processed
        // now and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
        // `removeRpcEndpointRef`.
    }

    def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
        synchronized {
            if (stopped) {
                // This endpoint will be stopped by Dispatcher.stop() method.
                return
            }
            unregisterRpcEndpoint(rpcEndpointRef.name)
        }
    }

    /**
      * 向此进程中所有已注册的[[RpcEndpoint]]发送消息。
      *
      * Send a message to all registered [[RpcEndpoint]]s in this process.
      *
      * This can be used to make network events known to all end points (e.g. "a new node connected").
      */
    def postToAll(message: InboxMessage): Unit = {
        val iter = endpoints.keySet().iterator()
        while (iter.hasNext) {
            val name = iter.next
            postMessage(name, message, (e) => logWarning(s"Message $message dropped. ${e.getMessage}"))
        }
    }

    /**
      * 张贴远程终结点发送的消息。
      *
      * Posts a message sent by a remote endpoint.
      */
    def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
        val rpcCallContext =
            new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
        val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
        postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
    }

    /**
      * 张贴由本地终结点发送的消息
      *
      * Posts a message sent by a local endpoint.
      */
    def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
        val rpcCallContext =
            new LocalNettyRpcCallContext(message.senderAddress, p)
        val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
        postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
    }

    /** Posts a one-way message. */
    def postOneWayMessage(message: RequestMessage): Unit = {
        postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content),
            (e) => throw e)
    }

    /**
      * 将消息发布到特定端点。
      *
      * Posts a message to a specific endpoint.
      *
      * @param endpointName      name of the endpoint.
      * @param message           the message to post
      * @param callbackIfStopped callback function if the endpoint is stopped.
      */
    private def postMessage(
                               endpointName: String,
                               message: InboxMessage,
                               callbackIfStopped: (Exception) => Unit): Unit = {
        val error = synchronized {
            val data = endpoints.get(endpointName)
            if (stopped) {
                Some(new RpcEnvStoppedException())
            } else if (data == null) {
                Some(new SparkException(s"Could not find $endpointName."))
            } else {
                data.inbox.post(message)
                receivers.offer(data)
                None
            }
        }
        // We don't need to call `onStop` in the `synchronized` block
        error.foreach(callbackIfStopped)
    }

    def stop(): Unit = {
        synchronized {
            if (stopped) {
                return
            }
            stopped = true
        }
        // Stop all endpoints. This will queue all endpoints for processing by the message loops.
        endpoints.keySet().asScala.foreach(unregisterRpcEndpoint)
        // Enqueue a message that tells the message loops to stop.
        receivers.offer(PoisonPill)
        threadpool.shutdown()
    }

    def awaitTermination(): Unit = {
        threadpool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
    }

    /**
      * 如果端点存在，则返回
      *
      * Return if the endpoint exists
      */
    def verify(name: String): Boolean = {
        endpoints.containsKey(name)
    }

    /**
      * 用于调度消息的线程池
      *
      * Thread pool used for dispatching messages.
      */
    private val threadpool: ThreadPoolExecutor = {
        val numThreads = nettyEnv.conf.getInt("org.apache.spark.rpc.netty.dispatcher.numThreads",
            math.max(2, Runtime.getRuntime.availableProcessors()))
        val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop")
        for (i <- 0 until numThreads) {
            pool.execute(new MessageLoop)
        }
        pool
    }

    /**
      * 用于调度消息的消息循环
      *
      * Message loop used for dispatching messages.
      */
    private class MessageLoop extends Runnable {
        override def run(): Unit = {
            try {
                while (true) {
                    try {
                        val data = receivers.take()
                        if (data == PoisonPill) {
                            // Put PoisonPill back so that other MessageLoops can see it.
                            receivers.offer(PoisonPill)
                            return
                        }
                        data.inbox.process(Dispatcher.this)
                    } catch {
                        case NonFatal(e) => logError(e.getMessage, e)
                    }
                }
            } catch {
                case ie: InterruptedException => // exit
            }
        }
    }

    /**
      * 指示MessageLoop应退出其消息循环的毒端点
      *
      * A poison endpoint that indicates MessageLoop should exit its message loop.
      */
    private val PoisonPill = new EndpointData(null, null, null)
}
