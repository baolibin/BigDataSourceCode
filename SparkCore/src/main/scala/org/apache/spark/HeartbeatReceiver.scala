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

package org.apache.spark

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util._

import scala.collection.mutable
import scala.concurrent.Future

/**
  * 从执行者到驱动者的心跳。这是多个内部组件使用的共享消息，用于传递正在进行的任务的活动性或执行信息。
  * 它也将使那些spark.network.timeout超时，没有心跳的主机失效。
  * org.apache.spark.executor.heartbeatInterval应明显小于spark.network.timeout超时。
  *
  * A heartbeat from executors to the driver. This is a shared message used by several internal
  * components to convey liveness or execution information for in-progress tasks. It will also
  * expire the hosts that have not heartbeated for more than org.apache.spark.network.timeout.
  * org.apache.spark.executor.heartbeatInterval should be significantly less than org.apache.spark.network.timeout.
  */
private[spark] case class Heartbeat(
                                       executorId: String,
                                       accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])], // taskId -> accumulator updates
                                       blockManagerId: BlockManagerId)

/**
  * SparkContext用于通知HeartbeatReceiver SparkContext的事件。taskScheduler已创建。
  *
  * An event that SparkContext uses to notify HeartbeatReceiver that SparkContext.taskScheduler is
  * created.
  */
private[spark] case object TaskSchedulerIsSet

private[spark] case object ExpireDeadHosts

private case class ExecutorRegistered(executorId: String)

private case class ExecutorRemoved(executorId: String)

private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
  * driver内部从executor端接受心跳信息。
  *
  * Lives in the driver to receive heartbeats from executors..
  */
private[spark] class HeartbeatReceiver(sc: SparkContext, clock: Clock)
    extends SparkListener with ThreadSafeRpcEndpoint with Logging {

    override val rpcEnv: RpcEnv = sc.env.rpcEnv

    sc.addSparkListener(this)
    // executor ID -> timestamp of when the last heartbeat from this executor was received
    private val executorLastSeen = new mutable.HashMap[String, Long]
    // "org.apache.spark.network.timeout" uses "seconds", while `org.apache.spark.storage.blockManagerSlaveTimeoutMs` uses
    // "milliseconds"
    private val slaveTimeoutMs =
    sc.conf.getTimeAsMs("org.apache.spark.storage.blockManagerSlaveTimeoutMs", "120s")
    private val executorTimeoutMs =
        sc.conf.getTimeAsSeconds("org.apache.spark.network.timeout", s"${slaveTimeoutMs}ms") * 1000
    // "org.apache.spark.network.timeoutInterval" uses "seconds", while
    // "org.apache.spark.storage.blockManagerTimeoutIntervalMs" uses "milliseconds"
    private val timeoutIntervalMs =
    sc.conf.getTimeAsMs("org.apache.spark.storage.blockManagerTimeoutIntervalMs", "60s")
    private val checkTimeoutIntervalMs =
        sc.conf.getTimeAsSeconds("org.apache.spark.network.timeoutInterval", s"${timeoutIntervalMs}ms") * 1000
    // "eventLoopThread" is used to run some pretty fast actions. The actions running in it should not
    // block the thread for a long time.
    private val eventLoopThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartbeat-receiver-event-loop-thread")
    private val killExecutorThread = ThreadUtils.newDaemonSingleThreadExecutor("kill-executor-thread")
    private[spark] var scheduler: TaskScheduler = null
    private var timeoutCheckingTask: ScheduledFuture[_] = null

    def this(sc: SparkContext) {
        this(sc, new SystemClock)
    }

    override def onStart(): Unit = {
        timeoutCheckingTask = eventLoopThread.scheduleAtFixedRate(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
                Option(self).foreach(_.ask[Boolean](ExpireDeadHosts))
            }
        }, 0, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

        // Messages sent and received locally
        case ExecutorRegistered(executorId) =>
            executorLastSeen(executorId) = clock.getTimeMillis()
            context.reply(true)
        case ExecutorRemoved(executorId) =>
            executorLastSeen.remove(executorId)
            context.reply(true)
        case TaskSchedulerIsSet =>
            scheduler = sc.taskScheduler
            context.reply(true)
        case ExpireDeadHosts =>
            expireDeadHosts()
            context.reply(true)

        // Messages received from executors
        case heartbeat@Heartbeat(executorId, accumUpdates, blockManagerId) =>
            if (scheduler != null) {
                if (executorLastSeen.contains(executorId)) {
                    executorLastSeen(executorId) = clock.getTimeMillis()
                    eventLoopThread.submit(new Runnable {
                        override def run(): Unit = Utils.tryLogNonFatalError {
                            val unknownExecutor = !scheduler.executorHeartbeatReceived(
                                executorId, accumUpdates, blockManagerId)
                            val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
                            context.reply(response)
                        }
                    })
                } else {
                    // This may happen if we get an executor's in-flight heartbeat immediately
                    // after we just removed it. It's not really an error condition so we should
                    // not log warning here. Otherwise there may be a lot of noise especially if
                    // we explicitly remove executors (SPARK-4134).
                    logDebug(s"Received heartbeat from unknown executor $executorId")
                    context.reply(HeartbeatResponse(reregisterBlockManager = true))
                }
            } else {
                // Because Executor will sleep several seconds before sending the first "Heartbeat", this
                // case rarely happens. However, if it really happens, log it and ask the executor to
                // register itself again.
                logWarning(s"Dropping $heartbeat because TaskScheduler is not ready yet")
                context.reply(HeartbeatResponse(reregisterBlockManager = true))
            }
    }

    private def expireDeadHosts(): Unit = {
        logTrace("Checking for hosts with no recent heartbeats in HeartbeatReceiver.")
        val now = clock.getTimeMillis()
        for ((executorId, lastSeenMs) <- executorLastSeen) {
            if (now - lastSeenMs > executorTimeoutMs) {
                logWarning(s"Removing executor $executorId with no recent heartbeats: " +
                    s"${now - lastSeenMs} ms exceeds timeout $executorTimeoutMs ms")
                scheduler.executorLost(executorId, SlaveLost("Executor heartbeat " +
                    s"timed out after ${now - lastSeenMs} ms"))
                // Asynchronously kill the executor to avoid blocking the current thread
                killExecutorThread.submit(new Runnable {
                    override def run(): Unit = Utils.tryLogNonFatalError {
                        // Note: we want to get an executor back after expiring this one,
                        // so do not simply call `sc.killExecutor` here (SPARK-8119)
                        sc.killAndReplaceExecutor(executorId)
                    }
                })
                executorLastSeen.remove(executorId)
            }
        }
    }

    /**
      * 如果心跳接收器未停止，通知其执行器注册。
      *
      * If the heartbeat receiver is not stopped, notify it of executor registrations.
      */
    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        addExecutor(executorAdded.executorId)
    }

    /**
      * 将ExecutorRegistered发送到事件循环以添加新的执行器。只是为了测试。
      *
      * Send ExecutorRegistered to the event loop to add a new executor. Only for test.
      *
      * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
      *         indicate if this operation is successful.
      */
    def addExecutor(executorId: String): Option[Future[Boolean]] = {
        Option(self).map(_.ask[Boolean](ExecutorRegistered(executorId)))
    }

    /**
      * 如果心跳接收器未停止，则通知它执行器已删除，以便它不会记录多余的错误。
      *
      * If the heartbeat receiver is not stopped, notify it of executor removals so it doesn't
      * log superfluous errors.
      *
      * Note that we must do this after the executor is actually removed to guard against the
      * following race condition: if we remove an executor's metadata from our data structure
      * prematurely, we may get an in-flight heartbeat from the executor before the executor is
      * actually removed, in which case we will still mark the executor as a dead host later
      * and expire it with loud error messages.
      */
    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
        removeExecutor(executorRemoved.executorId)
    }

    /**
      * 将ExecutorRemoved发送到事件循环以删除执行器。仅用于测试。
      *
      * Send ExecutorRemoved to the event loop to remove an executor. Only for test.
      *
      * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
      *         indicate if this operation is successful.
      */
    def removeExecutor(executorId: String): Option[Future[Boolean]] = {
        Option(self).map(_.ask[Boolean](ExecutorRemoved(executorId)))
    }

    override def onStop(): Unit = {
        if (timeoutCheckingTask != null) {
            timeoutCheckingTask.cancel(true)
        }
        eventLoopThread.shutdownNow()
        killExecutorThread.shutdownNow()
    }
}


private[spark] object HeartbeatReceiver {
    val ENDPOINT_NAME = "HeartbeatReceiver"
}
