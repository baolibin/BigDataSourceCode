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

import java.util.Properties

import javax.annotation.concurrent.GuardedBy
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util._

import scala.collection.mutable.ArrayBuffer

/**
  * [[TaskContext]]实现。
  *
  * A [[TaskContext]] implementation.
  *
  * 关于线程安全性的小说明。interrupted和fetchFailed字段是不稳定的，这确保了更新在线程之间总是可见的。
  * 通过锁定上下文实例来保护complete和failed标志及其回调。
  * 例如，这可以确保当我们在另一个线程中完成（并调用完成侦听器）时，不能在一个线程中添加完成侦听器。
  * 其他状态是不可变的，但是公开的“TaskMetrics”和“MetricsSystem”对象不是线程安全的。
  *
  * A small note on thread safety. The interrupted & fetchFailed fields are volatile, this makes
  * sure that updates are always visible across threads. The complete & failed flags and their
  * callbacks are protected by locking on the context instance. For instance, this ensures
  * that you cannot add a completion listener in one thread while we are completing (and calling
  * the completion listeners) in another thread. Other state is immutable, however the exposed
  * `TaskMetrics` & `MetricsSystem` objects are not thread safe.
  */
private[spark] class TaskContextImpl(
                                        val stageId: Int,
                                        val partitionId: Int,
                                        override val taskAttemptId: Long,
                                        override val attemptNumber: Int,
                                        override val taskMemoryManager: TaskMemoryManager,
                                        localProperties: Properties,
                                        @transient private val metricsSystem: MetricsSystem,
                                        // The default value is only used in tests.
                                        override val taskMetrics: TaskMetrics = TaskMetrics.empty)
    extends TaskContext
        with Logging {

    /**
      * 任务完成时要执行的回调函数列表
      *
      * List of callback functions to execute when the task completes.
      */
    @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

    /**
      * 任务失败时要执行的回调函数列表
      *
      * List of callback functions to execute when the task fails.
      */
    @transient private val onFailureCallbacks = new ArrayBuffer[TaskFailureListener]

    // If defined, the corresponding task has been killed and this option contains the reason.
    @volatile private var reasonIfKilled: Option[String] = None

    // Whether the task has completed.
    private var completed: Boolean = false

    // Whether the task has failed.
    private var failed: Boolean = false

    // Throwable that caused the task to fail
    private var failure: Throwable = _

    // If there was a fetch failure in the task, we store it here, to make sure user-code doesn't
    // hide the exception.  See SPARK-19276
    @volatile private var _fetchFailedException: Option[FetchFailedException] = None

    @GuardedBy("this")
    override def addTaskCompletionListener(listener: TaskCompletionListener)
    : this.type = synchronized {
        if (completed) {
            listener.onTaskCompletion(this)
        } else {
            onCompleteCallbacks += listener
        }
        this
    }

    @GuardedBy("this")
    override def addTaskFailureListener(listener: TaskFailureListener)
    : this.type = synchronized {
        if (failed) {
            listener.onTaskFailure(this, failure)
        } else {
            onFailureCallbacks += listener
        }
        this
    }

    @GuardedBy("this")
    override def isCompleted(): Boolean = synchronized(completed)

    override def isRunningLocally(): Boolean = false

    override def isInterrupted(): Boolean = reasonIfKilled.isDefined

    override def getLocalProperty(key: String): String = localProperties.getProperty(key)

    override def getMetricsSources(sourceName: String): Seq[Source] =
        metricsSystem.getSourcesByName(sourceName)

    /**
      * 将任务标记为失败并触发失败侦听器
      *
      * Marks the task as failed and triggers the failure listeners.
      */
    @GuardedBy("this")
    private[spark] def markTaskFailed(error: Throwable): Unit = synchronized {
        if (failed) return
        failed = true
        failure = error
        invokeListeners(onFailureCallbacks, "TaskFailureListener", Option(error)) {
            _.onTaskFailure(this, error)
        }
    }

    /**
      * 将任务标记为已完成并触发完成侦听器
      *
      * Marks the task as completed and triggers the completion listeners.
      */
    @GuardedBy("this")
    private[spark] def markTaskCompleted(error: Option[Throwable]): Unit = synchronized {
        if (completed) return
        completed = true
        invokeListeners(onCompleteCallbacks, "TaskCompletionListener", error) {
            _.onTaskCompletion(this)
        }
    }

    private def invokeListeners[T](
                                      listeners: Seq[T],
                                      name: String,
                                      error: Option[Throwable])(
                                      callback: T => Unit): Unit = {
        val errorMsgs = new ArrayBuffer[String](2)
        // Process callbacks in the reverse order of registration
        listeners.reverse.foreach { listener =>
            try {
                callback(listener)
            } catch {
                case e: Throwable =>
                    errorMsgs += e.getMessage
                    logError(s"Error in $name", e)
            }
        }
        if (errorMsgs.nonEmpty) {
            throw new TaskCompletionListenerException(errorMsgs, error)
        }
    }

    /**
      * 将任务标记为中断，即取消。
      *
      * Marks the task for interruption, i.e. cancellation.
      */
    private[spark] def markInterrupted(reason: String): Unit = {
        reasonIfKilled = Some(reason)
    }

    private[spark] override def killTaskIfInterrupted(): Unit = {
        val reason = reasonIfKilled
        if (reason.isDefined) {
            throw new TaskKilledException(reason.get)
        }
    }

    private[spark] override def getKillReason(): Option[String] = {
        reasonIfKilled
    }

    private[spark] override def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
        taskMetrics.registerAccumulator(a)
    }

    private[spark] override def setFetchFailed(fetchFailed: FetchFailedException): Unit = {
        this._fetchFailedException = Option(fetchFailed)
    }

    private[spark] def fetchFailed: Option[FetchFailedException] = _fetchFailedException

}
