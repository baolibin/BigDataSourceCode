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

import java.io.Serializable
import java.util.Properties
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.source.Source
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.{AccumulatorV2, TaskCompletionListener, TaskFailureListener}

/**
  * Task的上下文信息。
  */
object TaskContext {
    private[this] val taskContext: ThreadLocal[TaskContext] = new ThreadLocal[TaskContext]

    /**
      * 返回当前活动的TaskContext。这可以在用户函数内部调用，以访问有关运行任务的上下文信息。
      *
      * Return the currently active TaskContext. This can be called inside of
      * user functions to access contextual information about running tasks.
      */
    def get(): TaskContext = taskContext.get

    /**
      * 返回当前活动TaskContext的分区id。如果本地执行等情况下没有活动的TaskContext，则返回0。
      *
      * Returns the partition id of currently active TaskContext. It will return 0
      * if there is no active TaskContext for cases like local execution.
      */
    def getPartitionId(): Int = {
        val tc = taskContext.get()
        if (tc eq null) {
            0
        } else {
            tc.partitionId()
        }
    }

    // Note: protected[org.apache.spark] instead of private[org.apache.spark] to prevent the following two from
    // showing up in JavaDoc.

    /**
      * 设置线程本地TaskContext。
      *
      * Set the thread local TaskContext. Internal to Spark.
      */
    protected[spark] def setTaskContext(tc: TaskContext): Unit = taskContext.set(tc)

    /**
      * 取消设置线程本地TaskContext
      *
      * Unset the thread local TaskContext. Internal to Spark.
      */
    protected[spark] def unset(): Unit = taskContext.remove()

    /**
      * 不代表实际任务的空任务上下文。这仅用于测试。
      *
      * An empty task context that does not represent an actual task.  This is only used in tests.
      */
    private[spark] def empty(): TaskContextImpl = {
        new TaskContextImpl(0, 0, 0, 0, null, new Properties, null)
    }
}


/**
  * 有关任务的上下文信息，可在执行过程中读取或修改。
  *
  * Contextual information about a task which can be read or mutated during
  * execution. To access the TaskContext for a running task, use:
  * {{{
  *   org.apache.org.apache.spark.TaskContext.get()
  * }}}
  */
abstract class TaskContext extends Serializable {
    // Note: TaskContext must NOT define a get method. Otherwise it will prevent the Scala compiler
    // from generating a static get method (based on the companion object's get method).

    // Note: Update JavaTaskContextCompileCheck when new methods are added to this class.

    // Note: getters in this class are defined with parentheses to maintain backward compatibility.

    /**
      * 如果任务已完成，则返回true。
      *
      * Returns true if the task has completed.
      */
    def isCompleted(): Boolean

    /**
      * 如果任务已终止，则返回true。
      *
      * Returns true if the task has been killed.
      */
    def isInterrupted(): Boolean

    /**
      * 如果任务在驱动程序中本地运行，则返回true。
      *
      * Returns true if the task is running locally in the driver program.
      *
      * @return false
      */
    @deprecated("Local execution was removed, so this always returns false", "2.0.0")
    def isRunningLocally(): Boolean

    /**
      * Adds a (Java friendly) listener to be executed on task completion.
      * This will be called in all situations - success, failure, or cancellation. Adding a listener
      * to an already completed task will result in that listener being called immediately.
      *
      * An example use is for HadoopRDD to register a callback to close the input stream.
      *
      * Exceptions thrown by the listener will result in failure of the task.
      */
    def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext

    /**
      * Adds a listener in the form of a Scala closure to be executed on task completion.
      * This will be called in all situations - success, failure, or cancellation. Adding a listener
      * to an already completed task will result in that listener being called immediately.
      *
      * An example use is for HadoopRDD to register a callback to close the input stream.
      *
      * Exceptions thrown by the listener will result in failure of the task.
      */
    def addTaskCompletionListener(f: (TaskContext) => Unit): TaskContext = {
        addTaskCompletionListener(new TaskCompletionListener {
            override def onTaskCompletion(context: TaskContext): Unit = f(context)
        })
    }

    /**
      * 添加要在任务失败时执行的侦听器。在已经失败的任务中添加侦听器将导致立即调用该侦听器。
      *
      * Adds a listener to be executed on task failure. Adding a listener to an already failed task
      * will result in that listener being called immediately.
      */
    def addTaskFailureListener(listener: TaskFailureListener): TaskContext

    /**
      * 添加要在任务失败时执行的侦听器。在已经失败的任务中添加侦听器将导致立即调用该侦听器。
      *
      * Adds a listener to be executed on task failure.  Adding a listener to an already failed task
      * will result in that listener being called immediately.
      */
    def addTaskFailureListener(f: (TaskContext, Throwable) => Unit): TaskContext = {
        addTaskFailureListener(new TaskFailureListener {
            override def onTaskFailure(context: TaskContext, error: Throwable): Unit = f(context, error)
        })
    }

    /**
      * 此任务所属阶段的ID。
      *
      * The ID of the stage that this task belong to.
      */
    def stageId(): Int

    /**
      * 此任务计算的RDD分区的ID。
      *
      * The ID of the RDD partition that is computed by this task.
      */
    def partitionId(): Int

    /**
      * 此任务已尝试了多少次。第一次任务尝试将分配attemptNumber=0，后续尝试的尝试次数将增加。
      *
      * How many times this task has been attempted.  The first task attempt will be assigned
      * attemptNumber = 0, and subsequent attempts will have increasing attempt numbers.
      */
    def attemptNumber(): Int

    /**
      * 此任务尝试的唯一ID（在同一SparkContext中，没有两个任务尝试共享相同的尝试ID）。这大致相当于Hadoop的TaskAttempid。
      *
      * An ID that is unique to this task attempt (within the same SparkContext, no two task attempts
      * will share the same attempt ID).  This is roughly equivalent to Hadoop's TaskAttemptID.
      */
    def taskAttemptId(): Long

    /**
      * 获取驱动程序上游的本地属性集，如果缺少，则为null。
      *
      * Get a local property set upstream in the driver, or null if it is missing. See also
      * `org.apache.org.apache.spark.SparkContext.setLocalProperty`.
      */
    def getLocalProperty(key: String): String

    @DeveloperApi
    def taskMetrics(): TaskMetrics

    /**
      * 返回与运行任务的实例关联的具有给定名称的所有度量源。有关更多信息，请参阅“org.apache.org/apache.spark.metrics.MetricsSystem”。
      *
      * ::DeveloperApi::
      * Returns all metrics sources with the given name which are associated with the instance
      * which runs the task. For more information see `org.apache.org.apache.spark.metrics.MetricsSystem`.
      */
    @DeveloperApi
    def getMetricsSources(sourceName: String): Seq[Source]

    /**
      * 如果任务被中断，抛出TaskKilledException并说明中断原因。
      *
      * If the task is interrupted, throws TaskKilledException with the reason for the interrupt.
      */
    private[spark] def killTaskIfInterrupted(): Unit

    /**
      * 如果任务被中断，说明此任务被终止的原因，否则说明无。
      *
      * If the task is interrupted, the reason this task was killed, otherwise None.
      */
    private[spark] def getKillReason(): Option[String]

    /**
      * 返回此任务的托管内存的管理器。
      *
      * Returns the manager for this task's managed memory.
      */
    private[spark] def taskMemoryManager(): TaskMemoryManager

    /**
      * 注册属于此任务的累加器。累加器在执行器中反序列化时必须调用此方法。
      *
      * Register an accumulator that belongs to this task. Accumulators must call this method when
      * deserializing in executors.
      */
    private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit

    /**
      * 记录此任务由于从远程主机获取失败而失败的情况。这允许驱动程序触发获取失败处理，而不考虑用户代码的介入。
      *
      * Record that this task has failed due to a fetch failure from a remote host.  This allows
      * fetch-failure handling to get triggered by the driver, regardless of intervening user-code.
      */
    private[spark] def setFetchFailed(fetchFailed: FetchFailedException): Unit

}
