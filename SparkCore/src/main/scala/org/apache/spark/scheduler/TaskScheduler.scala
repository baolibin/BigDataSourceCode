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

package org.apache.spark.scheduler

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

/**
  * Low-level任务调度器接口，目前由[[TaskSchedulerImpl]]独家实施。
  *
  * Low-level task scheduler interface, currently implemented exclusively by
  * [[org.apache.spark.scheduler.TaskSchedulerImpl]].
  * This interface allows plugging in different task schedulers. Each TaskScheduler schedules tasks
  * for a single SparkContext. These schedulers get sets of tasks submitted to them from the
  * DAGScheduler for each stage, and are responsible for sending the tasks to the cluster, running
  * them, retrying if there are failures, and mitigating stragglers. They return events to the
  * DAGScheduler.
  */
private[spark] trait TaskScheduler {

    private val appId = "org.apache.spark-application-" + System.currentTimeMillis

    def rootPool: Pool

    def schedulingMode: SchedulingMode

    def start(): Unit

    // Invoked after system has successfully initialized (typically in org.apache.spark context).
    // Yarn uses this to bootstrap allocation of resources based on preferred locations,
    // wait for slave registrations, etc.
    def postStartHook() {}

    // Disconnect from the cluster.
    def stop(): Unit

    // Submit a sequence of tasks to run.
    def submitTasks(taskSet: TaskSet): Unit

    // Cancel a stage.
    def cancelTasks(stageId: Int, interruptThread: Boolean): Unit

    /**
      * 终止任务尝试。
      *
      * Kills a task attempt.
      *
      * @return Whether the task was successfully killed.
      */
    def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean

    // Set the DAG scheduler for upcalls. This is guaranteed to be set before submitTasks is called.
    def setDAGScheduler(dagScheduler: DAGScheduler): Unit

    // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
    def defaultParallelism(): Int

    /**
      * 更新正在进行的任务的度量，并让主机知道BlockManager仍然有效。如果驱动程序知道给定的块管理器，则返回true。
      * 否则，返回false，表示块管理器应该重新注册。
      *
      * Update metrics for in-progress tasks and let the master know that the BlockManager is still
      * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
      * indicating that the block manager should re-register.
      */
    def executorHeartbeatReceived(
                                     execId: String,
                                     accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
                                     blockManagerId: BlockManagerId): Boolean

    /**
      * 获取与作业关联的应用程序ID。
      *
      * Get an application ID associated with the job.
      *
      * @return An application ID
      */
    def applicationId(): String = appId

    /**
      * Process a lost executor
      */
    def executorLost(executorId: String, reason: ExecutorLossReason): Unit

    /**
      * Get an application's attempt ID associated with the job.
      *
      * @return An application's Attempt ID
      */
    def applicationAttemptId(): Option[String]

}
