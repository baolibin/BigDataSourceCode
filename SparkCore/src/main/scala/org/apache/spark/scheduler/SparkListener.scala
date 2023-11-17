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

import java.util.Properties

import com.fasterxml.jackson.annotation.JsonTypeInfo
import javax.annotation.Nullable
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage.{BlockManagerId, BlockUpdatedInfo}
import org.apache.spark.ui.SparkUI
import org.apache.spark.{SparkConf, TaskEndReason}

import scala.collection.Map

@DeveloperApi
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "Event")
trait SparkListenerEvent {
    /* Whether output this event to the event log */
    protected[spark] def logEvent: Boolean = true
}

@DeveloperApi
case class SparkListenerStageSubmitted(stageInfo: StageInfo, properties: Properties = null)
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerStageCompleted(stageInfo: StageInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskStart(stageId: Int, stageAttemptId: Int, taskInfo: TaskInfo)
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskGettingResult(taskInfo: TaskInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskEnd(
                                   stageId: Int,
                                   stageAttemptId: Int,
                                   taskType: String,
                                   reason: TaskEndReason,
                                   taskInfo: TaskInfo,
                                   // may be null if the task has failed
                                   @Nullable taskMetrics: TaskMetrics)
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerJobStart(
                                    jobId: Int,
                                    time: Long,
                                    stageInfos: Seq[StageInfo],
                                    properties: Properties = null)
    extends SparkListenerEvent {
    // Note: this is here for backwards-compatibility with older versions of this event which
    // only stored stageIds and not StageInfos:
    val stageIds: Seq[Int] = stageInfos.map(_.stageId)
}

@DeveloperApi
case class SparkListenerJobEnd(
                                  jobId: Int,
                                  time: Long,
                                  jobResult: JobResult)
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerEnvironmentUpdate(environmentDetails: Map[String, Seq[(String, String)]])
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockManagerAdded(
                                             time: Long,
                                             blockManagerId: BlockManagerId,
                                             maxMem: Long,
                                             maxOnHeapMem: Option[Long] = None,
                                             maxOffHeapMem: Option[Long] = None) extends SparkListenerEvent {
}

@DeveloperApi
case class SparkListenerBlockManagerRemoved(time: Long, blockManagerId: BlockManagerId)
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerUnpersistRDD(rddId: Int) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorAdded(time: Long, executorId: String, executorInfo: ExecutorInfo)
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorRemoved(time: Long, executorId: String, reason: String)
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorBlacklisted(
                                               time: Long,
                                               executorId: String,
                                               taskFailures: Int)
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorUnblacklisted(time: Long, executorId: String)
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerNodeBlacklisted(
                                           time: Long,
                                           hostId: String,
                                           executorFailures: Int)
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerNodeUnblacklisted(time: Long, hostId: String)
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockUpdated(blockUpdatedInfo: BlockUpdatedInfo) extends SparkListenerEvent

/**
  * 执行者定期更新。
  *
  * Periodic updates from executors.
  *
  * @param execId       executor id
  * @param accumUpdates sequence of (taskId, stageId, stageAttemptId, accumUpdates)
  */
@DeveloperApi
case class SparkListenerExecutorMetricsUpdate(
                                                 execId: String,
                                                 accumUpdates: Seq[(Long, Int, Int, Seq[AccumulableInfo])])
    extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationStart(
                                            appName: String,
                                            appId: Option[String],
                                            time: Long,
                                            sparkUser: String,
                                            appAttemptId: Option[String],
                                            driverLogs: Option[Map[String, String]] = None) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationEnd(time: Long) extends SparkListenerEvent

/**
  * 一个内部类，用于描述事件日志的元数据。此事件不应发布到下游侦听器。
  *
  * An internal class that describes the metadata of an event log.
  * This event is not meant to be posted to listeners downstream.
  */
private[spark] case class SparkListenerLogStart(sparkVersion: String) extends SparkListenerEvent

/**
  * 用于创建在其他模块（如SQL）中定义的历史侦听器的接口，这些模块用于重建历史UI。
  *
  * Interface for creating history listeners defined in other modules like SQL, which are used to
  * rebuild the history UI.
  */
private[spark] trait SparkHistoryListenerFactory {
    /**
      * 创建用于重建历史UI的侦听器。
      *
      * Create listeners used to rebuild the history UI.
      */
    def createListeners(conf: SparkConf, sparkUI: SparkUI): Seq[SparkListener]
}


/**
  * 用于侦听来自Spark调度程序的事件的接口。大多数应用程序可能应该直接扩展SparkListener或SparkFirehoseListener，而不是实现此类。
  *
  * Interface for listening to events from the Spark scheduler. Most applications should probably
  * extend SparkListener or SparkFirehoseListener directly, rather than implementing this class.
  *
  * Note that this is an internal interface which might change in different Spark releases.
  */
private[spark] trait SparkListenerInterface {

    /**
      * 当阶段成功或失败时调用，并提供有关已完成阶段的信息。
      *
      * Called when a stage completes successfully or fails, with information on the completed stage.
      */
    def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit

    /**
      * 提交阶段时调用
      *
      * Called when a stage is submitted
      */
    def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit

    /**
      * 在任务启动时调用
      *
      * Called when a task starts
      */
    def onTaskStart(taskStart: SparkListenerTaskStart): Unit

    /**
      * 当任务开始远程获取其结果时调用（对于不需要远程获取结果的任务将不会调用）。
      *
      * Called when a task begins remotely fetching its result (will not be called for tasks that do
      * not need to fetch the result remotely).
      */
    def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit

    /**
      * 任务结束时调用
      *
      * Called when a task ends
      */
    def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit

    /**
      * 作业启动时调用
      *
      * Called when a job starts
      */
    def onJobStart(jobStart: SparkListenerJobStart): Unit

    /**
      * 作业结束时调用
      *
      * Called when a job ends
      */
    def onJobEnd(jobEnd: SparkListenerJobEnd): Unit

    /**
      * 在更新环境属性时调用
      *
      * Called when environment properties have been updated
      */
    def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit

    /**
      * 当新的块管理器加入时调用
      *
      * Called when a new block manager has joined
      */
    def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit

    /**
      * 在删除现有块管理器时调用
      *
      * Called when an existing block manager has been removed
      */
    def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit

    /**
      * Called when an RDD is manually unpersisted by the application
      */
    def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit

    /**
      * Called when the application starts
      */
    def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit

    /**
      * Called when the application ends
      */
    def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit

    /**
      * Called when the driver receives task metrics from an executor in a heartbeat.
      */
    def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit

    /**
      * Called when the driver registers a new executor.
      */
    def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit

    /**
      * Called when the driver removes an executor.
      */
    def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit

    /**
      * Called when the driver blacklists an executor for a Spark application.
      */
    def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit

    /**
      * Called when the driver re-enables a previously blacklisted executor.
      */
    def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit

    /**
      * Called when the driver blacklists a node for a Spark application.
      */
    def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit

    /**
      * Called when the driver re-enables a previously blacklisted node.
      */
    def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit

    /**
      * Called when the driver receives a block update info.
      */
    def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit

    /**
      * Called when other events like SQL-specific events are posted.
      */
    def onOtherEvent(event: SparkListenerEvent): Unit
}


/**
  * “SparkListenerInterface”的默认实现，没有针对所有回调的op实现。
  *
  * 请注意，这是一个内部接口，可能会在不同的Spark发布中发生变化。
  *
  * :: DeveloperApi ::
  * A default implementation for `SparkListenerInterface` that has no-op implementations for
  * all callbacks.
  *
  * Note that this is an internal interface which might change in different Spark releases.
  */
@DeveloperApi
abstract class SparkListener extends SparkListenerInterface {
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {}

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {}

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {}

    override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {}

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {}

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {}

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {}

    override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {}

    override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {}

    override def onBlockManagerRemoved(
                                          blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {}

    override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {}

    override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {}

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {}

    override def onExecutorMetricsUpdate(
                                            executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {}

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {}

    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {}

    override def onExecutorBlacklisted(
                                          executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = {}

    override def onExecutorUnblacklisted(
                                            executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = {}

    override def onNodeBlacklisted(
                                      nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = {}

    override def onNodeUnblacklisted(
                                        nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = {}

    override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {}

    override def onOtherEvent(event: SparkListenerEvent): Unit = {}
}
