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

import scala.collection.mutable.HashSet

/**
  * stage是一组并行任务，所有任务都需要计算运行相同的函数,作为Spark作业的一部分。
  * 其中所有任务都具有相同的shuffle依赖项。
  * 调度程序运行的每一个DAG任务在发生shuffle的边界处被分为多个阶段，然后DAGScheduler按拓扑顺序运行这些阶段。
  *
  * A stage is a set of parallel tasks all computing the same function that need to run as part
  * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
  * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
  * DAGScheduler runs these stages in topological order.
  *
  * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
  * other stage(s), or a result stage, in which case its tasks directly compute a Spark action
  * (e.g. count(), save(), etc) by running a function on an RDD. For shuffle map stages, we also
  * track the nodes that each output partition is on.
  *
  * Each Stage also has a firstJobId, identifying the job that first submitted the stage.  When FIFO
  * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
  * faster on failure.
  *
  * Finally, a single stage can be re-executed in multiple attempts due to fault recovery. In that
  * case, the Stage object will track multiple StageInfo objects to pass to listeners or the web UI.
  * The latest one will be accessible through latestInfo.
  *
  * @param id         Unique stage ID
  * @param rdd        RDD that this stage runs on: for a shuffle map stage, it's the RDD we run map tasks
  *                   on, while for a result stage, it's the target RDD that we ran an action on
  * @param numTasks   Total number of tasks in stage; result stages in particular may not need to
  *                   compute all partitions, e.g. for first(), lookup(), and take().
  * @param parents    List of stages that this stage depends on (through shuffle dependencies).
  * @param firstJobId ID of the first job this stage was part of, for FIFO scheduling.
  * @param callSite   Location in the user program associated with this stage: either where the target
  *                   RDD was created, for a shuffle map stage, or where the action for a result stage was called.
  */
private[scheduler] abstract class Stage(
                                               val id: Int,
                                               val rdd: RDD[_],
                                               val numTasks: Int,
                                               val parents: List[Stage],
                                               val firstJobId: Int,
                                               val callSite: CallSite)
        extends Logging {

    val numPartitions = rdd.partitions.length

    /**
      * 设置stage属于哪些作业Id.
      * Set of jobs that this stage belongs to.
      */
    val jobIds = new HashSet[Int]
    val name: String = callSite.shortForm
    val details: String = callSite.longForm
    /**
      * Set of stage attempt IDs that have failed with a FetchFailure. We keep track of these
      * failures in order to avoid endless retries if a stage keeps failing with a FetchFailure.
      * We keep track of each attempt ID that has failed to avoid recording duplicate failures if
      * multiple tasks from the same stage attempt fail (SPARK-5945).
      */
    val fetchFailedAttemptIds = new HashSet[Int]
    /** The ID to use for the next new attempt for this stage. */
    private var nextAttemptId: Int = 0
    /**
      * Pointer to the [[StageInfo]] object for the most recent attempt. This needs to be initialized
      * here, before any attempts have actually been created, because the DAGScheduler uses this
      * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
      * have been created).
      */
    private var _latestInfo: StageInfo = StageInfo.fromStage(this, nextAttemptId)

    override final def hashCode(): Int = id

    override final def equals(other: Any): Boolean = other match {
        case stage: Stage => stage != null && stage.id == id
        case _ => false
    }

    /** Creates a new attempt for this stage by creating a new StageInfo with a new attempt ID. */
    def makeNewStageAttempt(
                                   numPartitionsToCompute: Int,
                                   taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
        val metrics = new TaskMetrics
        metrics.register(rdd.sparkContext)
        _latestInfo = StageInfo.fromStage(
            this, nextAttemptId, Some(numPartitionsToCompute), metrics, taskLocalityPreferences)
        nextAttemptId += 1
    }

    /**
      * 返回此阶段最近一次尝试的阶段信息
      * Returns the StageInfo for the most recent attempt for this stage.
      */
    def latestInfo: StageInfo = _latestInfo

    /**
      * 返回缺少的分区ID序列（即需要计算）
      * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
      */
    def findMissingPartitions(): Seq[Int]

    private[scheduler] def clearFailures(): Unit = {
        fetchFailedAttemptIds.clear()
    }
}
