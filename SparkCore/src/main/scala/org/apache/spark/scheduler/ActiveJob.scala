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

import org.apache.spark.util.CallSite

/**
  * DAGScheduler中正在运行的作业。
  * 作业可以有两种类型：结果作业（计算ResultState以执行操作）或map阶段作业（在提交任何下游阶段之前计算ShuffleMapStage的映射输出）。
  * 后者用于自适应查询规划，以便在提交后期之前查看map输出统计信息。我们使用这个类的finalStage字段来区分这两种类型的作业。
  *
  * A running job in the DAGScheduler. Jobs can be of two types: a result job, which computes a
  * ResultStage to execute an action, or a map-stage job, which computes the map outputs for a
  * ShuffleMapStage before any downstream stages are submitted. The latter is used for adaptive
  * query planning, to look at map output statistics before submitting later stages. We distinguish
  * between these two types of jobs using the finalStage field of this class.
  *
  * Jobs are only tracked for "leaf" stages that clients directly submitted, through DAGScheduler's
  * submitJob or submitMapStage methods. However, either type of job may cause the execution of
  * other earlier stages (for RDDs in the DAG it depends on), and multiple jobs may share some of
  * these previous stages. These dependencies are managed inside DAGScheduler.
  *
  * @param jobId      A unique ID for this job.
  * @param finalStage The stage that this job computes (either a ResultStage for an action or a
  *                   ShuffleMapStage for submitMapStage).
  * @param callSite   Where this job was initiated in the user's program (shown on UI).
  * @param listener   A listener to notify if tasks in this job finish or the job fails.
  * @param properties Scheduling properties attached to the job, such as fair scheduler pool name.
  */
private[spark] class ActiveJob(
                                  val jobId: Int,
                                  val finalStage: Stage,
                                  val callSite: CallSite,
                                  val listener: JobListener,
                                  val properties: Properties) {

    /**
      * 我们需要为此作业计算的分区数。注意，对于first（）和lookup（）等操作，结果阶段可能不需要计算其目标RDD中的所有分区。
      *
      * Number of partitions we need to compute for this job. Note that result stages may not need
      * to compute all partitions in their target RDD, for actions like first() and lookup().
      */
    val numPartitions = finalStage match {
        case r: ResultStage => r.partitions.length
        case m: ShuffleMapStage => m.rdd.partitions.length
    }

    /** Which partitions of the stage have finished */
    val finished = Array.fill[Boolean](numPartitions)(false)

    var numFinished = 0
}
