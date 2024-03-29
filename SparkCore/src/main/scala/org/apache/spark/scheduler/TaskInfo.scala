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

import org.apache.spark.TaskState
import org.apache.spark.TaskState.TaskState
import org.apache.spark.annotation.DeveloperApi

/**
  * 有关任务集中正在运行的任务尝试的信息。
  *
  * :: DeveloperApi ::
  *
  * Information about a running task attempt inside a TaskSet.
  */
@DeveloperApi
class TaskInfo(
                  val taskId: Long,

                  /**
                    * 此任务在其任务集中的索引。不一定与任务正在计算的RDD分区的ID相同。
                    *
                    * The index of this task within its task set. Not necessarily the same as the ID of the RDD
                    * partition that the task is computing.
                    */
                  val index: Int,
                  val attemptNumber: Int,
                  val launchTime: Long,
                  val executorId: String,
                  val host: String,
                  val taskLocality: TaskLocality.TaskLocality,
                  val speculative: Boolean) {

    /**
      * 任务开始远程获取结果的时间。如果任务结果在任务完成时立即发送（而不是发送IndirectTaskResult并稍后从块管理器获取结果），则不会设置。
      *
      * The time when the task started remotely getting the result. Will not be set if the
      * task result was sent immediately when the task finished (as opposed to sending an
      * IndirectTaskResult and later fetching the result from the block manager).
      */
    var gettingResultTime: Long = 0

    /**
      * 任务成功完成的时间（如有必要，包括远程获取结果的时间）。
      *
      * The time when the task has completed successfully (including the time to remotely fetch
      * results, if necessary).
      */
    var finishTime: Long = 0
    var failed = false
    var killed = false
    private[this] var _accumulables: Seq[AccumulableInfo] = Nil

    /**
      * 在此任务期间对可累积项进行中间更新。请注意，同一个可累加项在单个任务中多次更新是有效的，
      * 或者一个任务中存在两个名称相同但ID不同的可累加项是有效的。
      *
      * Intermediate updates to accumulables during this task. Note that it is valid for the same
      * accumulable to be updated multiple times in a single task or for two accumulables with the
      * same name but different IDs to exist in a task.
      */
    def accumulables: Seq[AccumulableInfo] = _accumulables

    def status: String = {
        if (running) {
            if (gettingResult) {
                "GET RESULT"
            } else {
                "RUNNING"
            }
        } else if (failed) {
            "FAILED"
        } else if (killed) {
            "KILLED"
        } else if (successful) {
            "SUCCESS"
        } else {
            "UNKNOWN"
        }
    }

    def gettingResult: Boolean = gettingResultTime != 0

    def successful: Boolean = finished && !failed && !killed

    def finished: Boolean = finishTime != 0

    def running: Boolean = !finished

    def id: String = s"$index.$attemptNumber"

    def duration: Long = {
        if (!finished) {
            throw new UnsupportedOperationException("duration() called on unfinished task")
        } else {
            finishTime - launchTime
        }
    }

    private[spark] def setAccumulables(newAccumulables: Seq[AccumulableInfo]): Unit = {
        _accumulables = newAccumulables
    }

    private[spark] def markGettingResult(time: Long) {
        gettingResultTime = time
    }

    private[spark] def markFinished(state: TaskState, time: Long) {
        // finishTime should be set larger than 0, otherwise "finished" below will return false.
        assert(time > 0)
        finishTime = time
        if (state == TaskState.FAILED) {
            failed = true
        } else if (state == TaskState.KILLED) {
            killed = true
        }
    }

    private[spark] def timeRunning(currentTime: Long): Long = currentTime - launchTime
}
