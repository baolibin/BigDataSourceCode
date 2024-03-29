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

import scala.collection.mutable.HashMap

/**
  * 用于跟踪失败任务以将其列入黑名单的小助手。一个任务集中一个Executor上所有失败的信息。
  *
  * Small helper for tracking failed tasks for blacklisting purposes.  Info on all failures on one
  * executor, within one task set.
  */
private[scheduler] class ExecutorFailuresInTaskSet(val node: String) {
    /**
      * 从任务集中任务的索引映射到此执行器上失败的次数和最近的失败时间。
      *
      * Mapping from index of the tasks in the taskset, to the number of times it has failed on this
      * executor and the most recent failure time.
      */
    val taskToFailureCountAndFailureTime = HashMap[Int, (Int, Long)]()

    def updateWithFailure(taskIndex: Int, failureTime: Long): Unit = {
        val (prevFailureCount, prevFailureTime) =
            taskToFailureCountAndFailureTime.getOrElse(taskIndex, (0, -1L))
        // these times always come from the driver, so we don't need to worry about skew, but might
        // as well still be defensive in case there is non-monotonicity in the clock
        val newFailureTime = math.max(prevFailureTime, failureTime)
        taskToFailureCountAndFailureTime(taskIndex) = (prevFailureCount + 1, newFailureTime)
    }

    /**
      * 返回此执行程序在给定任务索引上失败的次数。
      *
      * Return the number of times this executor has failed on the given task index.
      */
    def getNumTaskFailures(index: Int): Int = {
        taskToFailureCountAndFailureTime.getOrElse(index, (0, 0))._1
    }

    override def toString(): String = {
        s"numUniqueTasksWithFailures = $numUniqueTasksWithFailures; " +
            s"tasksToFailureCount = $taskToFailureCountAndFailureTime"
    }

    def numUniqueTasksWithFailures: Int = taskToFailureCountAndFailureTime.size
}
