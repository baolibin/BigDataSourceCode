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

import org.apache.spark.SparkConf
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.util.Clock

import scala.collection.mutable.{HashMap, HashSet}

/**
  * 处理任务集中的黑名单执行者和节点。
  * 这包括黑名单特定（任务，执行者）/（任务，节点）对，也完全黑名单执行者和整个任务集的节点。
  *
  * Handles blacklisting executors and nodes within a taskset.  This includes blacklisting specific
  * (task, executor) / (task, nodes) pairs, and also completely blacklisting executors and nodes
  * for the entire taskset.
  *
  * It also must store sufficient information in task failures for application level blacklisting,
  * which is handled by [[BlacklistTracker]].  Note that BlacklistTracker does not know anything
  * about task failures until a taskset completes successfully.
  *
  * THREADING:  This class is a helper to [[TaskSetManager]]; as with the methods in
  * [[TaskSetManager]] this class is designed only to be called from code with a lock on the
  * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
  */
private[scheduler] class TaskSetBlacklist(val conf: SparkConf, val stageId: Int, val clock: Clock)
    extends Logging {

    /**
      * 从每个执行器到该执行器上的任务失败的映射。这用于此任务集中的黑名单，也会转发到应用级别的[[BlacklistTracker]]如果此任务集成功完成，则列入黑名单。
      *
      * A map from each executor to the task failures on that executor.  This is used for blacklisting
      * within this taskset, and it is also relayed onto [[BlacklistTracker]] for app-level
      * blacklisting if this taskset completes successfully.
      */
    val execToFailures = new HashMap[String, ExecutorFailuresInTaskSet]()
    private val MAX_TASK_ATTEMPTS_PER_EXECUTOR = conf.get(config.MAX_TASK_ATTEMPTS_PER_EXECUTOR)
    private val MAX_TASK_ATTEMPTS_PER_NODE = conf.get(config.MAX_TASK_ATTEMPTS_PER_NODE)
    private val MAX_FAILURES_PER_EXEC_STAGE = conf.get(config.MAX_FAILURES_PER_EXEC_STAGE)
    private val MAX_FAILED_EXEC_PER_NODE_STAGE = conf.get(config.MAX_FAILED_EXEC_PER_NODE_STAGE)
    /**
      * 从节点映射到其上所有失败的执行器。需要，因为我们想知道节点上的执行者，即使他们已经去世。
      * （我们不想麻烦追踪node->execs映射，通常情况下没有任何故障）。
      * Map from node to all executors on it with failures.  Needed because we want to know about
      * executors on a node even after they have died. (We don't want to bother tracking the
      * node -> execs mapping in the usual case when there aren't any failures).
      */
    private val nodeToExecsWithFailures = new HashMap[String, HashSet[String]]()
    private val nodeToBlacklistedTaskIndexes = new HashMap[String, HashSet[Int]]()
    private val blacklistedExecs = new HashSet[String]()
    private val blacklistedNodes = new HashSet[String]()

    /**
      * 如果此执行器被列入给定任务的黑名单，则返回true。如果执行器在整个阶段被列入黑名单，或者在整个应用程序中被列入黑清单，则不需要返回true。
      * 也就是说，在调度器的内部循环中尽可能快地保持这种方法，在那里已经应用了这些过滤器。
      *
      * Return true if this executor is blacklisted for the given task.  This does *not*
      * need to return true if the executor is blacklisted for the entire stage, or blacklisted
      * for the entire application.  That is to keep this method as fast as possible in the inner-loop
      * of the scheduler, where those filters will have already been applied.
      */
    def isExecutorBlacklistedForTask(executorId: String, index: Int): Boolean = {
        execToFailures.get(executorId).exists { execFailures =>
            execFailures.getNumTaskFailures(index) >= MAX_TASK_ATTEMPTS_PER_EXECUTOR
        }
    }

    def isNodeBlacklistedForTask(node: String, index: Int): Boolean = {
        nodeToBlacklistedTaskIndexes.get(node).exists(_.contains(index))
    }

    /**
      * 如果此执行器在给定阶段被列入黑名单，则返回true。完全忽略执行器是否被列入整个应用程序的黑名单（或与执行器所在节点有关的任何内容）。
      * 也就是说，在调度器的内部循环中尽可能快地保持这种方法，在那里已经应用了这些过滤器。
      *
      * Return true if this executor is blacklisted for the given stage.  Completely ignores whether
      * the executor is blacklisted for the entire application (or anything to do with the node the
      * executor is on).  That is to keep this method as fast as possible in the inner-loop of the
      * scheduler, where those filters will already have been applied.
      */
    def isExecutorBlacklistedForTaskSet(executorId: String): Boolean = {
        blacklistedExecs.contains(executorId)
    }

    def isNodeBlacklistedForTaskSet(node: String): Boolean = {
        blacklistedNodes.contains(node)
    }

    private[scheduler] def updateBlacklistForFailedTask(
                                                           host: String,
                                                           exec: String,
                                                           index: Int): Unit = {
        val execFailures = execToFailures.getOrElseUpdate(exec, new ExecutorFailuresInTaskSet(host))
        execFailures.updateWithFailure(index, clock.getTimeMillis())

        // check if this task has also failed on other executors on the same host -- if its gone
        // over the limit, blacklist this task from the entire host.
        val execsWithFailuresOnNode = nodeToExecsWithFailures.getOrElseUpdate(host, new HashSet())
        execsWithFailuresOnNode += exec
        val failuresOnHost = execsWithFailuresOnNode.toIterator.flatMap { exec =>
            execToFailures.get(exec).map { failures =>
                // We count task attempts here, not the number of unique executors with failures.  This is
                // because jobs are aborted based on the number task attempts; if we counted unique
                // executors, it would be hard to config to ensure that you try another
                // node before hitting the max number of task failures.
                failures.getNumTaskFailures(index)
            }
        }.sum
        if (failuresOnHost >= MAX_TASK_ATTEMPTS_PER_NODE) {
            nodeToBlacklistedTaskIndexes.getOrElseUpdate(host, new HashSet()) += index
        }

        // Check if enough tasks have failed on the executor to blacklist it for the entire stage.
        if (execFailures.numUniqueTasksWithFailures >= MAX_FAILURES_PER_EXEC_STAGE) {
            if (blacklistedExecs.add(exec)) {
                logInfo(s"Blacklisting executor ${exec} for stage $stageId")
                // This executor has been pushed into the blacklist for this stage.  Let's check if it
                // pushes the whole node into the blacklist.
                val blacklistedExecutorsOnNode =
                execsWithFailuresOnNode.filter(blacklistedExecs.contains(_))
                if (blacklistedExecutorsOnNode.size >= MAX_FAILED_EXEC_PER_NODE_STAGE) {
                    if (blacklistedNodes.add(host)) {
                        logInfo(s"Blacklisting ${host} for stage $stageId")
                    }
                }
            }
        }
    }
}
