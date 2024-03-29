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

/**
  * 与集群管理器通信以请求或终止executors的客户端。目前只支持YARN模式。
  *
  * A client that communicates with the cluster manager to request or kill executors.
  * This is currently supported only in YARN mode.
  */
private[spark] trait ExecutorAllocationClient {


    /**
      * 向群集管理器请求额外数量的执行器。
      *
      * Request an additional number of executors from the cluster manager.
      *
      * @return whether the request is acknowledged by the cluster manager.
      */
    def requestExecutors(numAdditionalExecutors: Int): Boolean

    /**
      * 请求群集管理器杀死指定的执行器。
      *
      * Request that the cluster manager kill the specified executors.
      *
      * When asking the executor to be replaced, the executor loss is considered a failure, and
      * killed tasks that are running on the executor will count towards the failure limits. If no
      * replacement is being requested, then the tasks will not count towards the limit.
      *
      * @param executorIds identifiers of executors to kill
      * @param replace     whether to replace the killed executors with new ones, default false
      * @param force       whether to force kill busy executors, default false
      * @return the ids of the executors acknowledged by the cluster manager to be removed.
      */
    def killExecutors(
                         executorIds: Seq[String],
                         replace: Boolean = false,
                         force: Boolean = false): Seq[String]

    /**
      * 请求群集管理器杀死指定主机上的每个执行器。
      *
      * Request that the cluster manager kill every executor on the specified host.
      *
      * @return whether the request is acknowledged by the cluster manager.
      */
    def killExecutorsOnHost(host: String): Boolean

    /**
      * 请求群集管理器杀死指定的执行器。
      *
      * Request that the cluster manager kill the specified executor.
      *
      * @return whether the request is acknowledged by the cluster manager.
      */
    def killExecutor(executorId: String): Boolean = {
        val killedExecutors = killExecutors(Seq(executorId))
        killedExecutors.nonEmpty && killedExecutors(0).equals(executorId)
    }

    /**
      * 获取当前活动执行者的列表
      *
      * Get the list of currently active executors
      */
    private[spark] def getExecutorIds(): Seq[String]

    /**
      * 根据调度需要更新群集管理器。包含三位信息以帮助it部门做出决策。
      *
      * Update the cluster manager on our scheduling needs. Three bits of information are included
      * to help it make decisions.
      *
      * @param numExecutors         The total number of executors we'd like to have. The cluster manager
      *                             shouldn't kill any running executor to reach this number, but,
      *                             if all existing executors were to die, this is the number of executors
      *                             we'd want to be allocated.
      * @param localityAwareTasks   The number of tasks in all active stages that have a locality
      *                             preferences. This includes running, pending, and completed tasks.
      * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
      *                             that would like to like to run on that host.
      *                             This includes running, pending, and completed tasks.
      * @return whether the request is acknowledged by the cluster manager.
      */
    private[spark] def requestTotalExecutors(
                                                numExecutors: Int,
                                                localityAwareTasks: Int,
                                                hostToLocalTaskCount: Map[String, Int]): Boolean
}
