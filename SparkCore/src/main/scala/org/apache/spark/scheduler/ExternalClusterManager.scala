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

import org.apache.spark.SparkContext

/**
  * 一个集群管理器接口用于插件外部调度程序。
  *
  * A cluster manager interface to plugin external scheduler.
  */
private[spark] trait ExternalClusterManager {

    /**
      * 检查此群集管理器实例是否可以为某个主URL创建调度程序组件。
      *
      * Check if this cluster manager instance can create scheduler components
      * for a certain master URL.
      *
      * @param masterURL the master URL
      * @return True if the cluster manager can create scheduler backend/
      */
    def canCreate(masterURL: String): Boolean

    /**
      * 为给定的SparkContext创建任务调度器实例。
      *
      * Create a task scheduler instance for the given SparkContext
      *
      * @param sc        SparkContext
      * @param masterURL the master URL
      * @return TaskScheduler that will be responsible for task handling
      */
    def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler

    /**
      * 为给定的SparkContext和计划程序创建计划程序后端。
      *
      * Create a scheduler backend for the given SparkContext and scheduler. This is
      * called after task scheduler is created using `ExternalClusterManager.createTaskScheduler()`.
      *
      * @param sc        SparkContext
      * @param masterURL the master URL
      * @param scheduler TaskScheduler that will be used with the scheduler backend.
      * @return SchedulerBackend that works with a TaskScheduler
      */
    def createSchedulerBackend(sc: SparkContext,
                               masterURL: String,
                               scheduler: TaskScheduler): SchedulerBackend

    /**
      * 初始化任务调度程序和后端调度程序。这是在创建调度程序组件后调用的
      *
      * Initialize task scheduler and backend scheduler. This is called after the
      * scheduler components are created
      *
      * @param scheduler TaskScheduler that will be responsible for task handling
      * @param backend   SchedulerBackend that works with a TaskScheduler
      */
    def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit
}
