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
  * task生命周期的6个状态。
  */
private[spark] object TaskState extends Enumeration {

    type TaskState = Value
    /**
      * LAUNCHING： Task已经从Driver侧发送给了Executor侧。
      * RUNNING： Executor正在执行Task。
      * FINISHED： Task在Executor上成功执行完成。
      * FAILED： Executor执行Task失败。
      * KILLED： 执行Task的Executor被killed掉。
      * LOST： 仅用于Mesos fine-grained调度模式。
      */
    val LAUNCHING, RUNNING, FINISHED, FAILED, KILLED, LOST = Value
    private val FINISHED_STATES = Set(FINISHED, FAILED, KILLED, LOST)

    // 是否失败包括2种状态
    def isFailed(state: TaskState): Boolean = (LOST == state) || (FAILED == state)

    // 是否完成包括4种状态
    def isFinished(state: TaskState): Boolean = FINISHED_STATES.contains(state)
}
