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

package org.apache.spark.shuffle

/**
  * shuffle系统的可插拔接口。在SparkEnv中，在driver和每个executor上创建一个ShuffleManager。
  * 基于spark.shuffle.manager设置，driver向它注册shuffle，executors（或在driver中本地运行的任务）可以请求读写数据。
  *
  * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
  * and on each executor, based on the org.apache.spark.shuffle.manager setting. The driver registers shuffles
  * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
  *
  * NOTE: this will be instantiated by SparkEnv so its constructor can take a SparkConf and
  * boolean isDriver as parameters.
  */
private[spark] trait ShuffleManager {

    /**
      * 注册一个ShuffleHandle类
      * 传入参数包括shuffleId、父RDD的分区数、宽依赖信息。
      * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
      */
    def registerShuffle[K, V, C](
                                        shuffleId: Int,
                                        numMaps: Int,
                                        dependency: ShuffleDependency[K, V, C]): ShuffleHandle

    /**
      * 根据输入参数,返回对应的ShuffleWriter类
      * Get a writer for a given partition. Called on executors by map tasks.
      **/
    def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

    /**
      * 由reduce task来调用,根据startPartition到endPartition-1(包含)读取该task分区对应的数据
      * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
      * Called on executors by reduce tasks.
      */
    def getReader[K, C](
                               handle: ShuffleHandle,
                               startPartition: Int,
                               endPartition: Int,
                               context: TaskContext): ShuffleReader[K, C]

    /**
      * 删除指定shuffleId对应的ShuffleManager
      * Remove a shuffle's metadata from the ShuffleManager.
      *
      * @return true if the metadata removed successfully, otherwise false.
      */
    def unregisterShuffle(shuffleId: Int): Boolean

    /**
      * 用于实现shuffle数据块与物理文件的映射.
      * Return a resolver capable of retrieving shuffle block data based on block coordinates.
      */
    def shuffleBlockResolver: ShuffleBlockResolver

    /**
      * 关闭这个ShuffleManager
      * Shut down this ShuffleManager. */
    def stop(): Unit
}
