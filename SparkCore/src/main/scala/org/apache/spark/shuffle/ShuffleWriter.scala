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

import java.io.IOException

import org.apache.spark.scheduler.MapStatus

/**
  * 在一个map任务中向shuffle系统写出记录。
  * Hadoop的MapReduce是强制性写入磁盘,而Spark可选择写入内存还是磁盘.
  *
  * Obtained inside a map task to write out records to the shuffle system.
  */
private[spark] abstract class ShuffleWriter[K, V] {
    /** Write a sequence of records to this task's output */
    // Map函数溢写磁盘,溢写到环形内存缓冲区,会进行快速排序
    @throws[IOException]
    def write(records: Iterator[Product2[K, V]]): Unit

    /** Close this writer, passing along whether the map completed */
    def stop(success: Boolean): Option[MapStatus]
}
