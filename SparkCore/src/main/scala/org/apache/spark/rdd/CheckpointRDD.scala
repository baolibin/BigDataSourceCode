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

package org.apache.spark.rdd

import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

/**
  * 用于恢复检查点数据的RDD分区。
  *
  * An RDD partition used to recover checkpointed data.
  */
private[spark] class CheckpointRDDPartition(val index: Int) extends Partition

/**
  * 从存储器中恢复检查点数据的RDD。
  *
  * An RDD that recovers checkpointed data from storage.
  */
private[spark] abstract class CheckpointRDD[T: ClassTag](sc: SparkContext)
        extends RDD[T](sc, Nil) {

    // CheckpointRDD should not be checkpointed again
    override def doCheckpoint(): Unit = {}

    override def checkpoint(): Unit = {}

    override def localCheckpoint(): this.type = this

    override def compute(p: Partition, tc: TaskContext): Iterator[T] = ???

    // Note: There is a bug in MiMa that complains about `AbstractMethodProblem`s in the
    // base [[org.apache.org.apache.spark.rdd.RDD]] class if we do not override the following methods.
    // scalastyle:off
    protected override def getPartitions: Array[Partition] = ???

    // scalastyle:on

}
