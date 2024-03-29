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

import org.apache.spark.util.Utils
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

private[spark]
class ZippedWithIndexRDDPartition(val prev: Partition, val startIndex: Long)
        extends Partition with Serializable {
    override val index: Int = prev.index
}

/**
  * 表示用元素索引压缩的RDD。排序首先基于分区索引，然后是每个分区内项目的排序。
  * 所以第一个分区中的第一个项得到索引0，最后一个分区中的最后一个项得到最大的索引。
  *
  * Represents an RDD zipped with its element indices. The ordering is first based on the partition
  * index and then the ordering of items within each partition. So the first item in the first
  * partition gets index 0, and the last item in the last partition receives the largest index.
  *
  * @param prev parent RDD
  * @tparam T parent RDD item type
  */
private[spark]
class ZippedWithIndexRDD[T: ClassTag](prev: RDD[T]) extends RDD[(T, Long)](prev) {
    // ClassTag在运行时获得我们传递的类型参数的信息
    /**
      * 每个分区的起始索引
      * The start index of each partition. */
    @transient private val startIndices: Array[Long] = {
        val n = prev.partitions.length
        if (n == 0) {
            Array.empty
        } else if (n == 1) {
            Array(0L)
        } else {
            prev.context.runJob(
                prev,
                Utils.getIteratorSize _,
                0 until n - 1 // do not need to count the last partition
            ).scanLeft(0L)(_ + _)
        }
    }

    override def getPartitions: Array[Partition] = {
        firstParent[T].partitions.map(x => new ZippedWithIndexRDDPartition(x, startIndices(x.index)))
    }

    override def getPreferredLocations(split: Partition): Seq[String] =
        firstParent[T].preferredLocations(split.asInstanceOf[ZippedWithIndexRDDPartition].prev)

    override def compute(splitIn: Partition, context: TaskContext): Iterator[(T, Long)] = {
        val split = splitIn.asInstanceOf[ZippedWithIndexRDDPartition]
        val parentIter = firstParent[T].iterator(split.prev, context)
        Utils.getIteratorZipWithIndex(parentIter, split.startIndex)
    }
}
