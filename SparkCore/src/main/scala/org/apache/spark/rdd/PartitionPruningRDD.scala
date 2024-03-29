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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{NarrowDependency, Partition, TaskContext}

import scala.reflect.ClassTag

private[spark] class PartitionPruningRDDPartition(idx: Int, val parentSplit: Partition)
    extends Partition {
    override val index = idx
}


/**
  * 表示PartitionPrunningRDD与其父级之间的依赖关系。在这个在这种情况下，子RDD包含父RDD的分区的子集。
  *
  * Represents a dependency between the PartitionPruningRDD and its parent. In this
  * case, the child RDD contains a subset of partitions of the parents'.
  */
private[spark] class PruneDependency[T](rdd: RDD[T], partitionFilterFunc: Int => Boolean)
    extends NarrowDependency[T](rdd) {

    @transient
    val partitions: Array[Partition] = rdd.partitions
        .filter(s => partitionFilterFunc(s.index)).zipWithIndex
        .map { case (split, idx) => new PartitionPruningRDDPartition(idx, split): Partition }

    override def getParents(partitionId: Int): List[Int] = {
        List(partitions(partitionId).asInstanceOf[PartitionPruningRDDPartition].parentSplit.index)
    }
}


/**
  * 一个RDD，用于修剪RDD分区，这样我们就可以避免在所有分区上启动任务。
  * 一个示例用例：如果我们知道RDD是按范围划分的，并且执行DAG在键上有一个过滤器，
  * 那么我们可以避免在没有范围覆盖键的分区上启动任务。
  *
  * :: DeveloperApi ::
  * An RDD used to prune RDD partitions/partitions so we can avoid launching tasks on
  * all partitions. An example use case: If we know the RDD is partitioned by range,
  * and the execution DAG has a filter on the key, we can avoid launching tasks
  * on partitions that don't have the range covering the key.
  */
@DeveloperApi
class PartitionPruningRDD[T: ClassTag](
                                          prev: RDD[T],
                                          partitionFilterFunc: Int => Boolean)
    extends RDD[T](prev.context, List(new PruneDependency(prev, partitionFilterFunc))) {

    override def compute(split: Partition, context: TaskContext): Iterator[T] = {
        firstParent[T].iterator(
            split.asInstanceOf[PartitionPruningRDDPartition].parentSplit, context)
    }

    override protected def getPartitions: Array[Partition] =
        dependencies.head.asInstanceOf[PruneDependency[T]].partitions
}


@DeveloperApi
object PartitionPruningRDD {

    /**
      * 创建PartitionPrunningRDD。当在编译时不知道PartitionPrunningRDD的类型T时，此函数可用于创建它。
      *
      * Create a PartitionPruningRDD. This function can be used to create the PartitionPruningRDD
      * when its type T is not known at compile time.
      */
    def create[T](rdd: RDD[T], partitionFilterFunc: Int => Boolean): PartitionPruningRDD[T] = {
        new PartitionPruningRDD[T](rdd, partitionFilterFunc)(rdd.elementClassTag)
    }
}
