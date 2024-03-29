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

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer

import scala.reflect.ClassTag

private[spark] class ShuffledRDDPartition(val idx: Int) extends Partition {
    override val index: Int = idx
}

/**
  * shuffle产生的RDD（例如数据的重新分区）。
  *
  * :: DeveloperApi ::
  * The resulting RDD from a shuffle (e.g. repartitioning of data).
  *
  * @param prev the parent RDD.
  * @param part the partitioner used to partition the RDD
  * @tparam K the key class.
  * @tparam V the value class.
  * @tparam C the combiner class.
  */
// TODO: Make this return RDD[Product2[K, C]] or have some way to configure mutable pairs
@DeveloperApi
class ShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
                                                            @transient var prev: RDD[_ <: Product2[K, V]],
                                                            part: Partitioner)
    extends RDD[(K, C)](prev.context, Nil) {

    override val partitioner = Some(part)
    private var userSpecifiedSerializer: Option[Serializer] = None
    private var keyOrdering: Option[Ordering[K]] = None
    private var aggregator: Option[Aggregator[K, V, C]] = None
    private var mapSideCombine: Boolean = false

    /**
      * 为这个RDD的shuffle设置一个序列化程序，或者为null以使用默认值（org.apache.spark.serializer）
      *
      * Set a serializer for this RDD's shuffle, or null to use the default (org.apache.spark.serializer)
      */
    def setSerializer(serializer: Serializer): ShuffledRDD[K, V, C] = {
        this.userSpecifiedSerializer = Option(serializer)
        this
    }

    /**
      * 设置RDD洗牌的键顺序
      * Set key ordering for RDD's shuffle.
      */
    def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledRDD[K, V, C] = {
        this.keyOrdering = Option(keyOrdering)
        this
    }

    /**
      * 设置RDD洗牌的聚合器
      * Set aggregator for RDD's shuffle.
      */
    def setAggregator(aggregator: Aggregator[K, V, C]): ShuffledRDD[K, V, C] = {
        this.aggregator = Option(aggregator)
        this
    }

    /**
      * 为RDD的洗牌设置mapSideCombine标志
      * Set mapSideCombine flag for RDD's shuffle.
      */
    def setMapSideCombine(mapSideCombine: Boolean): ShuffledRDD[K, V, C] = {
        this.mapSideCombine = mapSideCombine
        this
    }

    override def getDependencies: Seq[Dependency[_]] = {
        val serializer = userSpecifiedSerializer.getOrElse {
            val serializerManager = SparkEnv.get.serializerManager
            if (mapSideCombine) {
                serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[C]])
            } else {
                serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[V]])
            }
        }
        List(new ShuffleDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
    }

    override def getPartitions: Array[Partition] = {
        Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
    }

    override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
        val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
        SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
            .read()
            .asInstanceOf[Iterator[(K, C)]]
    }

    override def clearDependencies() {
        super.clearDependencies()
        prev = null
    }

    override protected def getPreferredLocations(partition: Partition): Seq[String] = {
        val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
        val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
        tracker.getPreferredLocationsForShuffle(dep, partition.index)
    }
}
