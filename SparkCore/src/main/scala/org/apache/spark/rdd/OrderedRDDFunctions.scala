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
import org.apache.spark.internal.Logging
import org.apache.spark.{Partitioner, RangePartitioner}

import scala.reflect.ClassTag

/**
  * （键，值）对的RDD上可用的额外函数，其中键可通过隐式转换进行排序。
  * 它们将与作用域中具有隐式“Ordering[K]”的任何键类型“K”一起工作。
  * 所有标准基元类型都已存在排序对象。用户还可以为自定义类型定义自己的排序，或覆盖默认排序。
  * 将使用最近范围内的隐式排序。
  *
  * Extra functions available on RDDs of (key, value) pairs where the key is sortable through
  * an implicit conversion. They will work with any key type `K` that has an implicit `Ordering[K]`
  * in scope. Ordering objects already exist for all of the standard primitive types. Users can also
  * define their own orderings for custom types, or to override the default ordering. The implicit
  * ordering that is in the closest scope will be used.
  *
  * {{{
  *   import org.apache.org.apache.spark.SparkContext._
  *
  *   val rdd: RDD[(String, Int)] = ...
  *   implicit val caseInsensitiveOrdering = new Ordering[String] {
  *     override def compare(a: String, b: String) = a.toLowerCase.compare(b.toLowerCase)
  *   }
  *
  *   // Sort by key, using the above case insensitive ordering.
  *   rdd.sortByKey()
  * }}}
  */
class OrderedRDDFunctions[K: Ordering : ClassTag,
        V: ClassTag,
        P <: Product2[K, V] : ClassTag] @DeveloperApi()(
                                                               self: RDD[P])
        extends Logging with Serializable {
    private val ordering = implicitly[Ordering[K]]

    /**
      * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
      * `collect` or `save` on the resulting RDD will return or output an ordered list of records
      * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
      * order of the keys).
      */
    // TODO: this currently doesn't work on P other than Tuple2!
    def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
    : RDD[(K, V)] = self.withScope {
        val part = new RangePartitioner(numPartitions, self, ascending)
        new ShuffledRDD[K, V, V](self, part)
                .setKeyOrdering(if (ascending) ordering else ordering.reverse)
    }

    /**
      * Repartition the RDD according to the given partitioner and, within each resulting partition,
      * sort records by their keys.
      *
      * This is more efficient than calling `repartition` and then sorting within each partition
      * because it can push the sorting down into the shuffle machinery.
      */
    def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
        new ShuffledRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
    }

    /**
      * Returns an RDD containing only the elements in the inclusive range `lower` to `upper`.
      * If the RDD has been partitioned using a `RangePartitioner`, then this operation can be
      * performed efficiently by only scanning the partitions that might contain matching elements.
      * Otherwise, a standard `filter` is applied to all partitions.
      */
    def filterByRange(lower: K, upper: K): RDD[P] = self.withScope {

        def inRange(k: K): Boolean = ordering.gteq(k, lower) && ordering.lteq(k, upper)

        val rddToFilter: RDD[P] = self.partitioner match {
            case Some(rp: RangePartitioner[K, V]) =>
                val partitionIndicies = (rp.getPartition(lower), rp.getPartition(upper)) match {
                    case (l, u) => Math.min(l, u) to Math.max(l, u)
                }
                PartitionPruningRDD.create(self, partitionIndicies.contains)
            case _ =>
                self
        }
        rddToFilter.filter { case (k, v) => inRange(k) }
    }

}
