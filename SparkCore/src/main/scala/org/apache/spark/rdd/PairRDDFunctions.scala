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

import java.nio.ByteBuffer
import java.util.{HashMap => JHashMap}

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob, OutputFormat => NewOutputFormat}
import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark._
import org.apache.spark.annotation.Experimental
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{SparkHadoopMapReduceWriter, SparkHadoopWriter, SparkHadoopWriterUtils}
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.util.random.StratifiedSamplingUtils

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}
import scala.reflect.ClassTag

/**
  * 通过隐式转换在（键，值）对的RDD上提供的额外函数。
  *
  * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
  */
class PairRDDFunctions[K, V](self: RDD[(K, V)])
                            (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
    extends Logging with Serializable {

    /**
      * 泛型函数，用于使用一组自定义聚合函数组合每个键的元素。此方法用于向后兼容。它不向洗牌提供合并器类标签信息。
      *
      * Generic function to combine the elements for each key using a custom set of aggregation
      * functions. This method is here for backward compatibility. It does not provide combiner
      * classtag information to the shuffle.
      *
      * @see `combineByKeyWithClassTag`
      */
    def combineByKey[C](
                           createCombiner: V => C,
                           mergeValue: (C, V) => C,
                           mergeCombiners: (C, C) => C,
                           partitioner: Partitioner,
                           mapSideCombine: Boolean = true,
                           serializer: Serializer = null): RDD[(K, C)] = self.withScope {
        combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners,
            partitioner, mapSideCombine, serializer)(null)
    }

    /**
      * combineByKeyWithClassTag的简化版本，对输出RDD进行哈希分区。此方法用于向后兼容。它不向洗牌提供合并器类标签信息。
      *
      * Simplified version of combineByKeyWithClassTag that hash-partitions the output RDD.
      * This method is here for backward compatibility. It does not provide combiner
      * classtag information to the shuffle.
      *
      * @see `combineByKeyWithClassTag`
      */
    def combineByKey[C](
                           createCombiner: V => C,
                           mergeValue: (C, V) => C,
                           mergeCombiners: (C, C) => C,
                           numPartitions: Int): RDD[(K, C)] = self.withScope {
        combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners, numPartitions)(null)
    }

    /**
      * combineByKeyWithClassTag的简化版本，用于对输出RDD进行哈希分区。
      *
      * :: Experimental ::
      * Simplified version of combineByKeyWithClassTag that hash-partitions the output RDD.
      */
    @Experimental
    def combineByKeyWithClassTag[C](
                                       createCombiner: V => C,
                                       mergeValue: (C, V) => C,
                                       mergeCombiners: (C, C) => C,
                                       numPartitions: Int)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
        combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners,
            new HashPartitioner(numPartitions))
    }

    /**
      * 使用给定的组合函数和中性“零值”聚合每个键的值。
      *
      * Aggregate the values of each key, using given combine functions and a neutral "zero value".
      * This function can return a different result type, U, than the type of the values in this RDD,
      * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
      * as in scala.TraversableOnce. The former operation is used for merging values within a
      * partition, and the latter is used for merging values between partitions. To avoid memory
      * allocation, both of these functions are allowed to modify and return their first argument
      * instead of creating a new U.
      */
    def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U,
                                                                      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
        aggregateByKey(zeroValue, new HashPartitioner(numPartitions))(seqOp, combOp)
    }

    /**
      * 使用给定的组合函数和中性“零值”聚合每个键的值。
      *
      * Aggregate the values of each key, using given combine functions and a neutral "zero value".
      * This function can return a different result type, U, than the type of the values in this RDD,
      * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
      * as in scala.TraversableOnce. The former operation is used for merging values within a
      * partition, and the latter is used for merging values between partitions. To avoid memory
      * allocation, both of these functions are allowed to modify and return their first argument
      * instead of creating a new U.
      */
    def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
                                                  combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
        aggregateByKey(zeroValue, defaultPartitioner(self))(seqOp, combOp)
    }

    /**
      * 使用给定的组合函数和中性“零值”聚合每个键的值。
      *
      * Aggregate the values of each key, using given combine functions and a neutral "zero value".
      * This function can return a different result type, U, than the type of the values in this RDD,
      * V. Thus, we need one operation for merging a V into a U and one operation for merging two U's,
      * as in scala.TraversableOnce. The former operation is used for merging values within a
      * partition, and the latter is used for merging values between partitions. To avoid memory
      * allocation, both of these functions are allowed to modify and return their first argument
      * instead of creating a new U.
      */
    def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,
                                                                            combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
        // Serialize the zero value to a byte array so that we can get a new clone of it on each key
        val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
        val zeroArray = new Array[Byte](zeroBuffer.limit)
        zeroBuffer.get(zeroArray)

        lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
        val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

        // We will clean the combiner closure later in `combineByKey`
        val cleanedSeqOp = self.context.clean(seqOp)
        combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
            cleanedSeqOp, combOp, partitioner)
    }

    /**
      * 使用关联函数和中性“零值”合并每个键的值，可以将其添加到结果中任意次数，且不得更改结果
      *
      * Merge the values for each key using an associative function and a neutral "zero value" which
      * may be added to the result an arbitrary number of times, and must not change the result
      * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
      */
    def foldByKey(zeroValue: V, numPartitions: Int)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
        foldByKey(zeroValue, new HashPartitioner(numPartitions))(func)
    }

    /**
      * 使用关联函数和中性“零值”合并每个键的值，可以将其添加到结果中任意次数，且不得更改结果
      *
      * Merge the values for each key using an associative function and a neutral "zero value" which
      * may be added to the result an arbitrary number of times, and must not change the result
      * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
      */
    def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
        foldByKey(zeroValue, defaultPartitioner(self))(func)
    }

    /**
      * 使用关联函数和中性“零值”合并每个键的值，该值可以任意多次添加到结果中，并且不得更改结果
      *
      * Merge the values for each key using an associative function and a neutral "zero value" which
      * may be added to the result an arbitrary number of times, and must not change the result
      * (e.g., Nil for list concatenation, 0 for addition, or 1 for multiplication.).
      */
    def foldByKey(
                     zeroValue: V,
                     partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
        // Serialize the zero value to a byte array so that we can get a new clone of it on each key
        val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
        val zeroArray = new Array[Byte](zeroBuffer.limit)
        zeroBuffer.get(zeroArray)

        // When deserializing, use a lazy val to create just one instance of the serializer per task
        lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
        val createZero = () => cachedSerializer.deserialize[V](ByteBuffer.wrap(zeroArray))

        val cleanedFunc = self.context.clean(func)
        combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),
            cleanedFunc, cleanedFunc, partitioner)
    }

    /**
      * 返回按键采样的此RDD的子集（通过分层采样）。
      *
      * Return a subset of this RDD sampled by key (via stratified sampling).
      *
      * Create a sample of this RDD using variable sampling rates for different keys as specified by
      * `fractions`, a key to sampling rate map, via simple random sampling with one pass over the
      * RDD, to produce a sample of size that's approximately equal to the sum of
      * math.ceil(numItems * samplingRate) over all key values.
      *
      * @param withReplacement whether to sample with or without replacement
      * @param fractions       map of specific keys to sampling rates
      * @param seed            seed for the random number generator
      * @return RDD containing the sampled subset
      */
    def sampleByKey(withReplacement: Boolean,
                    fractions: Map[K, Double],
                    seed: Long = Utils.random.nextLong): RDD[(K, V)] = self.withScope {

        require(fractions.values.forall(v => v >= 0.0), "Negative sampling rates.")

        val samplingFunc = if (withReplacement) {
            StratifiedSamplingUtils.getPoissonSamplingFunction(self, fractions, false, seed)
        } else {
            StratifiedSamplingUtils.getBernoulliSamplingFunction(self, fractions, false, seed)
        }
        self.mapPartitionsWithIndex(samplingFunc, preservesPartitioning = true)
    }

    /**
      * 返回按密钥（通过分层采样）采样的RDD的子集，该子集包含每个层（具有相同密钥的一组对）的math.ceil（numItems*samplingRate）。
      *
      * Return a subset of this RDD sampled by key (via stratified sampling) containing exactly
      * math.ceil(numItems * samplingRate) for each stratum (group of pairs with the same key).
      *
      * This method differs from [[sampleByKey]] in that we make additional passes over the RDD to
      * create a sample size that's exactly equal to the sum of math.ceil(numItems * samplingRate)
      * over all key values with a 99.99% confidence. When sampling without replacement, we need one
      * additional pass over the RDD to guarantee sample size; when sampling with replacement, we need
      * two additional passes.
      *
      * @param withReplacement whether to sample with or without replacement
      * @param fractions       map of specific keys to sampling rates
      * @param seed            seed for the random number generator
      * @return RDD containing the sampled subset
      */
    def sampleByKeyExact(
                            withReplacement: Boolean,
                            fractions: Map[K, Double],
                            seed: Long = Utils.random.nextLong): RDD[(K, V)] = self.withScope {

        require(fractions.values.forall(v => v >= 0.0), "Negative sampling rates.")

        val samplingFunc = if (withReplacement) {
            StratifiedSamplingUtils.getPoissonSamplingFunction(self, fractions, true, seed)
        } else {
            StratifiedSamplingUtils.getBernoulliSamplingFunction(self, fractions, true, seed)
        }
        self.mapPartitionsWithIndex(samplingFunc, preservesPartitioning = true)
    }

    /**
      * 使用关联和交换的reduce函数合并每个键的值。这也将在将结果发送到reducer之前在每个映射器上执行本地合并，类似于MapReduce中的“合并器”。输出将使用numPartitions分区进行哈希分区。
      *
      * Merge the values for each key using an associative and commutative reduce function. This will
      * also perform the merging locally on each mapper before sending results to a reducer, similarly
      * to a "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
      */
    def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)] = self.withScope {
        reduceByKey(new HashPartitioner(numPartitions), func)
    }

    /**
      * 使用关联和交换的reduce函数合并每个键的值，但将结果立即以Map的形式返回给master。这也将在将结果发送到reducer之前在每个映射器上执行本地合并，类似于MapReduce中的“合并器”。
      *
      * Merge the values for each key using an associative and commutative reduce function, but return
      * the results immediately to the master as a Map. This will also perform the merging locally on
      * each mapper before sending results to a reducer, similarly to a "combiner" in MapReduce.
      */
    def reduceByKeyLocally(func: (V, V) => V): Map[K, V] = self.withScope {
        val cleanedF = self.sparkContext.clean(func)

        if (keyClass.isArray) {
            throw new SparkException("reduceByKeyLocally() does not support array keys")
        }

        val reducePartition = (iter: Iterator[(K, V)]) => {
            val map = new JHashMap[K, V]
            iter.foreach { pair =>
                val old = map.get(pair._1)
                map.put(pair._1, if (old == null) pair._2 else cleanedF(old, pair._2))
            }
            Iterator(map)
        }: Iterator[JHashMap[K, V]]

        val mergeMaps = (m1: JHashMap[K, V], m2: JHashMap[K, V]) => {
            m2.asScala.foreach { pair =>
                val old = m1.get(pair._1)
                m1.put(pair._1, if (old == null) pair._2 else cleanedF(old, pair._2))
            }
            m1
        }: JHashMap[K, V]

        self.mapPartitions(reducePartition).reduce(mergeMaps).asScala
    }

    /**
      * 计算每个键的元素数量，将结果收集到本地Map中。
      *
      * Count the number of elements for each key, collecting the results to a local Map.
      *
      * @note This method should only be used if the resulting map is expected to be small, as
      *       the whole thing is loaded into the driver's memory.
      *       To handle very large results, consider using rdd.mapValues(_ => 1L).reduceByKey(_ + _), which
      *       returns an RDD[T, Long] instead of a map.
      */
    def countByKey(): Map[K, Long] = self.withScope {
        self.mapValues(_ => 1L).reduceByKey(_ + _).collect().toMap
    }

    /**
      * 使用关联和交换的reduce函数合并每个键的值。这也将在将结果发送到reducer之前在每个映射器上本地执行合并，类似于MapReduce中的“合并器”。
      * 输出将使用现有的partitioner/parallelism级别进行哈希分区。
      *
      * Merge the values for each key using an associative and commutative reduce function. This will
      * also perform the merging locally on each mapper before sending results to a reducer, similarly
      * to a "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
      * parallelism level.
      */
    def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
        reduceByKey(defaultPartitioner(self), func)
    }

    /**
      * 使用关联和交换的reduce函数合并每个键的值。这也将在将结果发送到reducer之前在每个映射器上执行本地合并，类似于MapReduce中的“合并器”。
      *
      * Merge the values for each key using an associative and commutative reduce function. This will
      * also perform the merging locally on each mapper before sending results to a reducer, similarly
      * to a "combiner" in MapReduce.
      */
    def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
        combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
    }

    /**
      * countByKey的近似版本，如果在超时时间内未完成，则可以返回部分结果。
      *
      * Approximate version of countByKey that can return a partial result if it does
      * not finish within a timeout.
      *
      * The confidence is the probability that the error bounds of the result will
      * contain the true value. That is, if countApprox were called repeatedly
      * with confidence 0.9, we would expect 90% of the results to contain the
      * true count. The confidence must be in the range [0,1] or an exception will
      * be thrown.
      *
      * @param timeout    maximum time to wait for the job, in milliseconds
      * @param confidence the desired statistical confidence in the result
      * @return a potentially incomplete result, with error bounds
      */
    def countByKeyApprox(timeout: Long, confidence: Double = 0.95)
    : PartialResult[Map[K, BoundedDouble]] = self.withScope {
        self.map(_._1).countByValueApprox(timeout, confidence)
    }

    /**
      * 返回此RDD中每个键的不同值的近似数目。
      *
      * Return approximate number of distinct values for each key in this RDD.
      *
      * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
      * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
      * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
      *
      * @param relativeSD    Relative accuracy. Smaller values create counters that require more space.
      *                      It must be greater than 0.000017.
      * @param numPartitions number of partitions of the resulting RDD
      */
    def countApproxDistinctByKey(
                                    relativeSD: Double,
                                    numPartitions: Int): RDD[(K, Long)] = self.withScope {
        countApproxDistinctByKey(relativeSD, new HashPartitioner(numPartitions))
    }

    /**
      * 返回此RDD中每个键的不同值的近似数目。
      *
      * Return approximate number of distinct values for each key in this RDD.
      *
      * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
      * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
      * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
      *
      * @param relativeSD  Relative accuracy. Smaller values create counters that require more space.
      *                    It must be greater than 0.000017.
      * @param partitioner partitioner of the resulting RDD
      */
    def countApproxDistinctByKey(
                                    relativeSD: Double,
                                    partitioner: Partitioner): RDD[(K, Long)] = self.withScope {
        require(relativeSD > 0.000017, s"accuracy ($relativeSD) must be greater than 0.000017")
        val p = math.ceil(2.0 * math.log(1.054 / relativeSD) / math.log(2)).toInt
        assert(p <= 32)
        countApproxDistinctByKey(if (p < 4) 4 else p, 0, partitioner)
    }

    /**
      * 返回此RDD中每个键的不同值的近似数目。
      *
      * Return approximate number of distinct values for each key in this RDD.
      *
      * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
      * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
      * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
      *
      * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero (`sp` is
      * greater than `p`) would trigger sparse representation of registers, which may reduce the
      * memory consumption and increase accuracy when the cardinality is small.
      *
      * @param p           The precision value for the normal set.
      *                    `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
      * @param sp          The precision value for the sparse set, between 0 and 32.
      *                    If `sp` equals 0, the sparse representation is skipped.
      * @param partitioner Partitioner to use for the resulting RDD.
      */
    def countApproxDistinctByKey(
                                    p: Int,
                                    sp: Int,
                                    partitioner: Partitioner): RDD[(K, Long)] = self.withScope {
        require(p >= 4, s"p ($p) must be >= 4")
        require(sp <= 32, s"sp ($sp) must be <= 32")
        require(sp == 0 || p <= sp, s"p ($p) cannot be greater than sp ($sp)")
        val createHLL = (v: V) => {
            val hll = new HyperLogLogPlus(p, sp)
            hll.offer(v)
            hll
        }
        val mergeValueHLL = (hll: HyperLogLogPlus, v: V) => {
            hll.offer(v)
            hll
        }
        val mergeHLL = (h1: HyperLogLogPlus, h2: HyperLogLogPlus) => {
            h1.addAll(h2)
            h1
        }

        combineByKeyWithClassTag(createHLL, mergeValueHLL, mergeHLL, partitioner)
            .mapValues(_.cardinality())
    }

    /**
      * 返回此RDD中每个键的不同值的近似数目。
      *
      * Return approximate number of distinct values for each key in this RDD.
      *
      * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
      * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
      * <a href="http://dx.doi.org/10.1145/2452376.2452456">here</a>.
      *
      * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
      *                   It must be greater than 0.000017.
      */
    def countApproxDistinctByKey(relativeSD: Double = 0.05): RDD[(K, Long)] = self.withScope {
        countApproxDistinctByKey(relativeSD, defaultPartitioner(self))
    }

    /**
      * 将RDD中每个键的值分组为一个序列。Hash将生成的RDD与划分为“numPartitions”分区。每组中元素的顺序不能保证，甚至每次评估所得RDD时可能会有所不同。
      *
      * Group the values for each key in the RDD into a single sequence. Hash-partitions the
      * resulting RDD with into `numPartitions` partitions. The ordering of elements within
      * each group is not guaranteed, and may even differ each time the resulting RDD is evaluated.
      *
      * @note This operation may be very expensive. If you are grouping in order to perform an
      *       aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
      *       or `PairRDDFunctions.reduceByKey` will provide much better performance.
      * @note As currently implemented, groupByKey must be able to hold all the key-value pairs for any
      *       key in memory. If a key has too many values, it can result in an `OutOfMemoryError`.
      */
    def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])] = self.withScope {
        groupByKey(new HashPartitioner(numPartitions))
    }

    /**
      * 将RDD中每个键的值分组为一个序列。允许通过传递Partitioner来控制生成的键值对RDD的分区。
      *
      * Group the values for each key in the RDD into a single sequence. Allows controlling the
      * partitioning of the resulting key-value pair RDD by passing a Partitioner.
      * The ordering of elements within each group is not guaranteed, and may even differ
      * each time the resulting RDD is evaluated.
      *
      * @note This operation may be very expensive. If you are grouping in order to perform an
      *       aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
      *       or `PairRDDFunctions.reduceByKey` will provide much better performance.
      * @note As currently implemented, groupByKey must be able to hold all the key-value pairs for any
      *       key in memory. If a key has too many values, it can result in an `OutOfMemoryError`.
      */
    def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = self.withScope {
        // groupByKey shouldn't use map side combine because map side combine does not
        // reduce the amount of data shuffled and requires all map side data be inserted
        // into a hash table, leading to more objects in the old gen.
        val createCombiner = (v: V) => CompactBuffer(v)
        val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
        val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
        val bufs = combineByKeyWithClassTag[CompactBuffer[V]](
            createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
        bufs.asInstanceOf[RDD[(K, Iterable[V])]]
    }

    /**
      * 泛型函数，用于使用一组自定义聚合函数组合每个键的元素。对于“组合类型”C，将RDD[（K，V）]转换为RDD[类型（K，C）]的结果
      *
      * :: Experimental ::
      * Generic function to combine the elements for each key using a custom set of aggregation
      * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
      *
      * Users provide three functions:
      *
      *  - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
      *  - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
      *  - `mergeCombiners`, to combine two C's into a single one.
      *
      * In addition, users can control the partitioning of the output RDD, and whether to perform
      * map-side aggregation (if a mapper can produce multiple items with the same key).
      *
      * @note V and C can be different -- for example, one might group an RDD of type
      *       (Int, Int) into an RDD of type (Int, Seq[Int]).
      */
    @Experimental
    def combineByKeyWithClassTag[C](
                                       createCombiner: V => C,
                                       mergeValue: (C, V) => C,
                                       mergeCombiners: (C, C) => C,
                                       partitioner: Partitioner,
                                       mapSideCombine: Boolean = true,
                                       serializer: Serializer = null)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
        require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
        if (keyClass.isArray) {
            if (mapSideCombine) {
                throw new SparkException("Cannot use map-side combining with array keys.")
            }
            if (partitioner.isInstanceOf[HashPartitioner]) {
                throw new SparkException("HashPartitioner cannot partition array keys.")
            }
        }
        val aggregator = new Aggregator[K, V, C](
            self.context.clean(createCombiner),
            self.context.clean(mergeValue),
            self.context.clean(mergeCombiners))
        if (self.partitioner == Some(partitioner)) {
            self.mapPartitions(iter => {
                val context = TaskContext.get()
                new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
            }, preservesPartitioning = true)
        } else {
            new ShuffledRDD[K, V, C](self, partitioner)
                .setSerializer(serializer)
                .setAggregator(aggregator)
                .setMapSideCombine(mapSideCombine)
        }
    }

    /**
      * 返回使用指定分区器分区的RDD的副本。
      *
      * Return a copy of the RDD partitioned using the specified partitioner.
      */
    def partitionBy(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
        if (keyClass.isArray && partitioner.isInstanceOf[HashPartitioner]) {
            throw new SparkException("HashPartitioner cannot partition array keys.")
        }
        if (self.partitioner == Some(partitioner)) {
            self
        } else {
            new ShuffledRDD[K, V, V](self, partitioner)
        }
    }

    private[spark] def keyClass: Class[_] = kt.runtimeClass

    /**
      * combineByKeyWithClassTag的简化版本，该版本使用现有的partitioner/parallelism级别对生成的RDD进行哈希分区。此方法用于向后兼容性。它不向shuffle提供组合器类标签信息。
      *
      * Simplified version of combineByKeyWithClassTag that hash-partitions the resulting RDD using the
      * existing partitioner/parallelism level. This method is here for backward compatibility. It
      * does not provide combiner classtag information to the shuffle.
      *
      * @see `combineByKeyWithClassTag`
      */
    def combineByKey[C](
                           createCombiner: V => C,
                           mergeValue: (C, V) => C,
                           mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
        combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)
    }

    /**
      * combineByKeyWithClassTag的简化版本，该版本使用现有的partitioner/parallelism级别对生成的RDD进行哈希分区。
      *
      * :: Experimental ::
      * Simplified version of combineByKeyWithClassTag that hash-partitions the resulting RDD using the
      * existing partitioner/parallelism level.
      */
    @Experimental
    def combineByKeyWithClassTag[C](
                                       createCombiner: V => C,
                                       mergeValue: (C, V) => C,
                                       mergeCombiners: (C, C) => C)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
        combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners, defaultPartitioner(self))
    }

    /**
      * 将RDD中每个键的值分组为一个序列。Hash使用现有的partitioner/parallelism级别对生成的RDD进行分区。
      * 每组中元素的顺序不能保证，甚至每次评估所得RDD时可能会有所不同。
      *
      * Group the values for each key in the RDD into a single sequence. Hash-partitions the
      * resulting RDD with the existing partitioner/parallelism level. The ordering of elements
      * within each group is not guaranteed, and may even differ each time the resulting RDD is
      * evaluated.
      *
      * @note This operation may be very expensive. If you are grouping in order to perform an
      *       aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
      *       or `PairRDDFunctions.reduceByKey` will provide much better performance.
      */
    def groupByKey(): RDD[(K, Iterable[V])] = self.withScope {
        groupByKey(defaultPartitioner(self))
    }

    /**
      * 返回一个RDD，该RDD包含“this”和“other”中具有匹配关键字的所有元素对。
      * 每对元素将作为（k，（v1，v2））元组返回，其中（k，v1）在“this”中，并且（k，v2）在“其他”中。在集群中执行哈希联接。
      *
      * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
      * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
      * (k, v2) is in `other`. Performs a hash join across the cluster.
      */
    def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = self.withScope {
        join(other, defaultPartitioner(self, other))
    }

    /**
      * 返回一个RDD，该RDD包含“this”和“other”中具有匹配关键字的所有元素对。
      *
      * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
      * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
      * (k, v2) is in `other`. Performs a hash join across the cluster.
      */
    def join[W](other: RDD[(K, W)], numPartitions: Int): RDD[(K, (V, W))] = self.withScope {
        join(other, new HashPartitioner(numPartitions))
    }

    /**
      * 返回一个RDD，该RDD包含“this”和“other”中具有匹配关键字的所有元素对。
      *
      * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
      * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
      * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
      */
    def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] = self.withScope {
        this.cogroup(other, partitioner).flatMapValues(pair =>
            for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w)
        )
    }

    /**
      * 执行“this”和“other”的左外部联接。对于`this中的每个元素（k，v）`
      *
      * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
      * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
      * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
      * using the existing partitioner/parallelism level.
      */
    def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = self.withScope {
        leftOuterJoin(other, defaultPartitioner(self, other))
    }

    /**
      * 执行“this”和“other”的左外部联接。对于`this中的每个元素（k，v）`
      *
      * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
      * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
      * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
      * partition the output RDD.
      */
    def leftOuterJoin[W](
                            other: RDD[(K, W)],
                            partitioner: Partitioner): RDD[(K, (V, Option[W]))] = self.withScope {
        this.cogroup(other, partitioner).flatMapValues { pair =>
            if (pair._2.isEmpty) {
                pair._1.iterator.map(v => (v, None))
            } else {
                for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, Some(w))
            }
        }
    }

    /**
      * 执行“this”和“other”的左外部联接。对于`this中的每个元素（k，v）`
      *
      * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
      * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
      * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
      * into `numPartitions` partitions.
      */
    def leftOuterJoin[W](
                            other: RDD[(K, W)],
                            numPartitions: Int): RDD[(K, (V, Option[W]))] = self.withScope {
        leftOuterJoin(other, new HashPartitioner(numPartitions))
    }

    /**
      * 执行“this”和“other”的右外联接。对于“other”中的每个元素（k，w），生成的RDD将包含“this”中v的所有对（k，（Some（v），w）），或者如果“this’中没有元素具有关键字k，则包含对（k（None，w）。
      *
      * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
      * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
      * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
      * RDD using the existing partitioner/parallelism level.
      */
    def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = self.withScope {
        rightOuterJoin(other, defaultPartitioner(self, other))
    }

    /**
      * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
      * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
      * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
      * partition the output RDD.
      */
    def rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
    : RDD[(K, (Option[V], W))] = self.withScope {
        this.cogroup(other, partitioner).flatMapValues { pair =>
            if (pair._1.isEmpty) {
                pair._2.iterator.map(w => (None, w))
            } else {
                for (v <- pair._1.iterator; w <- pair._2.iterator) yield (Some(v), w)
            }
        }
    }

    /**
      * 通过flatMap函数传递键值对RDD中的每个值，而不更改键；这也保留了原始RDD的分区。
      *
      * Pass each value in the key-value pair RDD through a flatMap function without changing the
      * keys; this also retains the original RDD's partitioning.
      */
    def flatMapValues[U](f: V => TraversableOnce[U]): RDD[(K, U)] = self.withScope {
        val cleanF = self.context.clean(f)
        new MapPartitionsRDD[(K, U), (K, V)](self,
            (context, pid, iter) => iter.flatMap { case (k, v) =>
                cleanF(v).map(x => (k, x))
            },
            preservesPartitioning = true)
    }

    /**
      * 对于“this”或“other”中的每个键k，返回一个生成的RDD，该RDD包含一个元组，该元组具有“this”和“other”中该键的值列表。
      *
      * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
      * list of values for that key in `this` as well as `other`.
      */
    def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner)
    : RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
        if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
            throw new SparkException("HashPartitioner cannot partition array keys.")
        }
        val cg = new CoGroupedRDD[K](Seq(self, other), partitioner)
        cg.mapValues { case Array(vs, w1s) =>
            (vs.asInstanceOf[Iterable[V]], w1s.asInstanceOf[Iterable[W]])
        }
    }

    /**
      * 通过映射函数传递键值对RDD中的每个值，而不改变键；这也保留了原始RDD的分区。
      * Pass each value in the key-value pair RDD through a map function without changing the keys;
      * this also retains the original RDD's partitioning.
      */
    def mapValues[U](f: V => U): RDD[(K, U)] = self.withScope {
        val cleanF = self.context.clean(f)
        new MapPartitionsRDD[(K, U), (K, V)](self,
            (context, pid, iter) => iter.map { case (k, v) => (k, cleanF(v)) },
            preservesPartitioning = true)
    }

    /**
      * 执行“this”和“other”的右外联接。对于“other”中的每个元素（k，w），所得到的RDD将包含“this”中v的所有对（k，（Some（v），w）），或者如果“this”中没有元素具有关键字k，则包含对（k（None，w）。
      *
      * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
      * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
      * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
      * RDD into the given number of partitions.
      */
    def rightOuterJoin[W](
                             other: RDD[(K, W)],
                             numPartitions: Int): RDD[(K, (Option[V], W))] = self.withScope {
        rightOuterJoin(other, new HashPartitioner(numPartitions))
    }

    /**
      * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
      * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
      * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
      * element (k, w) in `other`, the resulting RDD will either contain all pairs
      * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
      * in `this` have key k. Hash-partitions the resulting RDD using the existing partitioner/
      * parallelism level.
      */
    def fullOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], Option[W]))] = self.withScope {
        fullOuterJoin(other, defaultPartitioner(self, other))
    }

    /**
      * 执行“this”和“other”的完全外部联接。对于`this中的每个元素（k，v）`
      *
      * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
      * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
      * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
      * element (k, w) in `other`, the resulting RDD will either contain all pairs
      * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
      * in `this` have key k. Uses the given Partitioner to partition the output RDD.
      */
    def fullOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
    : RDD[(K, (Option[V], Option[W]))] = self.withScope {
        this.cogroup(other, partitioner).flatMapValues {
            case (vs, Seq()) => vs.iterator.map(v => (Some(v), None))
            case (Seq(), ws) => ws.iterator.map(w => (None, Some(w)))
            case (vs, ws) => for (v <- vs.iterator; w <- ws.iterator) yield (Some(v), Some(w))
        }
    }

    /**
      * 执行“this”和“other”的完全外部联接。对于“this”中的每个元素（k，v），所得到的RDD将包含“other”中w的所有对（k，（Some（v），Some（w）），
      * 或者如果“other’中没有元素具有关键字k，则包含对（k、（Some），None））。
      *
      * Perform a full outer join of `this` and `other`. For each element (k, v) in `this`, the
      * resulting RDD will either contain all pairs (k, (Some(v), Some(w))) for w in `other`, or
      * the pair (k, (Some(v), None)) if no elements in `other` have key k. Similarly, for each
      * element (k, w) in `other`, the resulting RDD will either contain all pairs
      * (k, (Some(v), Some(w))) for v in `this`, or the pair (k, (None, Some(w))) if no elements
      * in `this` have key k. Hash-partitions the resulting RDD into the given number of partitions.
      */
    def fullOuterJoin[W](
                            other: RDD[(K, W)],
                            numPartitions: Int): RDD[(K, (Option[V], Option[W]))] = self.withScope {
        fullOuterJoin(other, new HashPartitioner(numPartitions))
    }

    /**
      * 将此RDD中的键值对作为Map返回给master。
      *
      * Return the key-value pairs in this RDD to the master as a Map.
      *
      * Warning: this doesn't return a multimap (so if you have multiple values to the same key, only
      * one value per key is preserved in the map returned)
      *
      * @note this method should only be used if the resulting data is expected to be small, as
      *       all the data is loaded into the driver's memory.
      */
    def collectAsMap(): Map[K, V] = self.withScope {
        val data = self.collect()
        val map = new mutable.HashMap[K, V]
        map.sizeHint(data.length)
        data.foreach { pair => map.put(pair._1, pair._2) }
        map
    }

    /**
      * 对于“this”或“other1”、“other2”或“other3”中的每个键k，返回一个结果RDD，该RDD包含一个元组，该元组具有“this”、“other1”、‘other2’和“other3”中该键的值列表。
      *
      * For each key k in `this` or `other1` or `other2` or `other3`,
      * return a resulting RDD that contains a tuple with the list of values
      * for that key in `this`, `other1`, `other2` and `other3`.
      */
    def cogroup[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)])
    : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
        cogroup(other1, other2, other3, defaultPartitioner(self, other1, other2, other3))
    }

    /**
      * 对于“this”或“other1”、“other2”或“other3”中的每个键k，返回一个结果RDD，该RDD包含一个元组，该元组具有“this”、“other1”、‘other2’和“other3”中该键的值列表。
      *
      * For each key k in `this` or `other1` or `other2` or `other3`,
      * return a resulting RDD that contains a tuple with the list of values
      * for that key in `this`, `other1`, `other2` and `other3`.
      */
    def cogroup[W1, W2, W3](other1: RDD[(K, W1)],
                            other2: RDD[(K, W2)],
                            other3: RDD[(K, W3)],
                            partitioner: Partitioner)
    : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
        if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
            throw new SparkException("HashPartitioner cannot partition array keys.")
        }
        val cg = new CoGroupedRDD[K](Seq(self, other1, other2, other3), partitioner)
        cg.mapValues { case Array(vs, w1s, w2s, w3s) =>
            (vs.asInstanceOf[Iterable[V]],
                w1s.asInstanceOf[Iterable[W1]],
                w2s.asInstanceOf[Iterable[W2]],
                w3s.asInstanceOf[Iterable[W3]])
        }
    }

    /**
      * 对于“this”或“other”中的每个键k，返回一个结果RDD，该RDD包含一个元组，该元组具有“this”和“other’中该键的值列表。
      *
      * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
      * list of values for that key in `this` as well as `other`.
      */
    def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
        cogroup(other, defaultPartitioner(self, other))
    }

    /**
      * 对于“this”、“other1”或“other2”中的每个键k，返回一个结果RDD，该RDD包含一个元组，该元组具有“this”、“other1”和“other2”中该键的值列表。
      *
      * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
      * tuple with the list of values for that key in `this`, `other1` and `other2`.
      */
    def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
    : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
        cogroup(other1, other2, defaultPartitioner(self, other1, other2))
    }

    /**
      * 对于“this”或“other”中的每个键k，返回一个生成的RDD，该RDD包含一个元组，该元组具有“this”和“other”中该键的值列表。
      *
      * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
      * list of values for that key in `this` as well as `other`.
      */
    def cogroup[W](
                      other: RDD[(K, W)],
                      numPartitions: Int): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
        cogroup(other, new HashPartitioner(numPartitions))
    }

    /**
      * 对于“this”或“other1”或“other2”中的每个键k，返回一个结果RDD，该RDD包含一个元组，该元组具有“this”、“other1”和“other2”中该键的值列表。
      *
      * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
      * tuple with the list of values for that key in `this`, `other1` and `other2`.
      */
    def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], numPartitions: Int)
    : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
        cogroup(other1, other2, new HashPartitioner(numPartitions))
    }

    /**
      * 对于“this”或“other1”、“other2”或“other3”中的每个键k，返回一个结果RDD，该RDD包含一个元组，该元组具有“this”、“other1”、‘other2’和“other3”中该键的值列表。
      *
      * For each key k in `this` or `other1` or `other2` or `other3`,
      * return a resulting RDD that contains a tuple with the list of values
      * for that key in `this`, `other1`, `other2` and `other3`.
      */
    def cogroup[W1, W2, W3](other1: RDD[(K, W1)],
                            other2: RDD[(K, W2)],
                            other3: RDD[(K, W3)],
                            numPartitions: Int)
    : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
        cogroup(other1, other2, other3, new HashPartitioner(numPartitions))
    }

    /** Alias for cogroup. */
    def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
        cogroup(other, defaultPartitioner(self, other))
    }

    /** Alias for cogroup. */
    def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
    : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
        cogroup(other1, other2, defaultPartitioner(self, other1, other2))
    }

    /**
      * 对于“this”或“other1”或“other2”中的每个键k，返回一个结果RDD，该RDD包含一个元组，该元组具有“this”、“other1”和“other2”中该键的值列表。
      *
      * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
      * tuple with the list of values for that key in `this`, `other1` and `other2`.
      */
    def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], partitioner: Partitioner)
    : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
        if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
            throw new SparkException("HashPartitioner cannot partition array keys.")
        }
        val cg = new CoGroupedRDD[K](Seq(self, other1, other2), partitioner)
        cg.mapValues { case Array(vs, w1s, w2s) =>
            (vs.asInstanceOf[Iterable[V]],
                w1s.asInstanceOf[Iterable[W1]],
                w2s.asInstanceOf[Iterable[W2]])
        }
    }

    /** Alias for cogroup. */
    def groupWith[W1, W2, W3](other1: RDD[(K, W1)], other2: RDD[(K, W2)], other3: RDD[(K, W3)])
    : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2], Iterable[W3]))] = self.withScope {
        cogroup(other1, other2, other3, defaultPartitioner(self, other1, other2, other3))
    }

    /**
      * 返回一个RDD，其中包含“this”中的对，这些对的键不在“other”中。
      *
      * Return an RDD with the pairs from `this` whose keys are not in `other`.
      *
      * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
      * RDD will be less than or equal to us.
      */
    def subtractByKey[W: ClassTag](other: RDD[(K, W)]): RDD[(K, V)] = self.withScope {
        subtractByKey(other, self.partitioner.getOrElse(new HashPartitioner(self.partitions.length)))
    }

    /**
      * 返回一个RDD，其中包含“this”中的对，这些对的键不在“other”中。
      *
      * Return an RDD with the pairs from `this` whose keys are not in `other`.
      */
    def subtractByKey[W: ClassTag](
                                      other: RDD[(K, W)],
                                      numPartitions: Int): RDD[(K, V)] = self.withScope {
        subtractByKey(other, new HashPartitioner(numPartitions))
    }

    /**
      * 返回一个RDD，其中包含“this”中的对，这些对的键不在“other”中。
      *
      * Return an RDD with the pairs from `this` whose keys are not in `other`.
      */
    def subtractByKey[W: ClassTag](other: RDD[(K, W)], p: Partitioner): RDD[(K, V)] = self.withScope {
        new SubtractedRDD[K, V, W](self, other, p)
    }

    /**
      * 返回RDD中键“key”的值列表。如果RDD只搜索键映射到的分区，就可以有效地执行此操作。
      *
      * Return the list of values in the RDD for key `key`. This operation is done efficiently if the
      * RDD has a known partitioner by only searching the partition that the key maps to.
      */
    def lookup(key: K): Seq[V] = self.withScope {
        self.partitioner match {
            case Some(p) =>
                val index = p.getPartition(key)
                val process = (it: Iterator[(K, V)]) => {
                    val buf = new ArrayBuffer[V]
                    for (pair <- it if pair._1 == key) {
                        buf += pair._2
                    }
                    buf
                }: Seq[V]
                val res = self.context.runJob(self, process, Array(index))
                res(0)
            case None =>
                self.filter(_._1 == key).map(_._2).collect()
        }
    }

    /**
      * 使用Hadoop“OutputFormat”类将RDD输出到任何Hadoop支持的文件系统，该类支持此RDD中的键和值类型K和V。
      *
      * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
      * supporting the key and value types K and V in this RDD.
      */
    def saveAsHadoopFile[F <: OutputFormat[K, V]](
                                                     path: String)(implicit fm: ClassTag[F]): Unit = self.withScope {
        saveAsHadoopFile(path, keyClass, valueClass, fm.runtimeClass.asInstanceOf[Class[F]])
    }

    /**
      * 使用Hadoop“OutputFormat”类将RDD输出到任何Hadoop支持的文件系统，该类支持此RDD中的键和值类型K和V。使用提供的编解码器压缩结果。
      *
      * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
      * supporting the key and value types K and V in this RDD. Compress the result with the
      * supplied codec.
      */
    def saveAsHadoopFile[F <: OutputFormat[K, V]](
                                                     path: String,
                                                     codec: Class[_ <: CompressionCodec])(implicit fm: ClassTag[F]): Unit = self.withScope {
        val runtimeClass = fm.runtimeClass
        saveAsHadoopFile(path, keyClass, valueClass, runtimeClass.asInstanceOf[Class[F]], codec)
    }

    /**
      * 使用Hadoop“OutputFormat”类将RDD输出到任何Hadoop支持的文件系统，该类支持此RDD中的键和值类型K和V。使用提供的编解码器进行压缩。
      *
      * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
      * supporting the key and value types K and V in this RDD. Compress with the supplied codec.
      */
    def saveAsHadoopFile(
                            path: String,
                            keyClass: Class[_],
                            valueClass: Class[_],
                            outputFormatClass: Class[_ <: OutputFormat[_, _]],
                            codec: Class[_ <: CompressionCodec]): Unit = self.withScope {
        saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass,
            new JobConf(self.context.hadoopConfiguration), Some(codec))
    }

    /**
      * 使用Hadoop“OutputFormat”类将RDD输出到任何Hadoop支持的文件系统，该类支持此RDD中的键和值类型K和V。
      *
      * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
      * supporting the key and value types K and V in this RDD.
      *
      * @note We should make sure our tasks are idempotent when speculation is enabled, i.e. do
      *       not use output committer that writes data directly.
      *       There is an example in https://issues.apache.org/jira/browse/SPARK-10063 to show the bad
      *       result of using direct output committer with speculation enabled.
      */
    def saveAsHadoopFile(
                            path: String,
                            keyClass: Class[_],
                            valueClass: Class[_],
                            outputFormatClass: Class[_ <: OutputFormat[_, _]],
                            conf: JobConf = new JobConf(self.context.hadoopConfiguration),
                            codec: Option[Class[_ <: CompressionCodec]] = None): Unit = self.withScope {
        // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
        val hadoopConf = conf
        hadoopConf.setOutputKeyClass(keyClass)
        hadoopConf.setOutputValueClass(valueClass)
        conf.setOutputFormat(outputFormatClass)
        for (c <- codec) {
            hadoopConf.setCompressMapOutput(true)
            hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true")
            hadoopConf.setMapOutputCompressorClass(c)
            hadoopConf.set("mapreduce.output.fileoutputformat.compress.codec", c.getCanonicalName)
            hadoopConf.set("mapreduce.output.fileoutputformat.compress.type",
                CompressionType.BLOCK.toString)
        }

        // Use configured output committer if already set
        if (conf.getOutputCommitter == null) {
            hadoopConf.setOutputCommitter(classOf[FileOutputCommitter])
        }

        // When speculation is on and output committer class name contains "Direct", we should warn
        // users that they may loss data if they are using a direct output committer.
        val speculationEnabled = self.conf.getBoolean("org.apache.spark.speculation", false)
        val outputCommitterClass = hadoopConf.get("mapred.output.committer.class", "")
        if (speculationEnabled && outputCommitterClass.contains("Direct")) {
            val warningMessage =
                s"$outputCommitterClass may be an output committer that writes data directly to " +
                    "the final location. Because speculation is enabled, this output committer may " +
                    "cause data loss (see the case in SPARK-10063). If possible, please use an output " +
                    "committer that does not have this behavior (e.g. FileOutputCommitter)."
            logWarning(warningMessage)
        }

        FileOutputFormat.setOutputPath(hadoopConf,
            SparkHadoopWriterUtils.createPathFromString(path, hadoopConf))
        saveAsHadoopDataset(hadoopConf)
    }

    /**
      * 将RDD输出到任何Hadoop支持的存储系统，使用该存储系统的Hadoop JobConf对象。JobConf应该设置OutputFormat和所需的任何输出路径（例如，要写入的表名），其方式与为Hadoop MapReduce作业配置的方式相同。
      *
      * Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for
      * that storage system. The JobConf should set an OutputFormat and any output paths required
      * (e.g. a table name to write to) in the same way as it would be configured for a Hadoop
      * MapReduce job.
      */
    def saveAsHadoopDataset(conf: JobConf): Unit = self.withScope {
        // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
        val hadoopConf = conf
        val outputFormatInstance = hadoopConf.getOutputFormat
        val keyClass = hadoopConf.getOutputKeyClass
        val valueClass = hadoopConf.getOutputValueClass
        if (outputFormatInstance == null) {
            throw new SparkException("Output format class not set")
        }
        if (keyClass == null) {
            throw new SparkException("Output key class not set")
        }
        if (valueClass == null) {
            throw new SparkException("Output value class not set")
        }
        SparkHadoopUtil.get.addCredentials(hadoopConf)

        logDebug("Saving as hadoop file of type (" + keyClass.getSimpleName + ", " +
            valueClass.getSimpleName + ")")

        if (SparkHadoopWriterUtils.isOutputSpecValidationEnabled(self.conf)) {
            // FileOutputFormat ignores the filesystem parameter
            val ignoredFs = FileSystem.get(hadoopConf)
            hadoopConf.getOutputFormat.checkOutputSpecs(ignoredFs, hadoopConf)
        }

        val writer = new SparkHadoopWriter(hadoopConf)
        writer.preSetup()

        val writeToFile = (context: TaskContext, iter: Iterator[(K, V)]) => {
            // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
            // around by taking a mod. We expect that no task will be attempted 2 billion times.
            val taskAttemptId = (context.taskAttemptId % Int.MaxValue).toInt

            val (outputMetrics, callback) = SparkHadoopWriterUtils.initHadoopOutputMetrics(context)

            writer.setup(context.stageId, context.partitionId, taskAttemptId)
            writer.open()
            var recordsWritten = 0L

            Utils.tryWithSafeFinallyAndFailureCallbacks {
                while (iter.hasNext) {
                    val record = iter.next()
                    writer.write(record._1.asInstanceOf[AnyRef], record._2.asInstanceOf[AnyRef])

                    // Update bytes written metric every few records
                    SparkHadoopWriterUtils.maybeUpdateOutputMetrics(outputMetrics, callback, recordsWritten)
                    recordsWritten += 1
                }
            }(finallyBlock = writer.close())
            writer.commit()
            outputMetrics.setBytesWritten(callback())
            outputMetrics.setRecordsWritten(recordsWritten)
        }

        self.context.runJob(self, writeToFile)
        writer.commitJob()
    }

    private[spark] def valueClass: Class[_] = vt.runtimeClass

    /**
      * 使用新的Hadoop API“OutputFormat”（mapreduce.OutputFormat）对象将RDD输出到任何Hadoop支持的文件系统，该对象支持此RDD中的键和值类型K和V。
      *
      * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
      * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
      */
    def saveAsNewAPIHadoopFile[F <: NewOutputFormat[K, V]](
                                                              path: String)(implicit fm: ClassTag[F]): Unit = self.withScope {
        saveAsNewAPIHadoopFile(path, keyClass, valueClass, fm.runtimeClass.asInstanceOf[Class[F]])
    }

    /**
      * 使用新的Hadoop API“OutputFormat”（mapreduce.OutputFormat）对象将RDD输出到任何Hadoop支持的文件系统，该对象支持此RDD中的键和值类型K和V。
      *
      * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
      * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
      */
    def saveAsNewAPIHadoopFile(
                                  path: String,
                                  keyClass: Class[_],
                                  valueClass: Class[_],
                                  outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
                                  conf: Configuration = self.context.hadoopConfiguration): Unit = self.withScope {
        // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
        val hadoopConf = conf
        val job = NewAPIHadoopJob.getInstance(hadoopConf)
        job.setOutputKeyClass(keyClass)
        job.setOutputValueClass(valueClass)
        job.setOutputFormatClass(outputFormatClass)
        val jobConfiguration = job.getConfiguration
        jobConfiguration.set("mapreduce.output.fileoutputformat.outputdir", path)
        saveAsNewAPIHadoopDataset(jobConfiguration)
    }

    /**
      * 使用该存储系统的Hadoop Configuration对象，将RDD输出到具有新Hadoop API的任何支持Hadoop的存储系统。
      * Conf应该设置OutputFormat和所需的任何输出路径（例如，要写入的表名），其方式与为Hadoop MapReduce作业配置的方式相同。
      *
      * Output the RDD to any Hadoop-supported storage system with new Hadoop API, using a Hadoop
      * Configuration object for that storage system. The Conf should set an OutputFormat and any
      * output paths required (e.g. a table name to write to) in the same way as it would be
      * configured for a Hadoop MapReduce job.
      *
      * @note We should make sure our tasks are idempotent when speculation is enabled, i.e. do
      *       not use output committer that writes data directly.
      *       There is an example in https://issues.apache.org/jira/browse/SPARK-10063 to show the bad
      *       result of using direct output committer with speculation enabled.
      */
    def saveAsNewAPIHadoopDataset(conf: Configuration): Unit = self.withScope {
        SparkHadoopMapReduceWriter.write(
            rdd = self,
            hadoopConf = conf)
    }

    /**
      * 返回一个RDD，其中包含每个元组的键。
      *
      * Return an RDD with the keys of each tuple.
      */
    def keys: RDD[K] = self.map(_._1)

    /**
      * 返回一个RDD，其中包含每个元组的值。
      *
      * Return an RDD with the values of each tuple.
      */
    def values: RDD[V] = self.map(_._2)

    private[spark] def keyOrdering: Option[Ordering[K]] = Option(ord)
}
