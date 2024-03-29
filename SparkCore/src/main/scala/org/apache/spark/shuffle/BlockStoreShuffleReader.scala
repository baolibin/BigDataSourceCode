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

import org.apache.spark._
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
  * 通过向其他节点的块存储请求从shuffle中获取和读取范围为[startPartition, endPartition]的分区。
  *
  * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
  * requesting them from other nodes' block stores.
  */
private[spark] class BlockStoreShuffleReader[K, C](
                                                      handle: BaseShuffleHandle[K, _, C],
                                                      startPartition: Int,
                                                      endPartition: Int,
                                                      context: TaskContext,
                                                      serializerManager: SerializerManager = SparkEnv.get.serializerManager,
                                                      blockManager: BlockManager = SparkEnv.get.blockManager,
                                                      mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
    extends ShuffleReader[K, C] with Logging {

    private val dep = handle.dependency

    /**
      * reduce task拉取map端输出的合并k-v对数据
      * Read the combined key-values for this reduce task
      */
    override def read(): Iterator[Product2[K, C]] = {
        val wrappedStreams = new ShuffleBlockFetcherIterator(
            context,
            blockManager.shuffleClient,
            blockManager,
            mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
            serializerManager.wrapStream,
            // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
            SparkEnv.get.conf.getSizeAsMb("org.apache.spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
            SparkEnv.get.conf.getInt("org.apache.spark.reducer.maxReqsInFlight", Int.MaxValue),
            SparkEnv.get.conf.get(config.REDUCER_MAX_REQ_SIZE_SHUFFLE_TO_MEM),
            SparkEnv.get.conf.getBoolean("org.apache.spark.shuffle.detectCorrupt", true))

        val serializerInstance = dep.serializer.newInstance()

        // Create a key/value iterator for each stream
        val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
            // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
            // NextIterator. The NextIterator makes sure that close() is called on the
            // underlying InputStream when all records have been read.
            serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
        }

        // Update the context task metrics for each record read.
        val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
        val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
            recordIter.map { record =>
                readMetrics.incRecordsRead(1)
                record
            },
            context.taskMetrics().mergeShuffleReadMetrics())

        // An interruptible iterator must be used here in order to support task cancellation
        val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

        val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
            if (dep.mapSideCombine) {
                // We are reading values that are already combined
                val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
                dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
            } else {
                // We don't know the value type, but also don't care -- the dependency *should*
                // have made sure its compatible w/ this aggregator, which will convert the value
                // type to the combined type C
                val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
                dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
            }
        } else {
            require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
            interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
        }

        // Sort the output if there is a sort ordering defined.
        dep.keyOrdering match {
            case Some(keyOrd: Ordering[K]) =>
                // Create an ExternalSorter to sort the data.
                val sorter =
                    new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
                sorter.insertAll(aggregatedIter)
                context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
                context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
                context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
                CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
            case None =>
                aggregatedIter
        }
    }
}
