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

package org.apache.spark.memory

import org.apache.spark.SparkConf
import org.apache.spark.storage.BlockId

/**
  * 静态内存管理，它将堆空间静态地划分为不相交的区域。
  *
  * A [[MemoryManager]] that statically partitions the heap space into disjoint regions.
  *
  * The sizes of the execution and storage regions are determined through
  * `org.apache.spark.shuffle.memoryFraction` and `org.apache.spark.storage.memoryFraction` respectively. The two
  * regions are cleanly separated such that neither usage can borrow memory from the other.
  */
private[spark] class StaticMemoryManager(
                                            conf: SparkConf,
                                            maxOnHeapExecutionMemory: Long,
                                            override val maxOnHeapStorageMemory: Long,
                                            numCores: Int)
    extends MemoryManager(
        conf,
        numCores,
        maxOnHeapStorageMemory,
        maxOnHeapExecutionMemory) {

    // Max number of bytes worth of blocks to evict when unrolling
    private val maxUnrollMemory: Long = {
        (maxOnHeapStorageMemory * conf.getDouble("org.apache.spark.storage.unrollFraction", 0.2)).toLong
    }

    // The StaticMemoryManager does not support off-heap storage memory:
    offHeapExecutionMemoryPool.incrementPoolSize(offHeapStorageMemoryPool.poolSize)
    offHeapStorageMemoryPool.decrementPoolSize(offHeapStorageMemoryPool.poolSize)

    def this(conf: SparkConf, numCores: Int) {
        this(
            conf,
            StaticMemoryManager.getMaxExecutionMemory(conf),
            StaticMemoryManager.getMaxStorageMemory(conf),
            numCores)
    }

    override def maxOffHeapStorageMemory: Long = 0L

    override def acquireStorageMemory(
                                         blockId: BlockId,
                                         numBytes: Long,
                                         memoryMode: MemoryMode): Boolean = synchronized {
        require(memoryMode != MemoryMode.OFF_HEAP,
            "StaticMemoryManager does not support off-heap storage memory")
        if (numBytes > maxOnHeapStorageMemory) {
            // Fail fast if the block simply won't fit
            logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
                s"memory limit ($maxOnHeapStorageMemory bytes)")
            false
        } else {
            onHeapStorageMemoryPool.acquireMemory(blockId, numBytes)
        }
    }

    override def acquireUnrollMemory(
                                        blockId: BlockId,
                                        numBytes: Long,
                                        memoryMode: MemoryMode): Boolean = synchronized {
        require(memoryMode != MemoryMode.OFF_HEAP,
            "StaticMemoryManager does not support off-heap unroll memory")
        val currentUnrollMemory = onHeapStorageMemoryPool.memoryStore.currentUnrollMemory
        val freeMemory = onHeapStorageMemoryPool.memoryFree
        // When unrolling, we will use all of the existing free memory, and, if necessary,
        // some extra space freed from evicting cached blocks. We must place a cap on the
        // amount of memory to be evicted by unrolling, however, otherwise unrolling one
        // big block can blow away the entire cache.
        val maxNumBytesToFree = math.max(0, maxUnrollMemory - currentUnrollMemory - freeMemory)
        // Keep it within the range 0 <= X <= maxNumBytesToFree
        val numBytesToFree = math.max(0, math.min(maxNumBytesToFree, numBytes - freeMemory))
        onHeapStorageMemoryPool.acquireMemory(blockId, numBytes, numBytesToFree)
    }

    private[memory]
    override def acquireExecutionMemory(
                                           numBytes: Long,
                                           taskAttemptId: Long,
                                           memoryMode: MemoryMode): Long = synchronized {
        memoryMode match {
            case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
            case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.acquireMemory(numBytes, taskAttemptId)
        }
    }
}


private[spark] object StaticMemoryManager {

    private val MIN_MEMORY_BYTES = 32 * 1024 * 1024

    /**
      * 返回最大可获得的存储内存字节数
      *
      * Return the total amount of memory available for the storage region, in bytes.
      */
    private def getMaxStorageMemory(conf: SparkConf): Long = {
        // 系统最大内存
        val systemMaxMemory = conf.getLong("org.apache.spark.testing.memory", Runtime.getRuntime.maxMemory)
        // 存储内存占系统最大内存比例
        val memoryFraction = conf.getDouble("org.apache.spark.storage.memoryFraction", 0.6)
        // 避免出现OOM，存储内存会设置一个安全系数
        val safetyFraction = conf.getDouble("org.apache.spark.storage.safetyFraction", 0.9)
        (systemMaxMemory * memoryFraction * safetyFraction).toLong
    }

    /**
      * 返回最大可获得的执行内存字节数
      *
      * Return the total amount of memory available for the execution region, in bytes.
      */
    private def getMaxExecutionMemory(conf: SparkConf): Long = {
        // 系统最大内存
        val systemMaxMemory = conf.getLong("org.apache.spark.testing.memory", Runtime.getRuntime.maxMemory)

        if (systemMaxMemory < MIN_MEMORY_BYTES) {
            throw new IllegalArgumentException(s"System memory $systemMaxMemory must " +
                s"be at least $MIN_MEMORY_BYTES. Please increase heap size using the --driver-memory " +
                s"option or org.apache.spark.driver.memory in Spark configuration.")
        }
        if (conf.contains("org.apache.spark.executor.memory")) {
            val executorMemory = conf.getSizeAsBytes("org.apache.spark.executor.memory")
            if (executorMemory < MIN_MEMORY_BYTES) {
                throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
                    s"$MIN_MEMORY_BYTES. Please increase executor memory using the " +
                    s"--executor-memory option or org.apache.spark.executor.memory in Spark configuration.")
            }
        }
        // 执行内存占系统最大内存比例
        val memoryFraction = conf.getDouble("org.apache.spark.shuffle.memoryFraction", 0.2)
        // 避免出现OOM，执行内存会设置一个安全系数
        val safetyFraction = conf.getDouble("org.apache.spark.shuffle.safetyFraction", 0.8)
        (systemMaxMemory * memoryFraction * safetyFraction).toLong
    }

}
