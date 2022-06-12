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

/**
  * 一种[[MemoryManager]]，它在执行和存储之间强制一个软边界，这样任何一方都可以从另一方借用内存。
  *
  * A [[MemoryManager]] that enforces a soft boundary between execution and storage such that
  * either side can borrow memory from the other.
  *
  * The region shared between execution and storage is a fraction of (the total heap space - 300MB)
  * configurable through `org.apache.spark.memory.fraction` (default 0.6). The position of the boundary
  * within this space is further determined by `org.apache.spark.memory.storageFraction` (default 0.5).
  * This means the size of the storage region is 0.6 * 0.5 = 0.3 of the heap space by default.
  *
  * Storage can borrow as much execution memory as is free until execution reclaims its space.
  * When this happens, cached blocks will be evicted from memory until sufficient borrowed
  * memory is released to satisfy the execution memory request.
  *
  * Similarly, execution can borrow as much storage memory as is free. However, execution
  * memory is *never* evicted by storage due to the complexities involved in implementing this.
  * The implication is that attempts to cache blocks may fail if execution has already eaten
  * up most of the storage space, in which case the new blocks will be evicted immediately
  * according to their respective storage levels.
  *
  * @param onHeapStorageRegionSize Size of the storage region, in bytes.
  *                                This region is not statically reserved; execution can borrow from
  *                                it if necessary. Cached blocks can be evicted only if actual
  *                                storage memory usage exceeds this region.
  */
private[spark] class UnifiedMemoryManager private[memory](
                                                             conf: SparkConf,
                                                             val maxHeapMemory: Long,
                                                             onHeapStorageRegionSize: Long,
                                                             numCores: Int)
    extends MemoryManager(
        conf,
        numCores,
        onHeapStorageRegionSize,
        maxHeapMemory - onHeapStorageRegionSize) {

    override def acquireUnrollMemory(
                                        blockId: BlockId,
                                        numBytes: Long,
                                        memoryMode: MemoryMode): Boolean = synchronized {
        acquireStorageMemory(blockId, numBytes, memoryMode)
    }

    assertInvariants()

    override def acquireStorageMemory(
                                         blockId: BlockId,
                                         numBytes: Long,
                                         memoryMode: MemoryMode): Boolean = synchronized {
        assertInvariants()
        assert(numBytes >= 0)
        val (executionPool, storagePool, maxMemory) = memoryMode match {
            case MemoryMode.ON_HEAP => (
                onHeapExecutionMemoryPool,
                onHeapStorageMemoryPool,
                maxOnHeapStorageMemory)
            case MemoryMode.OFF_HEAP => (
                offHeapExecutionMemoryPool,
                offHeapStorageMemoryPool,
                maxOffHeapStorageMemory)
        }
        if (numBytes > maxMemory) {
            // Fail fast if the block simply won't fit
            logInfo(s"Will not store $blockId as the required space ($numBytes bytes) exceeds our " +
                s"memory limit ($maxMemory bytes)")
            return false
        }
        if (numBytes > storagePool.memoryFree) {
            // There is not enough free memory in the storage pool, so try to borrow free memory from
            // the execution pool.
            val memoryBorrowedFromExecution = Math.min(executionPool.memoryFree,
                numBytes - storagePool.memoryFree)
            executionPool.decrementPoolSize(memoryBorrowedFromExecution)
            storagePool.incrementPoolSize(memoryBorrowedFromExecution)
        }
        storagePool.acquireMemory(blockId, numBytes)
    }

    override def maxOnHeapStorageMemory: Long = synchronized {
        maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed
    }

    override def maxOffHeapStorageMemory: Long = synchronized {
        maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed
    }

    private def assertInvariants(): Unit = {
        assert(onHeapExecutionMemoryPool.poolSize + onHeapStorageMemoryPool.poolSize == maxHeapMemory)
        assert(
            offHeapExecutionMemoryPool.poolSize + offHeapStorageMemoryPool.poolSize == maxOffHeapMemory)
    }

    /**
      * 尝试为当前任务获取多达'numBytes'的执行内存，并返回获得的字节数，如果无法分配，则返回0。
      *
      * Try to acquire up to `numBytes` of execution memory for the current task and return the
      * number of bytes obtained, or 0 if none can be allocated.
      *
      * This call may block until there is enough free memory in some situations, to make sure each
      * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
      * active tasks) before it is forced to spill. This can happen if the number of tasks increase
      * but an older task had a lot of memory already.
      */
    override private[memory] def acquireExecutionMemory(
                                                           numBytes: Long,
                                                           taskAttemptId: Long,
                                                           memoryMode: MemoryMode): Long = synchronized {
        assertInvariants()
        assert(numBytes >= 0)
        val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
            case MemoryMode.ON_HEAP => (
                onHeapExecutionMemoryPool,
                onHeapStorageMemoryPool,
                onHeapStorageRegionSize,
                maxHeapMemory)
            case MemoryMode.OFF_HEAP => (
                offHeapExecutionMemoryPool,
                offHeapStorageMemoryPool,
                offHeapStorageMemory,
                maxOffHeapMemory)
        }

        /**
          * 通过逐出缓存块来增加执行池，从而缩小存储池。
          *
          * Grow the execution pool by evicting cached blocks, thereby shrinking the storage pool.
          *
          * When acquiring memory for a task, the execution pool may need to make multiple
          * attempts. Each attempt must be able to evict storage in case another task jumps in
          * and caches a large block between the attempts. This is called once per attempt.
          */
        def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit = {
            if (extraMemoryNeeded > 0) {
                // There is not enough free memory in the execution pool, so try to reclaim memory from
                // storage. We can reclaim any free memory from the storage pool. If the storage pool
                // has grown to become larger than `storageRegionSize`, we can evict blocks and reclaim
                // the memory that storage has borrowed from execution.
                val memoryReclaimableFromStorage = math.max(
                    storagePool.memoryFree,
                    storagePool.poolSize - storageRegionSize)
                if (memoryReclaimableFromStorage > 0) {
                    // Only reclaim as much space as is necessary and available:
                    val spaceToReclaim = storagePool.freeSpaceToShrinkPool(
                        math.min(extraMemoryNeeded, memoryReclaimableFromStorage))
                    storagePool.decrementPoolSize(spaceToReclaim)
                    executionPool.incrementPoolSize(spaceToReclaim)
                }
            }
        }

        /**
          * 退出存储内存后执行池的大小。
          *
          * The size the execution pool would have after evicting storage memory.
          *
          * The execution memory pool divides this quantity among the active tasks evenly to cap
          * the execution memory allocation for each task. It is important to keep this greater
          * than the execution pool size, which doesn't take into account potential memory that
          * could be freed by evicting storage. Otherwise we may hit SPARK-12155.
          *
          * Additionally, this quantity should be kept below `maxMemory` to arbitrate fairness
          * in execution memory allocation across tasks, Otherwise, a task may occupy more than
          * its fair share of execution memory, mistakenly thinking that other tasks can acquire
          * the portion of storage memory that cannot be evicted.
          */
        def computeMaxExecutionPoolSize(): Long = {
            maxMemory - math.min(storagePool.memoryUsed, storageRegionSize)
        }

        executionPool.acquireMemory(
            numBytes, taskAttemptId, maybeGrowExecutionPool, computeMaxExecutionPoolSize)
    }
}

object UnifiedMemoryManager {

    // Set aside a fixed amount of memory for non-storage, non-execution purposes.
    // This serves a function similar to `org.apache.spark.memory.fraction`, but guarantees that we reserve
    // sufficient memory for the system even for small heaps. E.g. if we have a 1GB JVM, then
    // the memory used for execution and storage will be (1024 - 300) * 0.6 = 434MB by default.
    // 预留内存大小，默认300M
    private val RESERVED_SYSTEM_MEMORY_BYTES = 300 * 1024 * 1024

    def apply(conf: SparkConf, numCores: Int): UnifiedMemoryManager = {
        val maxMemory = getMaxMemory(conf)
        new UnifiedMemoryManager(
            conf,
            maxHeapMemory = maxMemory,
            onHeapStorageRegionSize =
                (maxMemory * conf.getDouble("org.apache.spark.memory.storageFraction", 0.5)).toLong,
            numCores = numCores)
    }

    /**
      * 返回执行和存储之间共享的内存总量（字节）。
      *
      * Return the total amount of memory shared between execution and storage, in bytes.
      */
    private def getMaxMemory(conf: SparkConf): Long = {
        // 系统内存
        val systemMemory = conf.getLong("org.apache.spark.testing.memory", Runtime.getRuntime.maxMemory)
        // 预留内存，默认300M
        val reservedMemory = conf.getLong("org.apache.spark.testing.reservedMemory",
            if (conf.contains("org.apache.spark.testing")) 0 else RESERVED_SYSTEM_MEMORY_BYTES)
        val minSystemMemory = (reservedMemory * 1.5).ceil.toLong
        if (systemMemory < minSystemMemory) {
            throw new IllegalArgumentException(s"System memory $systemMemory must " +
                s"be at least $minSystemMemory. Please increase heap size using the --driver-memory " +
                s"option or org.apache.spark.driver.memory in Spark configuration.")
        }
        // SPARK-12759 Check executor memory to fail fast if memory is insufficient
        if (conf.contains("org.apache.spark.executor.memory")) {
            val executorMemory = conf.getSizeAsBytes("org.apache.spark.executor.memory")
            if (executorMemory < minSystemMemory) {
                throw new IllegalArgumentException(s"Executor memory $executorMemory must be at least " +
                    s"$minSystemMemory. Please increase executor memory using the " +
                    s"--executor-memory option or org.apache.spark.executor.memory in Spark configuration.")
            }
        }
        // 可用内存
        val usableMemory = systemMemory - reservedMemory
        val memoryFraction = conf.getDouble("org.apache.spark.memory.fraction", 0.6)
        // 执行内存和存储内存占比内存大小
        (usableMemory * memoryFraction).toLong
    }
}
