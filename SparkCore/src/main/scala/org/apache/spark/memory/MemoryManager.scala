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

import javax.annotation.concurrent.GuardedBy
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.MemoryAllocator

/**
  * 一种抽象内存管理器，用于施行execution和storage之间如何共享内存。
  *
  * An abstract memory manager that enforces how memory is shared between execution and storage.
  *
  * 执行内存包括: shuffle, join, 排序, 聚合
  * 存储内存包括: 缓存数据, 以及通过集群传播内在的数据, 每个JVM上存在一个MemoryManager
  * In this context, execution memory refers to that used for computation in shuffles, joins,
  * sorts and aggregations, while storage memory refers to that used for caching and propagating
  * internal data across the cluster. There exists one MemoryManager per JVM.
  */
private[spark] abstract class MemoryManager(
                                                   conf: SparkConf,
                                                   numCores: Int,
                                                   onHeapStorageMemory: Long,
                                                   onHeapExecutionMemory: Long) extends Logging {

    // -- Methods related to memory allocation policies and bookkeeping ------------------------------

    /**
      * 跟踪将使用sun在JVM堆上还是堆外分配内存。
      * Tracks whether Tungsten memory will be allocated on the JVM heap or off-heap using
      * sun.misc.Unsafe.
      */
    final val tungstenMemoryMode: MemoryMode = {
        if (conf.getBoolean("org.apache.spark.memory.offHeap.enabled", false)) {
            require(conf.getSizeAsBytes("org.apache.spark.memory.offHeap.size", 0) > 0,
                "org.apache.spark.memory.offHeap.size must be > 0 when org.apache.spark.memory.offHeap.enabled == true")
            require(Platform.unaligned(),
                "No support for unaligned Unsafe. Set org.apache.spark.memory.offHeap.enabled to false.")
            MemoryMode.OFF_HEAP
        } else {
            MemoryMode.ON_HEAP
        }
    }
    /**
      * 分配内存供不安全代码/代码使用。
      * Allocates memory for use by Unsafe/Tungsten code.
      */
    private[memory] final val tungstenMemoryAllocator: MemoryAllocator = {
        tungstenMemoryMode match {
            case MemoryMode.ON_HEAP => MemoryAllocator.HEAP
            case MemoryMode.OFF_HEAP => MemoryAllocator.UNSAFE
        }
    }
    /**
      * 默认页面大小，以字节为单位。
      * The default page size, in bytes.
      *
      * If user didn't explicitly set "org.apache.spark.buffer.pageSize", we figure out the default value
      * by looking at the number of cores available to the process, and the total amount of memory,
      * and then divide it by a factor of safety.
      */
    val pageSizeBytes: Long = {
        val minPageSize = 1L * 1024 * 1024 // 1MB
        val maxPageSize = 64L * minPageSize // 64MB
        val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors()
        // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
        val safetyFactor = 16
        val maxTungstenMemory: Long = tungstenMemoryMode match {
            case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.poolSize
            case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.poolSize
        }
        val size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor)
        val default = math.min(maxPageSize, math.max(minPageSize, size))
        conf.getSizeAsBytes("org.apache.spark.buffer.pageSize", default)
    }
    @GuardedBy("this")
    protected val onHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.ON_HEAP)
    // 堆内内存大小分配
    onHeapStorageMemoryPool.incrementPoolSize(onHeapStorageMemory)
    onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory)
    @GuardedBy("this")
    protected val offHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.OFF_HEAP)
    @GuardedBy("this")
    protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.ON_HEAP)
    // 堆外内存大小分配， offHeapStorageMemory + offHeapExecutionMemory = maxOffHeapMemory
    offHeapExecutionMemoryPool.incrementPoolSize(maxOffHeapMemory - offHeapStorageMemory)
    offHeapStorageMemoryPool.incrementPoolSize(offHeapStorageMemory)
    @GuardedBy("this")
    protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP)
    // 堆外内存参数由spark.memory.offHeap.enabled决定，开启了堆外内存大小等于spark.memory.offHeap.size的值
    protected[this] val maxOffHeapMemory = conf.getSizeAsBytes("org.apache.spark.memory.offHeap.size", 0)
    // 存储内存等于参数spark.memory.storageFraction和总内存的乘积
    protected[this] val offHeapStorageMemory =
        (maxOffHeapMemory * conf.getDouble("org.apache.spark.memory.storageFraction", 0.5)).toLong

    /**
      * 最大堆内存储内存
      * Total available on heap memory for storage, in bytes. This amount can vary over time,
      * depending on the MemoryManager implementation.
      * In this model, this is equivalent to the amount of memory not occupied by execution.
      */
    def maxOnHeapStorageMemory: Long

    /**
      * 最大堆外存储内存
      * Total available off heap memory for storage, in bytes. This amount can vary over time,
      * depending on the MemoryManager implementation.
      */
    def maxOffHeapStorageMemory: Long

    /**
      * 设置此管理器用于移出缓存块的[[MemoryStore]]。
      * Set the [[MemoryStore]] used by this manager to evict cached blocks.
      * This must be set after construction due to initialization ordering constraints.
      */
    final def setMemoryStore(store: MemoryStore): Unit = synchronized {
        onHeapStorageMemoryPool.setMemoryStore(store)
        offHeapStorageMemoryPool.setMemoryStore(store)
    }

    /**
      * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
      *
      * @return whether all N bytes were successfully granted.
      */
    def acquireStorageMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

    /**
      * Acquire N bytes of memory to unroll the given block, evicting existing ones if necessary.
      *
      * This extra method allows subclasses to differentiate behavior between acquiring storage
      * memory and acquiring unroll memory. For instance, the memory management model in Spark
      * 1.5 and before places a limit on the amount of space that can be freed from unrolling.
      *
      * @return whether all N bytes were successfully granted.
      */
    def acquireUnrollMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

    /**
      * Release all storage memory acquired.
      */
    final def releaseAllStorageMemory(): Unit = synchronized {
        onHeapStorageMemoryPool.releaseAllMemory()
        offHeapStorageMemoryPool.releaseAllMemory()
    }

    /**
      * Release N bytes of unroll memory.
      */
    final def releaseUnrollMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
        releaseStorageMemory(numBytes, memoryMode)
    }

    /**
      * Release N bytes of storage memory.
      */
    def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
        memoryMode match {
            case MemoryMode.ON_HEAP => onHeapStorageMemoryPool.releaseMemory(numBytes)
            case MemoryMode.OFF_HEAP => offHeapStorageMemoryPool.releaseMemory(numBytes)
        }
    }

    /**
      * Execution memory currently in use, in bytes.
      */
    final def executionMemoryUsed: Long = synchronized {
        onHeapExecutionMemoryPool.memoryUsed + offHeapExecutionMemoryPool.memoryUsed
    }

    /**
      * Storage memory currently in use, in bytes.
      */
    final def storageMemoryUsed: Long = synchronized {
        onHeapStorageMemoryPool.memoryUsed + offHeapStorageMemoryPool.memoryUsed
    }

    /**
      * Try to acquire up to `numBytes` of execution memory for the current task and return the
      * number of bytes obtained, or 0 if none can be allocated.
      *
      * This call may block until there is enough free memory in some situations, to make sure each
      * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
      * active tasks) before it is forced to spill. This can happen if the number of tasks increase
      * but an older task had a lot of memory already.
      */
    private[memory]
    def acquireExecutionMemory(
                                      numBytes: Long,
                                      taskAttemptId: Long,
                                      memoryMode: MemoryMode): Long

    // -- Fields related to Tungsten managed memory -------------------------------------------------

    /**
      * Release numBytes of execution memory belonging to the given task.
      */
    private[memory]
    def releaseExecutionMemory(
                                      numBytes: Long,
                                      taskAttemptId: Long,
                                      memoryMode: MemoryMode): Unit = synchronized {
        memoryMode match {
            case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
            case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
        }
    }

    /**
      * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
      *
      * @return the number of bytes freed.
      */
    private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = synchronized {
        onHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId) +
                offHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId)
    }

    /**
      * Returns the execution memory consumption, in bytes, for the given task.
      */
    private[memory] def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long = synchronized {
        onHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId) +
                offHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId)
    }
}
