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

package org.apache.spark.memory;

import java.io.IOException;

import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * {@link TaskMemoryManager}的内存消费者，支持溢出。
 * <p>
 * A memory consumer of {@link TaskMemoryManager} that supports spilling.
 * <p>
 * Note: this only supports allocation / spilling of Tungsten memory.
 */
public abstract class MemoryConsumer {

    protected final TaskMemoryManager taskMemoryManager;
    private final long pageSize;
    private final MemoryMode mode;
    protected long used;

    protected MemoryConsumer(TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
        this.taskMemoryManager = taskMemoryManager;
        this.pageSize = pageSize;
        this.mode = mode;
    }

    protected MemoryConsumer(TaskMemoryManager taskMemoryManager) {
        this(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.ON_HEAP);
    }

    /**
     * Returns the memory mode, {@link MemoryMode#ON_HEAP} or {@link MemoryMode#OFF_HEAP}.
     */
    public MemoryMode getMode() {
        return mode;
    }

    /**
     * 返回已用内存的大小（以字节为单位）。
     * <p>
     * Returns the size of used memory in bytes.
     */
    protected long getUsed() {
        return used;
    }

    /**
     * 在建造过程中强制溢出。
     * <p>
     * Force spill during building.
     */
    public void spill() throws IOException {
        spill(Long.MAX_VALUE, this);
    }

    /**
     * 将一些数据溢出到磁盘以释放内存，当没有足够的内存用于任务时，TaskMemoryManager将调用这些数据。
     * <p>
     * Spill some data to disk to release memory, which will be called by TaskMemoryManager
     * when there is not enough memory for the task.
     * <p>
     * This should be implemented by subclass.
     * <p>
     * Note: In order to avoid possible deadlock, should not call acquireMemory() from spill().
     * <p>
     * Note: today, this only frees Tungsten-managed pages.
     * @param size    the amount of memory should be released
     * @param trigger the MemoryConsumer that trigger this spilling
     * @return the amount of released memory in bytes
     * @throws IOException
     */
    public abstract long spill(long size, MemoryConsumer trigger) throws IOException;

    /**
     * 分配“大小”的长数组。
     * <p>
     * Allocates a LongArray of `size`.
     */
    public LongArray allocateArray(long size) {
        long required = size * 8L;
        MemoryBlock page = taskMemoryManager.allocatePage(required, this);
        if (page == null || page.size() < required) {
            long got = 0;
            if (page != null) {
                got = page.size();
                taskMemoryManager.freePage(page, this);
            }
            taskMemoryManager.showMemoryUsage();
            throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
        }
        used += required;
        return new LongArray(page);
    }

    /**
     * 释放长数组。
     * <p>
     * Frees a LongArray.
     */
    public void freeArray(LongArray array) {
        freePage(array.memoryBlock());
    }

    /**
     * 为内存块分配至少“必需”字节。
     * <p>
     * Allocate a memory block with at least `required` bytes.
     * <p>
     * Throws IOException if there is not enough memory.
     * @throws OutOfMemoryError
     */
    protected MemoryBlock allocatePage(long required) {
        MemoryBlock page = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
        if (page == null || page.size() < required) {
            long got = 0;
            if (page != null) {
                got = page.size();
                taskMemoryManager.freePage(page, this);
            }
            taskMemoryManager.showMemoryUsage();
            throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
        }
        used += page.size();
        return page;
    }

    /**
     * Free a memory block.
     */
    protected void freePage(MemoryBlock page) {
        used -= page.size();
        taskMemoryManager.freePage(page, this);
    }

    /**
     * Allocates memory of `size`.
     */
    public long acquireMemory(long size) {
        long granted = taskMemoryManager.acquireExecutionMemory(size, this);
        used += granted;
        return granted;
    }

    /**
     * Release N bytes of memory.
     */
    public void freeMemory(long size) {
        taskMemoryManager.releaseExecutionMemory(size, this);
        used -= size;
    }
}
