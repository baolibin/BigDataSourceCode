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

package org.apache.spark.streaming.util;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * 这个抽象类表示一个预写日志（也称为日志），Spark Streaming使用该日志将接收到的数据（由接收器）和相关元数据保存到可靠的存储器中，
 * 以便在驱动程序故障后可以恢复这些数据。有关如何插入您自己的预写日志自定义实现的更多信息，请参阅Spark文档。
 * <p>
 * :: DeveloperApi ::
 * <p>
 * This abstract class represents a write ahead log (aka journal) that is used by Spark Streaming
 * to save the received data (by receivers) and associated metadata to a reliable storage, so that
 * they can be recovered after driver failures. See the Spark documentation for more information
 * on how to plug in your own custom implementation of a write ahead log.
 */
@org.apache.spark.annotation.DeveloperApi
public abstract class WriteAheadLog {
    /**
     * 将记录写入日志并返回记录句柄，该句柄包含读回已写入记录所需的所有信息。时间用于索引记录，以便以后可以清理。
     * 注意，该抽象类的实现必须确保在该函数返回时，写入的数据是持久的和可读的（使用记录句柄）。
     * <p>
     * Write the record to the log and return a record handle, which contains all the information
     * necessary to read back the written record. The time is used to the index the record,
     * such that it can be cleaned later. Note that implementations of this abstract class must
     * ensure that the written data is durable and readable (using the record handle) by the
     * time this function returns.
     */
    public abstract WriteAheadLogRecordHandle write(ByteBuffer record, long time);

    /**
     * 根据给定的记录句柄读取书面记录。
     * <p>
     * Read a written record based on the given record handle.
     */
    public abstract ByteBuffer read(WriteAheadLogRecordHandle handle);

    /**
     * 读取并返回所有已写入但尚未清理的记录的迭代器。
     * <p>
     * Read and return an iterator of all the records that have been written but not yet cleaned up.
     */
    public abstract Iterator<ByteBuffer> readAll();

    /**
     * 清除所有早于阈值时间的记录。它可以等待删除完成。
     * <p>
     * Clean all the records that are older than the threshold time. It can wait for
     * the completion of the deletion.
     */
    public abstract void clean(long threshTime, boolean waitForCompletion);

    /**
     * 关闭此日志并释放所有资源。
     * <p>
     * Close this log and release any resources.
     */
    public abstract void close();
}
