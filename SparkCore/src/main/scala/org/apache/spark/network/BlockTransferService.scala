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

package org.apache.spark.network

import java.io.{Closeable, File}
import java.nio.ByteBuffer

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, ShuffleClient}
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.ThreadUtils

private[spark]
abstract class BlockTransferService extends ShuffleClient with Closeable with Logging {

    /**
      * 通过向传输服务提供可用于获取本地块或放置本地块的BlockDataManager来初始化传输服务。
      *
      * Initialize the transfer service by giving it the BlockDataManager that can be used to fetch
      * local blocks or put local blocks.
      */
    def init(blockDataManager: BlockDataManager): Unit

    /**
      * 拆除中转服务。
      *
      * Tear down the transfer service.
      */
    def close(): Unit

    /**
      * 服务正在侦听的端口号，仅在调用[[init]]后可用。
      *
      * Port number the service is listening on, available only after [[init]] is invoked.
      */
    def port: Int

    /**
      * 服务正在侦听的主机名，仅在调用[[init]]后可用。
      *
      * Host name the service is listening on, available only after [[init]] is invoked.
      */
    def hostName: String

    /**
      * 从远程节点异步获取块序列，仅在调用[[init]]后可用。
      *
      * Fetch a sequence of blocks from a remote node asynchronously,
      * available only after [[init]] is invoked.
      *
      * Note that this API takes a sequence so the implementation can batch requests, and does not
      * return a future so the underlying implementation can invoke onBlockFetchSuccess as soon as
      * the data of a block is fetched, rather than waiting for all blocks to be fetched.
      */
    override def fetchBlocks(
                                host: String,
                                port: Int,
                                execId: String,
                                blockIds: Array[String],
                                listener: BlockFetchingListener,
                                shuffleFiles: Array[File]): Unit

    /**
      * 将单个块上载到远程节点，仅在调用[[init]]后可用。
      *
      * Upload a single block to a remote node, available only after [[init]] is invoked.
      */
    def uploadBlock(
                       hostname: String,
                       port: Int,
                       execId: String,
                       blockId: BlockId,
                       blockData: ManagedBuffer,
                       level: StorageLevel,
                       classTag: ClassTag[_]): Future[Unit]

    /**
      * [[fetchBlocks]]的一种特殊情况，因为它只提取一个块，并且正在阻塞。
      *
      * A special case of [[fetchBlocks]], as it fetches only one block and is blocking.
      *
      * It is also only available after [[init]] is invoked.
      */
    def fetchBlockSync(host: String, port: Int, execId: String, blockId: String): ManagedBuffer = {
        // A monitor for the thread to wait on.
        val result = Promise[ManagedBuffer]()
        fetchBlocks(host, port, execId, Array(blockId),
            new BlockFetchingListener {
                override def onBlockFetchFailure(blockId: String, exception: Throwable): Unit = {
                    result.failure(exception)
                }

                override def onBlockFetchSuccess(blockId: String, data: ManagedBuffer): Unit = {
                    val ret = ByteBuffer.allocate(data.size.toInt)
                    ret.put(data.nioByteBuffer())
                    ret.flip()
                    result.success(new NioManagedBuffer(ret))
                }
            }, shuffleFiles = null)
        ThreadUtils.awaitResult(result.future, Duration.Inf)
    }

    /**
      * 将单个块上载到远程节点，仅在调用[[init]]后可用。
      *
      * Upload a single block to a remote node, available only after [[init]] is invoked.
      *
      * This method is similar to [[uploadBlock]], except this one blocks the thread
      * until the upload finishes.
      */
    def uploadBlockSync(
                           hostname: String,
                           port: Int,
                           execId: String,
                           blockId: BlockId,
                           blockData: ManagedBuffer,
                           level: StorageLevel,
                           classTag: ClassTag[_]): Unit = {
        val future = uploadBlock(hostname, port, execId, blockId, blockData, level, classTag)
        ThreadUtils.awaitResult(future, Duration.Inf)
    }
}
