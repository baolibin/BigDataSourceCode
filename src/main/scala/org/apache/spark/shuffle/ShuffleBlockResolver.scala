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

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.ShuffleBlockId

private[spark]
/**
  * Implementers of this trait understand how to retrieve block data for a logical shuffle block
  * identifier (i.e. map, reduce, and shuffle). Implementations may use files or file segments to
  * encapsulate shuffle data. This is used by the BlockStore to abstract over different shuffle
  * implementations when shuffle data is retrieved.
  */
/**
  * shuffle数据块与物理文件的映射
  * 目前唯一实现为IndexShuffleBlockResolver类.
  * 该类对于同一个map任务所生成的shuffle文件，将存储在同一个文件中，对于每一个reduce需要读取偏移量存储在对应的index文件中。
  * 其中.data后缀文件为数据文件，.index后缀为对应的偏移量的索引文件。
  */
trait ShuffleBlockResolver {
    type ShuffleId = Int

    /**
      * Retrieve the data for the specified block. If the data for that block is not available,
      * throws an unspecified exception.
      */
    def getBlockData(blockId: ShuffleBlockId): ManagedBuffer

    def stop(): Unit
}
