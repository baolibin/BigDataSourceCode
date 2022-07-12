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
  * 这种特性的实现者了解如何为逻辑洗牌块标识符(即map、reduce和洗牌)检索块数据。
  * 实现可以使用文件或文件段来封装shuffle数据。当检索shuffle数据时，
  * BlockStore使用它对不同的shuffle实现进行抽象。
  *
  * Implementers of this trait understand how to retrieve block data for a logical shuffle block
  * identifier (i.e. map, reduce, and shuffle). Implementations may use files or file segments to
  * encapsulate shuffle data. This is used by the BlockStore to abstract over different shuffle
  * implementations when shuffle data is retrieved.
  */
trait ShuffleBlockResolver {
    type ShuffleId = Int

    /**
      * 检索指定块的数据。如果该块的数据不可用，则抛出未指定的异常。
      *
      * Retrieve the data for the specified block. If the data for that block is not available,
      * throws an unspecified exception.
      */
    def getBlockData(blockId: ShuffleBlockId): ManagedBuffer

    def stop(): Unit
}
