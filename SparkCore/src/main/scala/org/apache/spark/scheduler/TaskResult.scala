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

package org.apache.spark.scheduler

import java.io._
import java.nio.ByteBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.BlockId
import org.apache.spark.util.{AccumulatorV2, Utils}

import scala.collection.mutable.ArrayBuffer

// 任务结果。还包含对累加器变量的更新。
// Task result. Also contains updates to accumulator variables.
private[spark] sealed trait TaskResult[T]

/**
  * 对已存储在工作程序的BlockManager中的DirectTaskResult的引用。
  *
  * A reference to a DirectTaskResult that has been stored in the worker's BlockManager.
  */
private[spark] case class IndirectTaskResult[T](blockId: BlockId, size: Int)
    extends TaskResult[T] with Serializable

/**
  * TaskResult，包含任务的返回值和累加器更新。
  *
  * A TaskResult that contains the task's return value and accumulator updates.
  */
private[spark] class DirectTaskResult[T](
                                            var valueBytes: ByteBuffer,
                                            var accumUpdates: Seq[AccumulatorV2[_, _]])
    extends TaskResult[T] with Externalizable {

    private var valueObjectDeserialized = false
    private var valueObject: T = _

    def this() = this(null.asInstanceOf[ByteBuffer], null)

    override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
        out.writeInt(valueBytes.remaining)
        Utils.writeByteBuffer(valueBytes, out)
        out.writeInt(accumUpdates.size)
        accumUpdates.foreach(out.writeObject)
    }

    override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
        val blen = in.readInt()
        val byteVal = new Array[Byte](blen)
        in.readFully(byteVal)
        valueBytes = ByteBuffer.wrap(byteVal)

        val numUpdates = in.readInt
        if (numUpdates == 0) {
            accumUpdates = Seq()
        } else {
            val _accumUpdates = new ArrayBuffer[AccumulatorV2[_, _]]
            for (i <- 0 until numUpdates) {
                _accumUpdates += in.readObject.asInstanceOf[AccumulatorV2[_, _]]
            }
            accumUpdates = _accumUpdates
        }
        valueObjectDeserialized = false
    }

    /**
      * 当第一次调用“value（）”时，它需要从“valueBytes”反序列化“valueObject”。对于一个大型实例，可能需要数十秒的时间。因此，当第一次调用“value”时，调用者应该避免阻塞其他线程。
      *
      * When `value()` is called at the first time, it needs to deserialize `valueObject` from
      * `valueBytes`. It may cost dozens of seconds for a large instance. So when calling `value` at
      * the first time, the caller should avoid to block other threads.
      *
      * After the first time, `value()` is trivial and just returns the deserialized `valueObject`.
      */
    def value(resultSer: SerializerInstance = null): T = {
        if (valueObjectDeserialized) {
            valueObject
        } else {
            // This should not run when holding a lock because it may cost dozens of seconds for a large
            // value
            val ser = if (resultSer == null) SparkEnv.get.serializer.newInstance() else resultSer
            valueObject = ser.deserialize(valueBytes)
            valueObjectDeserialized = true
            valueObject
        }
    }
}
