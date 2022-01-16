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

package org.apache.spark.broadcast

import java.io.Serializable

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

/**
  * 广播变量。广播变量允许程序员在每台机器上缓存一个只读变量，而不是将其副本与任务一起发送。
  * 例如，它们可以有效地为每个节点提供一个大型输入数据集的副本。
  * Spark还尝试使用高效的广播算法来分配广播变量，以降低通信成本。
  *
  * A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable
  * cached on each machine rather than shipping a copy of it with tasks. They can be used, for
  * example, to give every node a copy of a large input dataset in an efficient manner. Spark also
  * attempts to distribute broadcast variables using efficient broadcast algorithms to reduce
  * communication cost.
  *
  * Broadcast variables are created from a variable `v` by calling
  * [[org.apache.spark.SparkContext#broadcast]].
  * The broadcast variable is a wrapper around `v`, and its value can be accessed by calling the
  * `value` method. The interpreter session below shows this:
  *
  * {{{
  * scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
  * broadcastVar: org.apache.org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(0)
  *
  * scala> broadcastVar.value
  * res0: Array[Int] = Array(1, 2, 3)
  * }}}
  *
  * After the broadcast variable is created, it should be used instead of the value `v` in any
  * functions run on the cluster so that `v` is not shipped to the nodes more than once.
  * In addition, the object `v` should not be modified after it is broadcast in order to ensure
  * that all nodes get the same value of the broadcast variable (e.g. if the variable is shipped
  * to a new node later).
  *
  * @param id A unique identifier for the broadcast variable.
  * @tparam T Type of the data contained in the broadcast variable.
  */
abstract class Broadcast[T: ClassTag](val id: Long) extends Serializable with Logging {

    /**
      * Flag signifying whether the broadcast variable is valid
      * (that is, not already destroyed) or not.
      */
    @volatile private var _isValid = true

    private var _destroySite = ""

    /**
      * 获取广播变量的值
      * Get the broadcasted value. */
    def value: T = {
        assertValid()
        getValue()
    }

    /**
      * 检查广播变量的值是否有效，无效则抛出异常
      * Check if this broadcast is valid. If not valid, exception is thrown. */
    protected def assertValid() {
        if (!_isValid) {
            throw new SparkException(
                "Attempted to use %s after it was destroyed (%s) ".format(toString, _destroySite))
        }
    }

    override def toString: String = "Broadcast(" + id + ")"

    /**
      * 在执行器上异步删除此广播的缓存副本。
      * Asynchronously delete cached copies of this broadcast on the executors.
      * If the broadcast is used after this is called, it will need to be re-sent to each executor.
      */
    def unpersist() {
        unpersist(blocking = false)
    }

    /**
      * Delete cached copies of this broadcast on the executors. If the broadcast is used after
      * this is called, it will need to be re-sent to each executor.
      *
      * @param blocking Whether to block until unpersisting has completed
      */
    def unpersist(blocking: Boolean) {
        assertValid()
        doUnpersist(blocking)
    }

    /**
      * Destroy all data and metadata related to this broadcast variable. Use this with caution;
      * once a broadcast variable has been destroyed, it cannot be used again.
      * This method blocks until destroy has completed
      */
    def destroy() {
        destroy(blocking = true)
    }

    /**
      * 销毁与此广播变量相关的所有数据和元数据。
      * Destroy all data and metadata related to this broadcast variable. Use this with caution;
      * once a broadcast variable has been destroyed, it cannot be used again.
      *
      * @param blocking Whether to block until destroy has completed
      */
    private[spark] def destroy(blocking: Boolean) {
        assertValid()
        _isValid = false
        _destroySite = Utils.getCallSite().shortForm
        logInfo("Destroying %s (from %s)".format(toString, _destroySite))
        doDestroy(blocking)
    }

    /**
      * 实际获取广播值。广播类的具体实现必须定义自己获取值的方法
      * Actually get the broadcasted value. Concrete implementations of Broadcast class must
      * define their own way to get the value.
      */
    protected def getValue(): T

    /**
      * Actually unpersist the broadcasted value on the executors. Concrete implementations of
      * Broadcast class must define their own logic to unpersist their own data.
      */
    protected def doUnpersist(blocking: Boolean)

    /**
      * Actually destroy all data and metadata related to this broadcast variable.
      * Implementation of Broadcast class must define their own logic to destroy their own
      * state.
      */
    protected def doDestroy(blocking: Boolean)

    /**
      * Whether this Broadcast is actually usable. This should be false once persisted state is
      * removed from the driver.
      */
    private[spark] def isValid: Boolean = {
        _isValid
    }
}
