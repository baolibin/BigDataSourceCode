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

package org.apache.spark.rpc

import java.util.concurrent.TimeoutException

import scala.concurrent.Future
import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.util.{ThreadUtils, Utils}

/**
  * 如果RpcTimeout修改“TimeoutException”，则引发异常。
  *
  * An exception thrown if RpcTimeout modifies a `TimeoutException`.
  */
private[rpc] class RpcTimeoutException(message: String, cause: TimeoutException)
    extends TimeoutException(message) {
    initCause(cause)
}


/**
  * 将超时与描述相关联，以便在发生TimeoutException时，可以将有关超时的其他上下文修改为异常消息。
  *
  * Associates a timeout with a description so that a when a TimeoutException occurs, additional
  * context about the timeout can be amended to the exception message.
  *
  * @param duration    timeout duration in seconds
  * @param timeoutProp the configuration property that controls this timeout
  */
private[spark] class RpcTimeout(val duration: FiniteDuration, val timeoutProp: String)
    extends Serializable {

    /**
      * 修改TimeoutException的标准消息以包含描述
      *
      * Amends the standard message of TimeoutException to include the description
      */
    private def createRpcTimeoutException(te: TimeoutException): RpcTimeoutException = {
        new RpcTimeoutException(te.getMessage + ". This timeout is controlled by " + timeoutProp, te)
    }

    /**
      * PartialFunction匹配TimeoutException并将超时描述添加到消息中
      *
      * PartialFunction to match a TimeoutException and add the timeout description to the message
      *
      * @note This can be used in the recover callback of a Future to add to a TimeoutException
      *       Example:
      *       val timeout = new RpcTimeout(5 millis, "short timeout")
      *       Future(throw new TimeoutException).recover(timeout.addMessageIfTimeout)
      */
    def addMessageIfTimeout[T]: PartialFunction[Throwable, T] = {
        // The exception has already been converted to a RpcTimeoutException so just raise it
        case rte: RpcTimeoutException => throw rte
        // Any other TimeoutException get converted to a RpcTimeoutException with modified message
        case te: TimeoutException => throw createRpcTimeoutException(te)
    }

    /**
      * 等待完成的结果并返回。如果此范围内没有结果timeout，抛出[[RpcTimeoutException]]以指示哪个配置控制超时。
      *
      * Wait for the completed result and return it. If the result is not available within this
      * timeout, throw a [[RpcTimeoutException]] to indicate which configuration controls the timeout.
      *
      * @param future the `Future` to be awaited
      * @throws RpcTimeoutException if after waiting for the specified time `future`
      *                             is still not ready
      */
    def awaitResult[T](future: Future[T]): T = {
        try {
            ThreadUtils.awaitResult(future, duration)
        } catch addMessageIfTimeout
    }
}


private[spark] object RpcTimeout {

    /**
      * 在配置中查找timeout属性，并使用描述中的属性键创建RpcTimeout。
      *
      * Lookup the timeout property in the configuration and create
      * a RpcTimeout with the property key in the description.
      *
      * @param conf        configuration properties containing the timeout
      * @param timeoutProp property key for the timeout in seconds
      * @throws NoSuchElementException if property is not set
      */
    def apply(conf: SparkConf, timeoutProp: String): RpcTimeout = {
        val timeout = {
            conf.getTimeAsSeconds(timeoutProp).seconds
        }
        new RpcTimeout(timeout, timeoutProp)
    }

    /**
      * 在配置中查找timeout属性，并使用描述中的属性键创建RpcTimeout。如果未设置属性，则使用给定的默认值
      *
      * Lookup the timeout property in the configuration and create
      * a RpcTimeout with the property key in the description.
      * Uses the given default value if property is not set
      *
      * @param conf         configuration properties containing the timeout
      * @param timeoutProp  property key for the timeout in seconds
      * @param defaultValue default timeout value in seconds if property not found
      */
    def apply(conf: SparkConf, timeoutProp: String, defaultValue: String): RpcTimeout = {
        val timeout = {
            conf.getTimeAsSeconds(timeoutProp, defaultValue).seconds
        }
        new RpcTimeout(timeout, timeoutProp)
    }

    /**
      * 在配置中查找超时属性的优先级列表，并使用描述中的第一个设置属性键创建RpcTimeout。如果未设置属性，则使用给定的默认值
      *
      * Lookup prioritized list of timeout properties in the configuration
      * and create a RpcTimeout with the first set property key in the
      * description.
      * Uses the given default value if property is not set
      *
      * @param conf            configuration properties containing the timeout
      * @param timeoutPropList prioritized list of property keys for the timeout in seconds
      * @param defaultValue    default timeout value in seconds if no properties found
      */
    def apply(conf: SparkConf, timeoutPropList: Seq[String], defaultValue: String): RpcTimeout = {
        require(timeoutPropList.nonEmpty)

        // Find the first set property or use the default value with the first property
        val itr = timeoutPropList.iterator
        var foundProp: Option[(String, String)] = None
        while (itr.hasNext && foundProp.isEmpty) {
            val propKey = itr.next()
            conf.getOption(propKey).foreach { prop => foundProp = Some(propKey, prop) }
        }
        val finalProp = foundProp.getOrElse(timeoutPropList.head, defaultValue)
        val timeout = {
            Utils.timeStringAsSeconds(finalProp._2).seconds
        }
        new RpcTimeout(timeout, finalProp._1)
    }
}
