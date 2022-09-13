/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.scala.function

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.functions.AbstractRichFunction
import org.apache.flink.api.common.state.KeyedStateStore
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector

/**
  * 使用上下文检索额外信息，在键控（分组）窗口上计算的函数的基抽象类。
  *
  * Base abstract class for functions that are evaluated over keyed (grouped)
  * windows using a context for retrieving extra information.
  *
  * @tparam IN  The type of the input value.
  * @tparam OUT The type of the output value.
  * @tparam KEY The type of the key.
  * @tparam W   The type of the window.
  */
@PublicEvolving
abstract class ProcessWindowFunction[IN, OUT, KEY, W <: Window]
    extends AbstractRichFunction {

    /**
      * 计算窗口并输出一个或多个元素。
      *
      * Evaluates the window and outputs none or several elements.
      *
      * @param key      The key for which this window is evaluated.
      * @param context  The context in which the window is being evaluated.
      * @param elements The elements in the window being evaluated.
      * @param out      A collector for emitting elements.
      * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
      */
    @throws[Exception]
    def process(key: KEY, context: Context, elements: Iterable[IN], out: Collector[OUT])

    /**
      * 清除窗口时，删除[[上下文]]中的任何状态。
      *
      * Deletes any state in the [[Context]] when the Window is purged.
      *
      * @param context The context to which the window is being evaluated
      * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
      */
    @throws[Exception]
    def clear(context: Context) {}

    /**
      * 保存窗口元数据的上下文
      *
      * The context holding window metadata
      */
    abstract class Context {
        /**
          * 返回正在计算的窗口。
          *
          * Returns the window that is being evaluated.
          */
        def window: W

        /**
          * 返回当前处理时间。
          *
          * Returns the current processing time.
          */
        def currentProcessingTime: Long

        /**
          * 返回当前事件时间水印。
          *
          * Returns the current event-time watermark.
          */
        def currentWatermark: Long

        /**
          * 每个键和每个窗口状态的状态访问器。
          *
          * State accessor for per-key and per-window state.
          */
        def windowState: KeyedStateStore

        /**
          * 每个键全局状态的状态访问器。
          *
          * State accessor for per-key global state.
          */
        def globalState: KeyedStateStore

        /**
          * 向[[OutputTag]]标识的侧输出发出记录。
          *
          * Emits a record to the side output identified by the [[OutputTag]].
          */
        def output[X](outputTag: OutputTag[X], value: X);
    }
}
