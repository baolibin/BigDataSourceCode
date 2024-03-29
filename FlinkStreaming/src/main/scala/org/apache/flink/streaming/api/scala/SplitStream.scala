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

package org.apache.flink.streaming.api.scala

import org.apache.flink.annotation.Public
import org.apache.flink.streaming.api.datastream.{SplitStream => SplitJavaStream}

/**
  * SplitStream表示已使用[[org.apache.flink.streaming.api.collector.selector.OutputSelector]].
  * 可以使用[[SplitStream#select（）]]函数选择命名输出。要在整个输出上应用转换，只需在此流上调用适当的方法。
  *
  * The SplitStream represents an operator that has been split using an
  * [[org.apache.flink.streaming.api.collector.selector.OutputSelector]].
  * Named outputs can be selected using the [[SplitStream#select()]] function.
  * To apply a transformation on the whole output simply call
  * the appropriate method on this stream.
  */
@Public
class SplitStream[T](javaStream: SplitJavaStream[T]) extends DataStream[T](javaStream) {

    /**
      * 设置下一个运算符将接收其值的输出名称。
      *
      * Sets the output names for which the next operator will receive values.
      */
    def select(outputNames: String*): DataStream[T] =
        asScalaStream(javaStream.select(outputNames: _*))
}
