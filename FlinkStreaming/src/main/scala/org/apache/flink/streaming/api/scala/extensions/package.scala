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

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.streaming.api.windowing.windows.Window

/**
  * acceptPartialFunctions使用具有唯一名称的方法扩展原始数据流，这些方法委托给核心高阶函数（例如“map”），
  * 这样我们就可以解决以函数为参数的重载方法也不能接受部分函数的问题。
  * 这使得可以直接应用模式匹配来分解元组、case类和集合等输入。
  *
  * acceptPartialFunctions extends the original DataStream with methods with unique names
  * that delegate to core higher-order functions (e.g. `map`) so that we can work around
  * the fact that overloaded methods taking functions as parameters can't accept partial
  * functions as well. This enables the possibility to directly apply pattern matching
  * to decompose inputs such as tuples, case classes and collections.
  *
  * The following is a small example that showcases how this extensions would work on
  * a Flink data stream:
  *
  * {{{
  *   object Main {
  *     import org.apache.flink.streaming.api.scala.extensions._
  *     case class Point(x: Double, y: Double)
  *     def main(args: Array[String]): Unit = {
  *       val env = StreamExecutionEnvironment.getExecutionEnvironment
  *       val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
  *       ds.filterWith {
  *         case Point(x, _) => x > 1
  *       }.reduceWith {
  *         case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
  *       }.mapWith {
  *         case Point(x, y) => (x, y)
  *       }.flatMapWith {
  *         case (x, y) => Seq('x' -> x, 'y' -> y)
  *       }.keyingBy {
  *         case (id, value) => id
  *       }
  *     }
  *   }
  * }}}
  *
  * 这个扩展包括对所有数据流表示的几个隐式转换，这些数据流表示可以从这个特性中获得。
  * 要使用这组扩展方法，用户必须通过导入acceptPartialFunctions。
  *
  * The extension consists of several implicit conversions over all the data stream representations
  * that could gain from this feature. To use this set of extensions methods the user has to
  * explicitly opt-in by importing
  * `org.apache.flink.streaming.api.scala.extensions.acceptPartialFunctions`.
  *
  * For more information and usage examples please consult the Apache Flink official documentation.
  *
  */
package object extensions {

    @PublicEvolving
    implicit def acceptPartialFunctions[T](ds: DataStream[T]): OnDataStream[T] =
        new OnDataStream[T](ds)

    @PublicEvolving
    implicit def acceptPartialFunctions[T, K](ds: KeyedStream[T, K]): OnKeyedStream[T, K] =
        new OnKeyedStream[T, K](ds)

    @PublicEvolving
    implicit def acceptPartialFunctions[L, R, K, W <: Window](
                                                                     ds: JoinedStreams[L, R]#Where[K]#EqualTo#WithWindow[W]): OnJoinedStream[L, R, K, W] =
        new OnJoinedStream[L, R, K, W](ds)

    @PublicEvolving
    implicit def acceptPartialFunctions[IN1, IN2](
                                                         ds: ConnectedStreams[IN1, IN2]): OnConnectedStream[IN1, IN2] =
        new OnConnectedStream[IN1, IN2](ds)

    @PublicEvolving
    implicit def acceptPartialFunctions[T, K, W <: Window](
                                                                  ds: WindowedStream[T, K, W]): OnWindowedStream[T, K, W] =
        new OnWindowedStream[T, K, W](ds)

}
