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
package org.apache.flink.streaming.api.scala.extensions.impl.acceptPartialFunctions

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream}

/**
  * 包装数据流，允许使用匿名分部函数来提取元组、case类实例或集合中的项
  *
  * Wraps a data stream, allowing to use anonymous partial functions to
  * perform extraction of items in a tuple, case class instance or collection
  *
  * @param stream The wrapped data stream
  * @tparam T The type of the data stream items
  */
class OnDataStream[T](stream: DataStream[T]) {

    /**
      * 将函数“fun”应用于流的每个项
      *
      * Applies a function `fun` to each item of the stream
      *
      * @param fun The function to be applied to each item
      * @tparam R The type of the items in the returned stream
      * @return A dataset of R
      */
    @PublicEvolving
    def mapWith[R: TypeInformation](fun: T => R): DataStream[R] =
        stream.map(fun)

    /**
      * 将函数“fun”应用于流的每个项，生成将在结果流中展平的项集合
      *
      * Applies a function `fun` to each item of the stream, producing a collection of items
      * that will be flattened in the resulting stream
      *
      * @param fun The function to be applied to each item
      * @tparam R The type of the items in the returned stream
      * @return A dataset of R
      */
    @PublicEvolving
    def flatMapWith[R: TypeInformation](fun: T => TraversableOnce[R]): DataStream[R] =
        stream.flatMap(fun)

    /**
      * 将谓词“fun”应用于流的每个项，只保留谓词所包含的那些项
      *
      * Applies a predicate `fun` to each item of the stream, keeping only those for which
      * the predicate holds
      *
      * @param fun The predicate to be tested on each item
      * @return A dataset of R
      */
    @PublicEvolving
    def filterWith(fun: T => Boolean): DataStream[T] =
        stream.filter(fun)

    /**
      * 根据键控功能对项目进行键控`
      *
      * Keys the items according to a keying function `fun`
      *
      * @param fun The keying function
      * @tparam K The type of the key, for which type information must be known
      * @return A stream of Ts keyed by Ks
      */
    @PublicEvolving
    def keyingBy[K: TypeInformation](fun: T => K): KeyedStream[T, K] =
        stream.keyBy(fun)

}
