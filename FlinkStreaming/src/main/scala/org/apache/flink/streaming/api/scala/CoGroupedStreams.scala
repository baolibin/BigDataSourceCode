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

import org.apache.flink.annotation.{PublicEvolving, Public}
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.datastream.{CoGroupedStreams => JavaCoGroupedStreams}
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
  * `CoGroupedStreams`表示已共同分组的两个[[DataStream]]。流式协作组操作在窗口中的元素上进行评估。
  *
  * `CoGroupedStreams` represents two [[DataStream]]s that have been co-grouped.
  * A streaming co-group operation is evaluated over elements in a window.
  *
  * To finalize the co-group operation you also need to specify a [[KeySelector]] for
  * both the first and second input and a [[WindowAssigner]]
  *
  * Note: Right now, the groups are being built in memory so you need to ensure that they don't
  * get too big. Otherwise the JVM might crash.
  *
  * Example:
  *
  * {{{
  * val one: DataStream[(String, Int)]  = ...
  * val two: DataStream[(String, Int)] = ...
  *
  * val result = one.coGroup(two)
  *     .where(new MyFirstKeySelector())
  *     .equalTo(new MyFirstKeySelector())
  *     .window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
  *     .apply(new MyCoGroupFunction())
  * } }}}
  */
@Public
class CoGroupedStreams[T1, T2](input1: DataStream[T1], input2: DataStream[T2]) {

    /**
      * 为来自第一个输入的元素指定[[键选择器]]。
      *
      * Specifies a [[KeySelector]] for elements from the first input.
      */
    def where[KEY: TypeInformation](keySelector: T1 => KEY): Where[KEY] = {
        val cleanFun = clean(keySelector)
        val keyType = implicitly[TypeInformation[KEY]]
        val javaSelector = new KeySelector[T1, KEY] with ResultTypeQueryable[KEY] {
            def getKey(in: T1) = cleanFun(in)

            override def getProducedType: TypeInformation[KEY] = keyType
        }
        new Where[KEY](javaSelector, keyType)
    }

    /**
      * 为第一个输入定义了[[键选择器]]的协作组操作。
      *
      * A co-group operation that has [[KeySelector]]s defined for the first input.
      *
      * You need to specify a [[KeySelector]] for the second input using [[equalTo()]]
      * before you can proceed with specifying a [[WindowAssigner]] using [[EqualTo.window()]].
      *
      * @tparam KEY Type of the key. This must be the same for both inputs
      */
    class Where[KEY](keySelector1: KeySelector[T1, KEY], keyType: TypeInformation[KEY]) {

        /**
          * 为第二个输入中的元素指定[[KeySelector]]。
          *
          * Specifies a [[KeySelector]] for elements from the second input.
          */
        def equalTo(keySelector: T2 => KEY): EqualTo = {
            val cleanFun = clean(keySelector)
            val localKeyType = keyType
            val javaSelector = new KeySelector[T2, KEY] with ResultTypeQueryable[KEY] {
                def getKey(in: T2) = cleanFun(in)

                override def getProducedType: TypeInformation[KEY] = localKeyType
            }
            new EqualTo(javaSelector)
        }

        /**
          * 为第一和第二输入定义的[[KeySelector]]的协同操作。
          *
          * A co-group operation that a [[KeySelector]] defined for the first and the second input.
          *
          * A window can now be specified using [[window()]].
          */
        class EqualTo(keySelector2: KeySelector[T2, KEY]) {

            /**
              * 指定协作组操作工作的窗口。
              *
              * Specifies the window on which the co-group operation works.
              */
            @PublicEvolving
            def window[W <: Window](
                                       assigner: WindowAssigner[_ >: JavaCoGroupedStreams.TaggedUnion[T1, T2], W])
            : WithWindow[W] = {
                if (keySelector1 == null || keySelector2 == null) {
                    throw new UnsupportedOperationException(
                        "You first need to specify KeySelectors for both inputs using where() and equalTo().")
                }
                new WithWindow[W](clean(assigner), null, null, null)
            }

            /**
              * 为两个输入定义了[[KeySelector]]和[[WindowAssigner]]的协作组操作。
              * A co-group operation that has [[KeySelector]]s defined for both inputs as
              * well as a [[WindowAssigner]].
              *
              * @tparam W Type of { @link Window} on which the co-group operation works.
              */
            @PublicEvolving
            class WithWindow[W <: Window](
                                             windowAssigner: WindowAssigner[_ >: JavaCoGroupedStreams.TaggedUnion[T1, T2], W],
                                             trigger: Trigger[_ >: JavaCoGroupedStreams.TaggedUnion[T1, T2], _ >: W],
                                             evictor: Evictor[_ >: JavaCoGroupedStreams.TaggedUnion[T1, T2], _ >: W],
                                             val allowedLateness: Time) {

                /**
                  * 设置应用于触发窗口发射的[[Trigger]]。
                  *
                  * Sets the [[Trigger]] that should be used to trigger window emission.
                  */
                @PublicEvolving
                def trigger(newTrigger: Trigger[_ >: JavaCoGroupedStreams.TaggedUnion[T1, T2], _ >: W])
                : WithWindow[W] = {
                    new WithWindow[W](windowAssigner, newTrigger, evictor, allowedLateness)
                }

                /**
                  * 设置应用于在发射之前从窗口中逐出元素的[[Evictor]]。
                  *
                  * Sets the [[Evictor]] that should be used to evict elements from a window before
                  * emission.
                  *
                  * Note: When using an evictor window performance will degrade significantly, since
                  * pre-aggregation of window results cannot be used.
                  */
                @PublicEvolving
                def evictor(
                               newEvictor: Evictor[_ >: JavaCoGroupedStreams.TaggedUnion[T1, T2], _ >: W])
                : WithWindow[W] = {
                    new WithWindow[W](windowAssigner, trigger, newEvictor, allowedLateness)
                }

                /**
                  * 设置允许元素延迟的时间代表[[WindowdStream#allowed Latency（Time）]]
                  * Sets the time by which elements are allowed to be late.
                  * Delegates to [[WindowedStream#allowedLateness(Time)]]
                  */
                @PublicEvolving
                def allowedLateness(newLateness: Time): WithWindow[W] = {
                    new WithWindow[W](windowAssigner, trigger, evictor, newLateness)
                }

                /**
                  * 使用为窗口组执行的用户功能完成协作组操作。
                  *
                  * Completes the co-group operation with the user function that is executed
                  * for windowed groups.
                  */
                def apply[O: TypeInformation](
                                                 fun: (Iterator[T1], Iterator[T2]) => O): DataStream[O] = {
                    require(fun != null, "CoGroup function must not be null.")

                    val coGrouper = new CoGroupFunction[T1, T2, O] {
                        val cleanFun = clean(fun)

                        def coGroup(
                                       left: java.lang.Iterable[T1],
                                       right: java.lang.Iterable[T2], out: Collector[O]) = {
                            out.collect(cleanFun(left.iterator().asScala, right.iterator().asScala))
                        }
                    }
                    apply(coGrouper)
                }

                /**
                  * 使用为窗口化组执行的用户功能完成协同组操作。
                  *
                  * Completes the co-group operation with the user function that is executed
                  * for windowed groups.
                  */
                def apply[O: TypeInformation](
                                                 fun: (Iterator[T1], Iterator[T2], Collector[O]) => Unit): DataStream[O] = {
                    require(fun != null, "CoGroup function must not be null.")

                    val coGrouper = new CoGroupFunction[T1, T2, O] {
                        val cleanFun = clean(fun)

                        def coGroup(
                                       left: java.lang.Iterable[T1],
                                       right: java.lang.Iterable[T2], out: Collector[O]) = {
                            cleanFun(left.iterator.asScala, right.iterator.asScala, out)
                        }
                    }
                    apply(coGrouper)
                }

                /**
                  * 使用为窗口化组执行的用户功能完成协同组操作。
                  *
                  * Completes the co-group operation with the user function that is executed
                  * for windowed groups.
                  */
                def apply[T: TypeInformation](function: CoGroupFunction[T1, T2, T]): DataStream[T] = {

                    val coGroup = new JavaCoGroupedStreams[T1, T2](input1.javaStream, input2.javaStream)

                    asScalaStream(coGroup
                        .where(keySelector1)
                        .equalTo(keySelector2)
                        .window(windowAssigner)
                        .trigger(trigger)
                        .evictor(evictor)
                        .allowedLateness(allowedLateness)
                        .apply(clean(function), implicitly[TypeInformation[T]]))
                }
            }

        }

    }

    /**
      * 返回给定函数的“闭包清理”版本。仅当org.apache.frink.api.common.ExecutionConfig中未禁用闭包清理时才进行清理。
      *
      * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
      * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]].
      */
    private[flink] def clean[F <: AnyRef](f: F): F = {
        new StreamExecutionEnvironment(input1.javaStream.getExecutionEnvironment).scalaClean(f)
    }
}
