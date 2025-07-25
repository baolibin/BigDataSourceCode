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

import org.apache.flink.annotation.{Public, PublicEvolving}
import org.apache.flink.api.common.functions.{AggregateFunction, FoldFunction, ReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{AllWindowedStream => JavaAllWStream}
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType
import org.apache.flink.streaming.api.functions.aggregation.{ComparableAggregator, SumAggregator}
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction}
import org.apache.flink.streaming.api.scala.function.util.{ScalaAllWindowFunction, ScalaAllWindowFunctionWrapper, ScalaFoldFunction, ScalaProcessAllWindowFunctionWrapper, ScalaReduceFunction}
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector
import org.apache.flink.util.Preconditions.checkNotNull

/**
  * [[AllWindowedStream]]表示一个数据流，其中元素流基于[[WindowAssigner]]. 窗口发射基于[[Trigger]]触发。
  *
  * A [[AllWindowedStream]] represents a data stream where the stream of
  * elements is split into windows based on a
  * [[org.apache.flink.streaming.api.windowing.assigners.WindowAssigner]]. Window emission
  * is triggered based on a [[Trigger]].
  *
  * If an [[Evictor]] is specified it will be
  * used to evict elements from the window after
  * evaluation was triggered by the [[Trigger]] but before the actual evaluation of the window.
  * When using an evictor window performance will degrade significantly, since
  * pre-aggregation of window results cannot be used.
  *
  * Note that the [[AllWindowedStream()]] is purely and API construct, during runtime
  * the [[AllWindowedStream()]] will be collapsed together with the
  * operation over the window into one single operation.
  *
  * @tparam T The type of elements in the stream.
  * @tparam W The type of [[Window]] that the
  *           [[org.apache.flink.streaming.api.windowing.assigners.WindowAssigner]]
  *           assigns the elements to.
  */
@Public
class AllWindowedStream[T, W <: Window](javaStream: JavaAllWStream[T, W]) {

    /**
      * 将允许的延迟设置为用户指定的值。
      *
      * Sets the allowed lateness to a user-specified value.
      * If not explicitly set, the allowed lateness is [[0L]].
      * Setting the allowed lateness is only valid for event-time windows.
      * If a value different than 0 is provided with a processing-time
      * [[org.apache.flink.streaming.api.windowing.assigners.WindowAssigner]],
      * then an exception is thrown.
      */
    @PublicEvolving
    def allowedLateness(lateness: Time): AllWindowedStream[T, W] = {
        javaStream.allowedLateness(lateness)
        this
    }

    /**
      * 将延迟到达的数据发送到由给定[[OutputTag]]标识的侧输出。
      * 水印超过窗口末尾加上使用[[allowedLateness（Time）]]设置的允许延迟时间后，数据被视为延迟。
      *
      * Send late arriving data to the side output identified by the given [[OutputTag]]. Data
      * is considered late after the watermark has passed the end of the window plus the allowed
      * lateness set using [[allowedLateness(Time)]].
      *
      * You can get the stream of late data using [[DataStream.getSideOutput()]] on the [[DataStream]]
      * resulting from the windowed operation with the same [[OutputTag]].
      */
    @PublicEvolving
    def sideOutputLateData(outputTag: OutputTag[T]): AllWindowedStream[T, W] = {
        javaStream.sideOutputLateData(outputTag)
        this
    }

    /**
      * 设置应用于触发窗口发射的[[触发器]]。
      *
      * Sets the [[Trigger]] that should be used to trigger window emission.
      */
    @PublicEvolving
    def trigger(trigger: Trigger[_ >: T, _ >: W]): AllWindowedStream[T, W] = {
        javaStream.trigger(trigger)
        this
    }

    /**
      * 设置应用于在发射前从窗口中逐出元素的[[逐出器]]。
      *
      * Sets the [[Evictor]] that should be used to evict elements from a window before emission.
      *
      * Note: When using an evictor window performance will degrade significantly, since
      * pre-aggregation of window results cannot be used.
      */
    @PublicEvolving
    def evictor(evictor: Evictor[_ >: T, _ >: W]): AllWindowedStream[T, W] = {
        javaStream.evictor(evictor)
        this
    }

    // ------------------------------------------------------------------------
    //  Operations on the windows
    // ------------------------------------------------------------------------

    // ---------------------------- reduce() ------------------------------------

    /**
      * 将reduce函数应用于窗口。对于每个键的窗口的每次评估，都会单独调用窗口函数。reduce函数的输出被解释为规则的非窗口流。
      *
      * Applies a reduce function to the window. The window function is called for each evaluation
      * of the window for each key individually. The output of the reduce function is interpreted
      * as a regular non-windowed stream.
      *
      * This window will try and pre-aggregate data as much as the window policies permit. For example,
      * tumbling time windows can perfectly pre-aggregate the data, meaning that only one element per
      * key is stored. Sliding time windows will pre-aggregate on the granularity of the slide
      * interval, so a few elements are stored per key (one per slide interval).
      * Custom windows may not be able to pre-aggregate, or may need to store extra values in an
      * aggregation tree.
      *
      * @param function The reduce function.
      * @return The data stream that is the result of applying the reduce function to the window.
      */
    def reduce(function: ReduceFunction[T]): DataStream[T] = {
        asScalaStream(javaStream.reduce(clean(function)))
    }

    /**
      * 将reduce函数应用于窗口。每个键的每个窗口求值都会单独调用窗口函数。reduce函数的输出被解释为规则的非窗口流。
      *
      * Applies a reduce function to the window. The window function is called for each evaluation
      * of the window for each key individually. The output of the reduce function is interpreted
      * as a regular non-windowed stream.
      *
      * This window will try and pre-aggregate data as much as the window policies permit. For example,
      * tumbling time windows can perfectly pre-aggregate the data, meaning that only one element per
      * key is stored. Sliding time windows will pre-aggregate on the granularity of the slide
      * interval, so a few elements are stored per key (one per slide interval).
      * Custom windows may not be able to pre-aggregate, or may need to store extra values in an
      * aggregation tree.
      *
      * @param function The reduce function.
      * @return The data stream that is the result of applying the reduce function to the window.
      */
    def reduce(function: (T, T) => T): DataStream[T] = {
        if (function == null) {
            throw new NullPointerException("Reduce function must not be null.")
        }
        val cleanFun = clean(function)
        val reducer = new ScalaReduceFunction[T](cleanFun)

        reduce(reducer)
    }

    /**
      * 将给定的窗口函数应用于每个窗口。对于每个键的窗口的每次评估，都会单独调用窗口函数。窗口函数的输出被解释为规则的非窗口流。
      *
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given pre-aggregation reducer.
      *
      * @param preAggregator  The reduce function that is used for pre-aggregation
      * @param windowFunction The window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    def reduce[R: TypeInformation](
                                      preAggregator: ReduceFunction[T],
                                      windowFunction: AllWindowFunction[T, R, W]): DataStream[R] = {

        val cleanedReducer = clean(preAggregator)
        val cleanedWindowFunction = clean(windowFunction)

        val applyFunction = new ScalaAllWindowFunctionWrapper[T, R, W](cleanedWindowFunction)

        val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
        asScalaStream(javaStream.reduce(cleanedReducer, applyFunction, returnType))
    }

    /**
      * 将给定的窗口函数应用于每个窗口。对于每个键的窗口的每次评估，都会单独调用窗口函数。窗口函数的输出被解释为规则的非窗口流。
      *
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given pre-aggregation reducer.
      *
      * @param preAggregator  The reduce function that is used for pre-aggregation
      * @param windowFunction The window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    def reduce[R: TypeInformation](
                                      preAggregator: (T, T) => T,
                                      windowFunction: (W, Iterable[T], Collector[R]) => Unit): DataStream[R] = {

        if (preAggregator == null) {
            throw new NullPointerException("Reduce function must not be null.")
        }
        if (windowFunction == null) {
            throw new NullPointerException("WindowApply function must not be null.")
        }

        val cleanReducer = clean(preAggregator)
        val cleanWindowFunction = clean(windowFunction)

        val reducer = new ScalaReduceFunction[T](cleanReducer)
        val applyFunction = new ScalaAllWindowFunction[T, R, W](cleanWindowFunction)

        val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
        asScalaStream(javaStream.reduce(reducer, applyFunction, returnType))
    }

    /**
      * 将给定的窗口函数应用于每个窗口。对于每个键的窗口的每次评估，都会单独调用窗口函数。窗口函数的输出被解释为规则的非窗口流。
      *
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given pre-aggregation reducer.
      *
      * @param preAggregator  The reduce function that is used for pre-aggregation
      * @param windowFunction The process window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    @PublicEvolving
    def reduce[R: TypeInformation](
                                      preAggregator: ReduceFunction[T],
                                      windowFunction: ProcessAllWindowFunction[T, R, W]): DataStream[R] = {

        val cleanedReducer = clean(preAggregator)
        val cleanedWindowFunction = clean(windowFunction)

        val applyFunction = new ScalaProcessAllWindowFunctionWrapper[T, R, W](cleanedWindowFunction)

        val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
        asScalaStream(javaStream.reduce(cleanedReducer, applyFunction, returnType))
    }

    /**
      * 将给定的窗口函数应用于每个窗口。对于每个键的窗口的每次评估，都会单独调用窗口函数。窗口函数的输出被解释为规则的非窗口流。
      *
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given pre-aggregation reducer.
      *
      * @param preAggregator  The reduce function that is used for pre-aggregation
      * @param windowFunction The process window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    @PublicEvolving
    def reduce[R: TypeInformation](
                                      preAggregator: (T, T) => T,
                                      windowFunction: ProcessAllWindowFunction[T, R, W]): DataStream[R] = {

        if (preAggregator == null) {
            throw new NullPointerException("Reduce function must not be null.")
        }
        if (windowFunction == null) {
            throw new NullPointerException("WindowApply function must not be null.")
        }

        val cleanReducer = clean(preAggregator)
        val cleanWindowFunction = clean(windowFunction)

        val reducer = new ScalaReduceFunction[T](cleanReducer)
        val applyFunction = new ScalaProcessAllWindowFunctionWrapper[T, R, W](cleanWindowFunction)

        val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
        asScalaStream(javaStream.reduce(reducer, applyFunction, returnType))
    }

    // --------------------------- aggregate() ----------------------------------

    /**
      * 将给定的聚合函数应用于每个窗口。为每个元素调用聚合函数，以增量方式聚合值，并将状态保持为每个窗口一个累加器。
      *
      * Applies the given aggregation function to each window. The aggregation function
      * is called for each element, aggregating values incrementally and keeping the state to
      * one accumulator per window.
      *
      * @param aggregateFunction The aggregation function.
      * @return The data stream that is the result of applying the fold function to the window.
      */
    @PublicEvolving
    def aggregate[ACC: TypeInformation, R: TypeInformation](
                                                               aggregateFunction: AggregateFunction[T, ACC, R]): DataStream[R] = {

        checkNotNull(aggregateFunction, "AggregationFunction must not be null")

        val accumulatorType: TypeInformation[ACC] = implicitly[TypeInformation[ACC]]
        val resultType: TypeInformation[R] = implicitly[TypeInformation[R]]

        asScalaStream(javaStream.aggregate(
            clean(aggregateFunction), accumulatorType, resultType))
    }

    /**
      * 将给定的窗口函数应用于每个窗口。对于每个键的窗口的每次评估，都会单独调用窗口函数。窗口函数的输出被解释为规则的非窗口流。
      *
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given aggregation function.
      *
      * @param preAggregator  The aggregation function that is used for pre-aggregation
      * @param windowFunction The window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    @PublicEvolving
    def aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation](
                                                                                   preAggregator: AggregateFunction[T, ACC, V],
                                                                                   windowFunction: AllWindowFunction[V, R, W]): DataStream[R] = {

        checkNotNull(preAggregator, "AggregationFunction must not be null")
        checkNotNull(windowFunction, "Window function must not be null")

        val cleanedPreAggregator = clean(preAggregator)
        val cleanedWindowFunction = clean(windowFunction)

        val applyFunction = new ScalaAllWindowFunctionWrapper[V, R, W](cleanedWindowFunction)

        val accumulatorType: TypeInformation[ACC] = implicitly[TypeInformation[ACC]]
        val resultType: TypeInformation[R] = implicitly[TypeInformation[R]]

        asScalaStream(javaStream.aggregate(
            cleanedPreAggregator, applyFunction, accumulatorType, resultType))
    }

    /**
      * 将给定的窗口函数应用于每个窗口。对于每个键的窗口的每次评估，都会单独调用窗口函数。窗口函数的输出被解释为规则的非窗口流。
      *
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given aggregation function.
      *
      * @param preAggregator  The aggregation function that is used for pre-aggregation
      * @param windowFunction The process window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    @PublicEvolving
    def aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation]
    (preAggregator: AggregateFunction[T, ACC, V],
     windowFunction: ProcessAllWindowFunction[V, R, W]): DataStream[R] = {

        checkNotNull(preAggregator, "AggregationFunction must not be null")
        checkNotNull(windowFunction, "Window function must not be null")

        val cleanedPreAggregator = clean(preAggregator)
        val cleanedWindowFunction = clean(windowFunction)

        val applyFunction = new ScalaProcessAllWindowFunctionWrapper[V, R, W](cleanedWindowFunction)

        val accumulatorType: TypeInformation[ACC] = implicitly[TypeInformation[ACC]]
        val aggregationResultType: TypeInformation[V] = implicitly[TypeInformation[V]]
        val resultType: TypeInformation[R] = implicitly[TypeInformation[R]]

        asScalaStream(javaStream.aggregate(
            cleanedPreAggregator, applyFunction,
            accumulatorType, aggregationResultType, resultType))
    }

    /**
      * 将给定的窗口函数应用于每个窗口。对窗口的每次评估都会调用窗口函数。窗口函数的输出被解释为规则的非窗口流。
      *
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given aggregation function.
      *
      * @param preAggregator  The aggregation function that is used for pre-aggregation
      * @param windowFunction The window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    @PublicEvolving
    def aggregate[ACC: TypeInformation, V: TypeInformation, R: TypeInformation](
                                                                                   preAggregator: AggregateFunction[T, ACC, V],
                                                                                   windowFunction: (W, Iterable[V], Collector[R]) => Unit): DataStream[R] = {

        checkNotNull(preAggregator, "AggregationFunction must not be null")
        checkNotNull(windowFunction, "Window function must not be null")

        val cleanPreAggregator = clean(preAggregator)
        val cleanWindowFunction = clean(windowFunction)

        val applyFunction = new ScalaAllWindowFunction[V, R, W](cleanWindowFunction)

        val accumulatorType: TypeInformation[ACC] = implicitly[TypeInformation[ACC]]
        val resultType: TypeInformation[R] = implicitly[TypeInformation[R]]

        asScalaStream(javaStream.aggregate(
            cleanPreAggregator, applyFunction, accumulatorType, resultType))
    }

    // ----------------------------- fold() -------------------------------------

    /**
      * 将给定的折叠功能应用于每个窗口。对于每个键的窗口的每次评估，都会单独调用窗口函数。reduce函数的输出被解释为规则的非窗口流。
      *
      * Applies the given fold function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the reduce function is
      * interpreted as a regular non-windowed stream.
      *
      * @param function The fold function.
      * @return The data stream that is the result of applying the fold function to the window.
      */
    @deprecated("use [[aggregate()]] instead")
    def fold[R: TypeInformation](
                                    initialValue: R,
                                    function: FoldFunction[T, R]): DataStream[R] = {

        if (function == null) {
            throw new NullPointerException("Fold function must not be null.")
        }

        val resultType: TypeInformation[R] = implicitly[TypeInformation[R]]
        asScalaStream(javaStream.fold(initialValue, function, resultType))
    }

    /**
      * 将给定的折叠功能应用于每个窗口。每次单独评估每个键的窗口时，都会调用窗口函数。reduce函数的输出被解释为常规的非窗口流。
      *
      * Applies the given fold function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the reduce function is
      * interpreted as a regular non-windowed stream.
      *
      * @param function The fold function.
      * @return The data stream that is the result of applying the fold function to the window.
      */
    @deprecated("use [[aggregate()]] instead")
    def fold[R: TypeInformation](initialValue: R)(function: (R, T) => R): DataStream[R] = {
        if (function == null) {
            throw new NullPointerException("Fold function must not be null.")
        }
        val cleanFun = clean(function)
        val folder = new ScalaFoldFunction[T, R](cleanFun)

        fold(initialValue, folder)
    }

    /**
      * 将给定的窗口函数应用于每个窗口。每次单独评估每个键的窗口时，都会调用窗口函数。窗口函数的输出被解释为常规的非窗口流。
      *
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given pre-aggregation folder.
      *
      * @param initialValue   Initial value of the fold
      * @param preAggregator  The reduce function that is used for pre-aggregation
      * @param windowFunction The window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    @deprecated("use [[aggregate()]] instead")
    def fold[ACC: TypeInformation, R: TypeInformation](
                                                          initialValue: ACC,
                                                          preAggregator: FoldFunction[T, ACC],
                                                          windowFunction: AllWindowFunction[ACC, R, W]): DataStream[R] = {

        val cleanFolder = clean(preAggregator)
        val cleanWindowFunction = clean(windowFunction)

        val applyFunction = new ScalaAllWindowFunctionWrapper[ACC, R, W](cleanWindowFunction)

        asScalaStream(javaStream.fold(
            initialValue,
            cleanFolder,
            applyFunction,
            implicitly[TypeInformation[ACC]],
            implicitly[TypeInformation[R]]))
    }

    /**
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given pre-aggregation folder.
      *
      * @param initialValue   Initial value of the fold
      * @param preAggregator  The reduce function that is used for pre-aggregation
      * @param windowFunction The process window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    @deprecated("use [[aggregate()]] instead")
    @PublicEvolving
    def fold[ACC: TypeInformation, R: TypeInformation](
                                                          initialValue: ACC,
                                                          preAggregator: FoldFunction[T, ACC],
                                                          windowFunction: ProcessAllWindowFunction[ACC, R, W]): DataStream[R] = {

        val cleanFolder = clean(preAggregator)
        val cleanWindowFunction = clean(windowFunction)

        val applyFunction = new ScalaProcessAllWindowFunctionWrapper[ACC, R, W](cleanWindowFunction)

        asScalaStream(javaStream.fold(
            initialValue,
            cleanFolder,
            applyFunction,
            implicitly[TypeInformation[ACC]],
            implicitly[TypeInformation[R]]))
    }

    /**
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given pre-aggregation folder.
      *
      * @param initialValue   Initial value of the fold
      * @param preAggregator  The reduce function that is used for pre-aggregation
      * @param windowFunction The window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    @deprecated("use [[aggregate()]] instead")
    def fold[ACC: TypeInformation, R: TypeInformation](
                                                          initialValue: ACC,
                                                          preAggregator: (ACC, T) => ACC,
                                                          windowFunction: (W, Iterable[ACC], Collector[R]) => Unit): DataStream[R] = {

        if (preAggregator == null) {
            throw new NullPointerException("Reduce function must not be null.")
        }
        if (windowFunction == null) {
            throw new NullPointerException("WindowApply function must not be null.")
        }

        val cleanFolder = clean(preAggregator)
        val cleanWindowFunction = clean(windowFunction)

        val folder = new ScalaFoldFunction[T, ACC](cleanFolder)
        val applyFunction = new ScalaAllWindowFunction[ACC, R, W](cleanWindowFunction)

        val accType: TypeInformation[ACC] = implicitly[TypeInformation[ACC]]
        val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
        asScalaStream(javaStream.fold(initialValue, folder, applyFunction, accType, returnType))
    }

    /**
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given pre-aggregation folder.
      *
      * @param initialValue   Initial value of the fold
      * @param preAggregator  The reduce function that is used for pre-aggregation
      * @param windowFunction The window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    @deprecated("use [[aggregate()]] instead")
    @PublicEvolving
    def fold[ACC: TypeInformation, R: TypeInformation](
                                                          initialValue: ACC,
                                                          preAggregator: (ACC, T) => ACC,
                                                          windowFunction: ProcessAllWindowFunction[ACC, R, W]): DataStream[R] = {

        if (preAggregator == null) {
            throw new NullPointerException("Reduce function must not be null.")
        }
        if (windowFunction == null) {
            throw new NullPointerException("WindowApply function must not be null.")
        }

        val cleanFolder = clean(preAggregator)
        val cleanWindowFunction = clean(windowFunction)

        val folder = new ScalaFoldFunction[T, ACC](cleanFolder)
        val applyFunction = new ScalaProcessAllWindowFunctionWrapper[ACC, R, W](cleanWindowFunction)

        val accType: TypeInformation[ACC] = implicitly[TypeInformation[ACC]]
        val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
        asScalaStream(javaStream.fold(initialValue, folder, applyFunction, accType, returnType))
    }

    // ---------------------------- apply() -------------------------------------

    /**
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Not that this function requires that all data in the windows is buffered until the window
      * is evaluated, as the function provides no means of pre-aggregation.
      *
      * @param function The process window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    @PublicEvolving
    def process[R: TypeInformation](
                                       function: ProcessAllWindowFunction[T, R, W]): DataStream[R] = {

        val cleanedFunction = clean(function)
        val javaFunction = new ScalaProcessAllWindowFunctionWrapper[T, R, W](cleanedFunction)

        asScalaStream(javaStream.process(javaFunction, implicitly[TypeInformation[R]]))
    }

    /**
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Not that this function requires that all data in the windows is buffered until the window
      * is evaluated, as the function provides no means of pre-aggregation.
      *
      * @param function The window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    def apply[R: TypeInformation](
                                     function: AllWindowFunction[T, R, W]): DataStream[R] = {

        val cleanedFunction = clean(function)
        val javaFunction = new ScalaAllWindowFunctionWrapper[T, R, W](cleanedFunction)

        asScalaStream(javaStream.apply(javaFunction, implicitly[TypeInformation[R]]))
    }

    /**
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Not that this function requires that all data in the windows is buffered until the window
      * is evaluated, as the function provides no means of pre-aggregation.
      *
      * @param function The window function.
      * @return The data stream that is the result of applying the window function to the window.
      */
    def apply[R: TypeInformation](
                                     function: (W, Iterable[T], Collector[R]) => Unit): DataStream[R] = {

        val cleanedFunction = clean(function)
        val applyFunction = new ScalaAllWindowFunction[T, R, W](cleanedFunction)

        asScalaStream(javaStream.apply(applyFunction, implicitly[TypeInformation[R]]))
    }

    /**
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given pre-aggregation reducer.
      *
      * @param preAggregator  The reduce function that is used for pre-aggregation
      * @param windowFunction The window function.
      * @return The data stream that is the result of applying the window function to the window.
      * @deprecated Use [[reduce(ReduceFunction, AllWindowFunction)]] instead.
      */
    @deprecated
    def apply[R: TypeInformation](
                                     preAggregator: ReduceFunction[T],
                                     windowFunction: AllWindowFunction[T, R, W]): DataStream[R] = {

        val cleanedReducer = clean(preAggregator)
        val cleanedWindowFunction = clean(windowFunction)

        val applyFunction = new ScalaAllWindowFunctionWrapper[T, R, W](cleanedWindowFunction)

        val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
        asScalaStream(javaStream.apply(cleanedReducer, applyFunction, returnType))
    }

    /**
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given pre-aggregation reducer.
      *
      * @param preAggregator  The reduce function that is used for pre-aggregation
      * @param windowFunction The window function.
      * @return The data stream that is the result of applying the window function to the window.
      * @deprecated Use [[reduce(ReduceFunction, AllWindowFunction)]] instead.
      */
    @deprecated
    def apply[R: TypeInformation](
                                     preAggregator: (T, T) => T,
                                     windowFunction: (W, Iterable[T], Collector[R]) => Unit): DataStream[R] = {

        if (preAggregator == null) {
            throw new NullPointerException("Reduce function must not be null.")
        }
        if (windowFunction == null) {
            throw new NullPointerException("WindowApply function must not be null.")
        }

        val cleanReducer = clean(preAggregator)
        val cleanWindowFunction = clean(windowFunction)

        val reducer = new ScalaReduceFunction[T](cleanReducer)
        val applyFunction = new ScalaAllWindowFunction[T, R, W](cleanWindowFunction)

        val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
        asScalaStream(javaStream.apply(reducer, applyFunction, returnType))
    }

    /**
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given pre-aggregation folder.
      *
      * @param initialValue   Initial value of the fold
      * @param preAggregator  The reduce function that is used for pre-aggregation
      * @param windowFunction The window function.
      * @return The data stream that is the result of applying the window function to the window.
      * @deprecated Use [[fold(R, FoldFunction, AllWindowFunction)]] instead.
      */
    @deprecated
    def apply[R: TypeInformation](
                                     initialValue: R,
                                     preAggregator: FoldFunction[T, R],
                                     windowFunction: AllWindowFunction[R, R, W]): DataStream[R] = {

        val cleanFolder = clean(preAggregator)
        val cleanWindowFunction = clean(windowFunction)

        val applyFunction = new ScalaAllWindowFunctionWrapper[R, R, W](cleanWindowFunction)

        asScalaStream(javaStream.apply(
            initialValue,
            cleanFolder,
            applyFunction,
            implicitly[TypeInformation[R]]))
    }

    /**
      * Applies the given window function to each window. The window function is called for each
      * evaluation of the window for each key individually. The output of the window function is
      * interpreted as a regular non-windowed stream.
      *
      * Arriving data is pre-aggregated using the given pre-aggregation folder.
      *
      * @param initialValue   Initial value of the fold
      * @param preAggregator  The reduce function that is used for pre-aggregation
      * @param windowFunction The window function.
      * @return The data stream that is the result of applying the window function to the window.
      * @deprecated Use [[fold(R, FoldFunction, AllWindowFunction]] instead.
      */
    @deprecated
    def apply[R: TypeInformation](
                                     initialValue: R,
                                     preAggregator: (R, T) => R,
                                     windowFunction: (W, Iterable[R], Collector[R]) => Unit): DataStream[R] = {

        if (preAggregator == null) {
            throw new NullPointerException("Reduce function must not be null.")
        }
        if (windowFunction == null) {
            throw new NullPointerException("WindowApply function must not be null.")
        }

        val cleanFolder = clean(preAggregator)
        val cleanWindowFunction = clean(windowFunction)

        val folder = new ScalaFoldFunction[T, R](cleanFolder)
        val applyFunction = new ScalaAllWindowFunction[R, R, W](cleanWindowFunction)

        val returnType: TypeInformation[R] = implicitly[TypeInformation[R]]
        asScalaStream(javaStream.apply(initialValue, folder, applyFunction, returnType))
    }

    // ------------------------------------------------------------------------
    //  Aggregations on the keyed windows
    // ------------------------------------------------------------------------

    /**
      * Applies an aggregation that that gives the maximum of the elements in the window at
      * the given position.
      */
    def max(position: Int): DataStream[T] = aggregate(AggregationType.MAX, position)

    /**
      * Applies an aggregation that that gives the maximum of the elements in the window at
      * the given field.
      */
    def max(field: String): DataStream[T] = aggregate(AggregationType.MAX, field)

    /**
      * Applies an aggregation that that gives the minimum of the elements in the window at
      * the given position.
      */
    def min(position: Int): DataStream[T] = aggregate(AggregationType.MIN, position)

    /**
      * Applies an aggregation that that gives the minimum of the elements in the window at
      * the given field.
      */
    def min(field: String): DataStream[T] = aggregate(AggregationType.MIN, field)

    /**
      * Applies an aggregation that sums the elements in the window at the given position.
      */
    def sum(position: Int): DataStream[T] = aggregate(AggregationType.SUM, position)

    /**
      * Applies an aggregation that sums the elements in the window at the given field.
      */
    def sum(field: String): DataStream[T] = aggregate(AggregationType.SUM, field)

    /**
      * Applies an aggregation that that gives the maximum element of the window by
      * the given position. When equality, returns the first.
      */
    def maxBy(position: Int): DataStream[T] = aggregate(AggregationType.MAXBY,
        position)

    /**
      * Applies an aggregation that that gives the maximum element of the window by
      * the given field. When equality, returns the first.
      */
    def maxBy(field: String): DataStream[T] = aggregate(AggregationType.MAXBY,
        field)

    /**
      * Applies an aggregation that that gives the minimum element of the window by
      * the given position. When equality, returns the first.
      */
    def minBy(position: Int): DataStream[T] = aggregate(AggregationType.MINBY,
        position)

    /**
      * Applies an aggregation that that gives the minimum element of the window by
      * the given field. When equality, returns the first.
      */
    def minBy(field: String): DataStream[T] = aggregate(AggregationType.MINBY,
        field)

    private def aggregate(aggregationType: AggregationType, field: String): DataStream[T] = {
        val position = fieldNames2Indices(getInputType(), Array(field))(0)
        aggregate(aggregationType, position)
    }

    def aggregate(aggregationType: AggregationType, position: Int): DataStream[T] = {

        val jStream = javaStream.asInstanceOf[JavaAllWStream[Product, W]]

        val reducer = aggregationType match {
            case AggregationType.SUM =>
                new SumAggregator(position, jStream.getInputType, jStream.getExecutionEnvironment.getConfig)

            case _ =>
                new ComparableAggregator(
                    position,
                    jStream.getInputType,
                    aggregationType,
                    true,
                    jStream.getExecutionEnvironment.getConfig)
        }

        new DataStream[Product](jStream.reduce(reducer)).asInstanceOf[DataStream[T]]
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
      * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
      * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]].
      */
    private[flink] def clean[F <: AnyRef](f: F): F = {
        new StreamExecutionEnvironment(javaStream.getExecutionEnvironment).scalaClean(f)
    }

    /**
      * Gets the output type.
      */
    private def getInputType(): TypeInformation[T] = javaStream.getInputType
}
