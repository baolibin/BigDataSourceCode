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

import org.apache.flink.annotation.{Internal, Public, PublicEvolving}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction, Partitioner}
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.operators.ResourceSpec
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.{Tuple => JavaTuple}
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.scala.operators.ScalaCsvOutputFormat
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.datastream.{AllWindowedStream => JavaAllWindowedStream, DataStream => JavaStream, KeyedStream => JavaKeyedStream, _}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, ProcessFunction, TimestampExtractor}
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

@Public
class DataStream[T](stream: JavaStream[T]) {

    /**
      * 返回与当前[[DataStream]]关联的[[StreamExecutionVironment]]。
      *
      * Returns the [[StreamExecutionEnvironment]] associated with the current [[DataStream]].
      *
      * @return associated execution environment
      * @deprecated Use [[executionEnvironment]] instead
      */
    @deprecated
    @PublicEvolving
    def getExecutionEnvironment: StreamExecutionEnvironment =
        new StreamExecutionEnvironment(stream.getExecutionEnvironment)

    /**
      * 返回此数据流元素的类型信息。
      *
      * Returns the TypeInformation for the elements of this DataStream.
      *
      * @deprecated Use [[dataType]] instead.
      */
    @deprecated
    @PublicEvolving
    def getType(): TypeInformation[T] = stream.getType()

    /**
      * 返回此操作的并行性。
      *
      * Returns the parallelism of this operation.
      *
      * @deprecated Use [[parallelism]] instead.
      */
    @deprecated
    @PublicEvolving
    def getParallelism = stream.getParallelism

    /**
      * 返回执行配置。
      *
      * Returns the execution config.
      *
      * @deprecated Use [[executionConfig]] instead.
      */
    @deprecated
    @PublicEvolving
    def getExecutionConfig = stream.getExecutionConfig

    /**
      * 返回数据流的ID。
      *
      * Returns the ID of the DataStream.
      */
    @Internal
    private[flink] def getId = stream.getId()

    // --------------------------------------------------------------------------
    //  Scalaesk accessors
    // --------------------------------------------------------------------------

    /**
      * 获取基础java数据流对象。
      *
      * Gets the underlying java DataStream object.
      */
    def javaStream: JavaStream[T] = stream

    /**
      * 返回此数据流元素的类型信息。
      *
      * Returns the TypeInformation for the elements of this DataStream.
      */
    def dataType: TypeInformation[T] = stream.getType()

    /**
      * 返回执行配置。
      *
      * Returns the execution config.
      */
    def executionConfig: ExecutionConfig = stream.getExecutionConfig()

    /**
      * 返回与此数据流关联的[[StreamExecutionEnvironment]]
      *
      * Returns the [[StreamExecutionEnvironment]] associated with this data stream
      */
    def executionEnvironment: StreamExecutionEnvironment =
        new StreamExecutionEnvironment(stream.getExecutionEnvironment())


    /**
      * 返回此操作的并行度。
      *
      * Returns the parallelism of this operation.
      */
    def parallelism: Int = stream.getParallelism()

    /**
      * 设置此操作的并行度。该值必须至少为1。
      *
      * Sets the parallelism of this operation. This must be at least 1.
      */
    def setParallelism(parallelism: Int): DataStream[T] = {
        stream match {
            case ds: SingleOutputStreamOperator[T] => ds.setParallelism(parallelism)
            case _ =>
                throw new UnsupportedOperationException(
                    "Operator " + stream + " cannot set the parallelism.")
        }
        this
    }

    def setMaxParallelism(maxParallelism: Int): DataStream[T] = {
        stream match {
            case ds: SingleOutputStreamOperator[T] => ds.setMaxParallelism(maxParallelism)
            case _ =>
                throw new UnsupportedOperationException("Operator " + stream + " cannot set the maximum" +
                    "paralllelism")
        }

        this
    }

    /**
      * 返回此操作的最小资源。
      *
      * Returns the minimum resources of this operation.
      */
    @PublicEvolving
    def minResources: ResourceSpec = stream.getMinResources()

    /**
      * 返回此操作的首选资源。
      *
      * Returns the preferred resources of this operation.
      */
    @PublicEvolving
    def preferredResources: ResourceSpec = stream.getPreferredResources()

    // ---------------------------------------------------------------------------
    //  Fine-grained resource profiles are an incomplete work-in-progress feature
    //  The setters are hence commented out at this point.
    // ---------------------------------------------------------------------------
    //  /**
    //   * Sets the minimum and preferred resources of this operation.
    //   */
    //  @PublicEvolving
    //  def resources(minResources: ResourceSpec, preferredResources: ResourceSpec) : DataStream[T] =
    //    stream match {
    //      case stream : SingleOutputStreamOperator[T] => asScalaStream(
    //        stream.setResources(minResources, preferredResources))
    //      case _ =>
    //        throw new UnsupportedOperationException("Operator does not support " +
    //          "configuring custom resources specs.")
    //      this
    //  }
    //
    //  /**
    //   * Sets the resource of this operation.
    //   */
    //  @PublicEvolving
    //  def resources(resources: ResourceSpec) : Unit = {
    //    this.resources(resources, resources)
    //  }

    /**
      * 获取当前数据流的名称。此名称在运行时由可视化和日志记录使用。
      *
      * Gets the name of the current data stream. This name is
      * used by the visualization and logging during runtime.
      *
      * @return Name of the stream.
      */
    def name: String = stream match {
        case stream: SingleOutputStreamOperator[T] => stream.getName
        case _ => throw new
                UnsupportedOperationException("Only supported for operators.")
    }

    // --------------------------------------------------------------------------

    /**
      * 获取当前数据流的名称。此名称在运行时由可视化和日志记录使用。
      *
      * Gets the name of the current data stream. This name is
      * used by the visualization and logging during runtime.
      *
      * @return Name of the stream.
      * @deprecated Use [[name]] instead
      */
    @deprecated
    @PublicEvolving
    def getName: String = name

    /**
      * 设置当前数据流的名称。此名称在运行时由可视化和日志记录使用。
      *
      * Sets the name of the current data stream. This name is
      * used by the visualization and logging during runtime.
      *
      * @return The named operator
      */
    def name(name: String): DataStream[T] = stream match {
        case stream: SingleOutputStreamOperator[T] => asScalaStream(stream.name(name))
        case _ => throw new UnsupportedOperationException("Only supported for operators.")
            this
    }

    /**
      * 设置此运算符的ID。
      *
      * Sets an ID for this operator.
      *
      * The specified ID is used to assign the same operator ID across job
      * submissions (for example when starting a job from a savepoint).
      *
      * <strong>Important</strong>: this ID needs to be unique per
      * transformation and job. Otherwise, job submission will fail.
      *
      * @param uid The unique user-specified ID of this transformation.
      * @return The operator with the specified ID.
      */
    @PublicEvolving
    def uid(uid: String): DataStream[T] = javaStream match {
        case stream: SingleOutputStreamOperator[T] => asScalaStream(stream.uid(uid))
        case _ => throw new UnsupportedOperationException("Only supported for operators.")
            this
    }

    @PublicEvolving
    def getSideOutput[X: TypeInformation](tag: OutputTag[X]): DataStream[X] = javaStream match {
        case stream: SingleOutputStreamOperator[X] =>
            asScalaStream(stream.getSideOutput(tag: OutputTag[X]))
    }

    /**
      * 设置用户为此运算符提供的哈希。这将与创建JobVertexID一样使用。
      *
      * Sets an user provided hash for this operator. This will be used AS IS the create
      * the JobVertexID.
      * <p/>
      * <p>The user provided hash is an alternative to the generated hashes, that is
      * considered when identifying an operator through the default hash mechanics fails
      * (e.g. because of changes between Flink versions).
      * <p/>
      * <p><strong>Important</strong>: this should be used as a workaround or for trouble
      * shooting. The provided hash needs to be unique per transformation and job. Otherwise,
      * job submission will fail. Furthermore, you cannot assign user-specified hash to
      * intermediate nodes in an operator chain and trying so will let your job fail.
      *
      * @param hash the user provided hash for this operator.
      * @return The operator with the user provided hash.
      */
    @PublicEvolving
    def setUidHash(hash: String): DataStream[T] = javaStream match {
        case stream: SingleOutputStreamOperator[T] =>
            asScalaStream(stream.setUidHash(hash))
        case _ => throw new UnsupportedOperationException("Only supported for operators.")
            this
    }

    /**
      * Turns off chaining for this operator so thread co-location will not be
      * used as an optimization. </p> Chaining can be turned off for the whole
      * job by [[StreamExecutionEnvironment.disableOperatorChaining()]]
      * however it is not advised for performance considerations.
      *
      */
    @PublicEvolving
    def disableChaining(): DataStream[T] = {
        stream match {
            case ds: SingleOutputStreamOperator[T] => ds.disableChaining()
            case _ =>
                throw new UnsupportedOperationException("Only supported for operators.")
        }
        this
    }

    /**
      * 启动从此操作员开始的新任务链。即使可能，此操作符也不会链接到任何以前的任务（线程位于同一位置以提高性能）。
      *
      * Starts a new task chain beginning at this operator. This operator will
      * not be chained (thread co-located for increased performance) to any
      * previous tasks even if possible.
      *
      */
    @PublicEvolving
    def startNewChain(): DataStream[T] = {
        stream match {
            case ds: SingleOutputStreamOperator[T] => ds.startNewChain()
            case _ =>
                throw new UnsupportedOperationException("Only supported for operators.")
        }
        this
    }

    /**
      * 设置此操作的插槽共享组。如果可能，同一插槽共享组中的并行操作实例将位于同一TaskManager插槽中。
      *
      * Sets the slot sharing group of this operation. Parallel instances of
      * operations that are in the same slot sharing group will be co-located in the same
      * TaskManager slot, if possible.
      *
      * Operations inherit the slot sharing group of input operations if all input operations
      * are in the same slot sharing group and no slot sharing group was explicitly specified.
      *
      * Initially an operation is in the default slot sharing group. An operation can be put into
      * the default group explicitly by setting the slot sharing group to `"default"`.
      *
      * @param slotSharingGroup The slot sharing group name.
      */
    @PublicEvolving
    def slotSharingGroup(slotSharingGroup: String): DataStream[T] = {
        stream match {
            case ds: SingleOutputStreamOperator[T] => ds.slotSharingGroup(slotSharingGroup)
            case _ =>
                throw new UnsupportedOperationException("Only supported for operators.")
        }
        this
    }

    /**
      * 设置输出缓冲区刷新的最大时间频率（ms）。默认情况下，输出缓冲区仅在满时刷新。
      *
      * Sets the maximum time frequency (ms) for the flushing of the output
      * buffer. By default the output buffers flush only when they are full.
      *
      * @param timeoutMillis
      * The maximum time between two output flushes.
      * @return The operator with buffer timeout set.
      */
    def setBufferTimeout(timeoutMillis: Long): DataStream[T] = {
        stream match {
            case ds: SingleOutputStreamOperator[T] => ds.setBufferTimeout(timeoutMillis)
            case _ =>
                throw new UnsupportedOperationException("Only supported for operators.")
        }
        this
    }

    // --------------------------------------------------------------------------
    //  Stream Transformations
    // --------------------------------------------------------------------------

    /**
      * 通过将相同类型的数据流输出彼此合并来创建新的数据流。使用此运算符合并的数据流将同时进行转换。
      *
      * Creates a new DataStream by merging DataStream outputs of
      * the same type with each other. The DataStreams merged using this operator
      * will be transformed simultaneously.
      *
      */
    def union(dataStreams: DataStream[T]*): DataStream[T] =
        asScalaStream(stream.union(dataStreams.map(_.javaStream): _*))

    /**
      * Creates a new ConnectedStreams by connecting
      * DataStream outputs of different type with each other. The
      * DataStreams connected using this operators can be used with CoFunctions.
      */
    def connect[T2](dataStream: DataStream[T2]): ConnectedStreams[T, T2] =
        asScalaStream(stream.connect(dataStream.javaStream))

    /**
      * Creates a new [[BroadcastConnectedStream]] by connecting the current
      * [[DataStream]] or [[KeyedStream]] with a [[BroadcastStream]].
      *
      * The latter can be created using the [[broadcast(MapStateDescriptor[])]] method.
      *
      * The resulting stream can be further processed using the
      * ``broadcastConnectedStream.process(myFunction)``
      * method, where ``myFunction`` can be either a
      * [[org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction]]
      * or a [[org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction]]
      * depending on the current stream being a [[KeyedStream]] or not.
      *
      * @param broadcastStream The broadcast stream with the broadcast state to be
      *                        connected with this stream.
      * @return The [[BroadcastConnectedStream]].
      */
    @PublicEvolving
    def connect[R](broadcastStream: BroadcastStream[R]): BroadcastConnectedStream[T, R] =
        asScalaStream(stream.connect(broadcastStream))

    /**
      * 按给定的键位置（对于元组/数组类型）对数据流的元素进行分组，以便与分组运算符（如分组reduce或分组聚合）一起使用。
      *
      * Groups the elements of a DataStream by the given key positions (for tuple/array types) to
      * be used with grouped operators like grouped reduce or grouped aggregations.
      */
    def keyBy(fields: Int*): KeyedStream[T, JavaTuple] = asScalaStream(stream.keyBy(fields: _*))

    /**
      * 按给定的字段表达式对数据流的元素进行分组，以便与分组运算符（如分组reduce或分组聚合）一起使用。
      *
      * Groups the elements of a DataStream by the given field expressions to
      * be used with grouped operators like grouped reduce or grouped aggregations.
      */
    def keyBy(firstField: String, otherFields: String*): KeyedStream[T, JavaTuple] =
        asScalaStream(stream.keyBy(firstField +: otherFields.toArray: _*))

    /**
      * Groups the elements of a DataStream by the given K key to
      * be used with grouped operators like grouped reduce or grouped aggregations.
      */
    def keyBy[K: TypeInformation](fun: T => K): KeyedStream[T, K] = {

        val cleanFun = clean(fun)
        val keyType: TypeInformation[K] = implicitly[TypeInformation[K]]

        val keyExtractor = new KeySelector[T, K] with ResultTypeQueryable[K] {
            def getKey(in: T) = cleanFun(in)

            override def getProducedType: TypeInformation[K] = keyType
        }
        asScalaStream(new JavaKeyedStream(stream, keyExtractor, keyType))
    }

    /**
      * Groups the elements of a DataStream by the given K key to
      * be used with grouped operators like grouped reduce or grouped aggregations.
      */
    def keyBy[K: TypeInformation](fun: KeySelector[T, K]): KeyedStream[T, K] = {

        val cleanFun = clean(fun)
        val keyType: TypeInformation[K] = implicitly[TypeInformation[K]]

        asScalaStream(new JavaKeyedStream(stream, cleanFun, keyType))
    }

    /**
      * 使用自定义分区器在指定的键字段上对元组数据流进行分区。此方法采用要分区的键位置，以及接受键类型的分区器。
      *
      * Partitions a tuple DataStream on the specified key fields using a custom partitioner.
      * This method takes the key position to partition on, and a partitioner that accepts the key
      * type.
      *
      * Note: This method works only on single field keys.
      */
    def partitionCustom[K: TypeInformation](partitioner: Partitioner[K], field: Int): DataStream[T] =
        asScalaStream(stream.partitionCustom(partitioner, field))

    /**
      * Partitions a POJO DataStream on the specified key fields using a custom partitioner.
      * This method takes the key expression to partition on, and a partitioner that accepts the key
      * type.
      *
      * Note: This method works only on single field keys.
      */
    def partitionCustom[K: TypeInformation](partitioner: Partitioner[K], field: String)
    : DataStream[T] =
        asScalaStream(stream.partitionCustom(partitioner, field))

    /**
      * Partitions a DataStream on the key returned by the selector, using a custom partitioner.
      * This method takes the key selector to get the key to partition on, and a partitioner that
      * accepts the key type.
      *
      * Note: This method works only on single field keys, i.e. the selector cannot return tuples
      * of fields.
      */
    def partitionCustom[K: TypeInformation](partitioner: Partitioner[K], fun: T => K)
    : DataStream[T] = {

        val keyType = implicitly[TypeInformation[K]]
        val cleanFun = clean(fun)

        val keyExtractor = new KeySelector[T, K] with ResultTypeQueryable[K] {
            def getKey(in: T) = cleanFun(in)

            override def getProducedType(): TypeInformation[K] = keyType
        }

        asScalaStream(stream.partitionCustom(partitioner, keyExtractor))
    }

    /**
      * 设置数据流的分区，以便将输出元组广泛地投射到下一个组件的每个并行实例。
      *
      * Sets the partitioning of the DataStream so that the output tuples
      * are broad casted to every parallel instance of the next component.
      */
    def broadcast: DataStream[T] = asScalaStream(stream.broadcast())

    /**
      * Sets the partitioning of the [[DataStream]] so that the output elements
      * are broadcasted to every parallel instance of the next operation. In addition,
      * it implicitly creates as many
      * [[org.apache.flink.api.common.state.BroadcastState broadcast states]]
      * as the specified descriptors which can be used to store the element of the stream.
      *
      * @param broadcastStateDescriptors the descriptors of the broadcast states to create.
      * @return A [[BroadcastStream]] which can be used in the
      *         [[DataStream.connect(BroadcastStream)]] to create a
      *         [[BroadcastConnectedStream]] for further processing of the elements.
      */
    @PublicEvolving
    def broadcast(broadcastStateDescriptors: MapStateDescriptor[_, _]*): BroadcastStream[T] = {
        if (broadcastStateDescriptors == null) {
            throw new NullPointerException("State Descriptors must not be null.")
        }
        javaStream.broadcast(broadcastStateDescriptors: _*)
    }

    /**
      * Sets the partitioning of the DataStream so that the output values all go to
      * the first instance of the next processing operator. Use this setting with care
      * since it might cause a serious performance bottleneck in the application.
      */
    @PublicEvolving
    def global: DataStream[T] = asScalaStream(stream.global())

    /**
      * 设置数据流的分区，以便将输出元组无序排列到下一个组件。
      *
      * Sets the partitioning of the DataStream so that the output tuples
      * are shuffled to the next component.
      */
    @PublicEvolving
    def shuffle: DataStream[T] = asScalaStream(stream.shuffle())

    /**
      * 设置数据流的分区，以便将输出元组转发到下一个组件的本地子任务（只要可能）。
      *
      * Sets the partitioning of the DataStream so that the output tuples
      * are forwarded to the local subtask of the next component (whenever
      * possible).
      */
    def forward: DataStream[T] = asScalaStream(stream.forward())

    /**
      * 设置数据流的分区，以便将输出元组均匀分布到下一个组件。
      *
      * Sets the partitioning of the DataStream so that the output tuples
      * are distributed evenly to the next component.
      */
    def rebalance: DataStream[T] = asScalaStream(stream.rebalance())

    /**
      * Sets the partitioning of the [[DataStream]] so that the output tuples
      * are distributed evenly to a subset of instances of the downstream operation.
      *
      * The subset of downstream operations to which the upstream operation sends
      * elements depends on the degree of parallelism of both the upstream and downstream operation.
      * For example, if the upstream operation has parallelism 2 and the downstream operation
      * has parallelism 4, then one upstream operation would distribute elements to two
      * downstream operations while the other upstream operation would distribute to the other
      * two downstream operations. If, on the other hand, the downstream operation has parallelism
      * 2 while the upstream operation has parallelism 4 then two upstream operations will
      * distribute to one downstream operation while the other two upstream operations will
      * distribute to the other downstream operations.
      *
      * In cases where the different parallelisms are not multiples of each other one or several
      * downstream operations will have a differing number of inputs from upstream operations.
      */
    @PublicEvolving
    def rescale: DataStream[T] = asScalaStream(stream.rescale())

    /**
      * Initiates an iterative part of the program that creates a loop by feeding
      * back data streams. To create a streaming iteration the user needs to define
      * a transformation that creates two DataStreams. The first one is the output
      * that will be fed back to the start of the iteration and the second is the output
      * stream of the iterative part.
      *
      * stepfunction: initialStream => (feedback, output)
      *
      * A common pattern is to use output splitting to create feedback and output DataStream.
      * Please refer to the [[split]] method of the DataStream
      *
      * By default a DataStream with iteration will never terminate, but the user
      * can use the maxWaitTime parameter to set a max waiting time for the iteration head.
      * If no data received in the set time the stream terminates.
      *
      * Parallelism of the feedback stream must match the parallelism of the original stream.
      * Please refer to the [[setParallelism]] method for parallelism modification
      */
    @PublicEvolving
    def iterate[R](stepFunction: DataStream[T] => (DataStream[T], DataStream[R]),
                   maxWaitTimeMillis: Long = 0): DataStream[R] = {
        val iterativeStream = stream.iterate(maxWaitTimeMillis)

        val (feedback, output) = stepFunction(new DataStream[T](iterativeStream))

        iterativeStream.closeWith(feedback.javaStream)
        output
    }

    /**
      * Initiates an iterative part of the program that creates a loop by feeding
      * back data streams. To create a streaming iteration the user needs to define
      * a transformation that creates two DataStreams. The first one is the output
      * that will be fed back to the start of the iteration and the second is the output
      * stream of the iterative part.
      *
      * The input stream of the iterate operator and the feedback stream will be treated
      * as a ConnectedStreams where the input is connected with the feedback stream.
      *
      * This allows the user to distinguish standard input from feedback inputs.
      *
      * stepfunction: initialStream => (feedback, output)
      *
      * The user must set the max waiting time for the iteration head.
      * If no data received in the set time the stream terminates. If this parameter is set
      * to 0 then the iteration sources will indefinitely, so the job must be killed to stop.
      *
      */
    @PublicEvolving
    def iterate[R, F: TypeInformation](
                                          stepFunction: ConnectedStreams[T, F] => (DataStream[F], DataStream[R]),
                                          maxWaitTimeMillis: Long): DataStream[R] = {

        val feedbackType: TypeInformation[F] = implicitly[TypeInformation[F]]

        val connectedIterativeStream = stream.iterate(maxWaitTimeMillis).
            withFeedbackType(feedbackType)

        val (feedback, output) = stepFunction(asScalaStream(connectedIterativeStream))
        connectedIterativeStream.closeWith(feedback.javaStream)
        output
    }

    /**
      * 通过将给定函数应用于此数据流的每个元素来创建新的数据流。
      *
      * Creates a new DataStream by applying the given function to every element of this DataStream.
      */
    def map[R: TypeInformation](fun: T => R): DataStream[R] = {
        if (fun == null) {
            throw new NullPointerException("Map function must not be null.")
        }
        val cleanFun = clean(fun)
        val mapper = new MapFunction[T, R] {
            def map(in: T): R = cleanFun(in)
        }

        map(mapper)
    }

    /**
      * 通过将给定函数应用于此数据流的每个元素来创建新的数据流。
      *
      * Creates a new DataStream by applying the given function to every element of this DataStream.
      */
    def map[R: TypeInformation](mapper: MapFunction[T, R]): DataStream[R] = {
        if (mapper == null) {
            throw new NullPointerException("Map function must not be null.")
        }

        val outType: TypeInformation[R] = implicitly[TypeInformation[R]]
        asScalaStream(stream.map(mapper).returns(outType).asInstanceOf[JavaStream[R]])
    }

    /**
      * 通过将给定函数应用于每个元素并展平结果来创建新的数据流。
      *
      * Creates a new DataStream by applying the given function to every element and flattening
      * the results.
      */
    def flatMap[R: TypeInformation](flatMapper: FlatMapFunction[T, R]): DataStream[R] = {
        if (flatMapper == null) {
            throw new NullPointerException("FlatMap function must not be null.")
        }

        val outType: TypeInformation[R] = implicitly[TypeInformation[R]]
        asScalaStream(stream.flatMap(flatMapper).returns(outType).asInstanceOf[JavaStream[R]])
    }

    /**
      * Creates a new DataStream by applying the given function to every element and flattening
      * the results.
      */
    def flatMap[R: TypeInformation](fun: (T, Collector[R]) => Unit): DataStream[R] = {
        if (fun == null) {
            throw new NullPointerException("FlatMap function must not be null.")
        }
        val cleanFun = clean(fun)
        val flatMapper = new FlatMapFunction[T, R] {
            def flatMap(in: T, out: Collector[R]) {
                cleanFun(in, out)
            }
        }
        flatMap(flatMapper)
    }

    /**
      * 通过将给定函数应用于每个元素并展平结果来创建新的数据流。
      *
      * Creates a new DataStream by applying the given function to every element and flattening
      * the results.
      */
    def flatMap[R: TypeInformation](fun: T => TraversableOnce[R]): DataStream[R] = {
        if (fun == null) {
            throw new NullPointerException("FlatMap function must not be null.")
        }
        val cleanFun = clean(fun)
        val flatMapper = new FlatMapFunction[T, R] {
            def flatMap(in: T, out: Collector[R]) {
                cleanFun(in) foreach out.collect
            }
        }
        flatMap(flatMapper)
    }

    /**
      * Applies the given [[ProcessFunction]] on the input stream, thereby
      * creating a transformed output stream.
      *
      * The function will be called for every element in the stream and can produce
      * zero or more output.
      *
      * @param processFunction The [[ProcessFunction]] that is called for each element
      *                        in the stream.
      */
    @PublicEvolving
    def process[R: TypeInformation](
                                       processFunction: ProcessFunction[T, R]): DataStream[R] = {

        if (processFunction == null) {
            throw new NullPointerException("ProcessFunction must not be null.")
        }

        asScalaStream(javaStream.process(processFunction, implicitly[TypeInformation[R]]))
    }


    /**
      * 创建仅包含满足给定筛选器谓词的元素的新数据流。
      *
      * Creates a new DataStream that contains only the elements satisfying the given filter predicate.
      */
    def filter(filter: FilterFunction[T]): DataStream[T] = {
        if (filter == null) {
            throw new NullPointerException("Filter function must not be null.")
        }
        asScalaStream(stream.filter(filter))
    }

    /**
      * 创建仅包含满足给定筛选器谓词的元素的新数据流。
      *
      * Creates a new DataStream that contains only the elements satisfying the given filter predicate.
      */
    def filter(fun: T => Boolean): DataStream[T] = {
        if (fun == null) {
            throw new NullPointerException("Filter function must not be null.")
        }
        val cleanFun = clean(fun)
        val filterFun = new FilterFunction[T] {
            def filter(in: T) = cleanFun(in)
        }
        filter(filterFun)
    }

    /**
      * 将此数据流设置为滚动时间窗口。
      *
      * Windows this DataStream into tumbling time windows.
      *
      * This is a shortcut for either `.window(TumblingEventTimeWindows.of(size))` or
      * `.window(TumblingProcessingTimeWindows.of(size))` depending on the time characteristic
      * set using
      * [[StreamExecutionEnvironment.setStreamTimeCharacteristic]].
      *
      * Note: This operation can be inherently non-parallel since all elements have to pass through
      * the same operator instance. (Only for special cases, such as aligned time windows is
      * it possible to perform this operation in parallel).
      *
      * @param size The size of the window.
      */
    def timeWindowAll(size: Time): AllWindowedStream[T, TimeWindow] = {
        new AllWindowedStream(javaStream.timeWindowAll(size))
    }

    /**
      * Windows this DataStream into sliding time windows.
      *
      * This is a shortcut for either `.window(SlidingEventTimeWindows.of(size, slide))` or
      * `.window(SlidingProcessingTimeWindows.of(size, slide))` depending on the time characteristic
      * set using
      * [[StreamExecutionEnvironment.setStreamTimeCharacteristic]].
      *
      * Note: This operation can be inherently non-parallel since all elements have to pass through
      * the same operator instance. (Only for special cases, such as aligned time windows is
      * it possible to perform this operation in parallel).
      *
      * @param size The size of the window.
      */
    def timeWindowAll(size: Time, slide: Time): AllWindowedStream[T, TimeWindow] = {
        new AllWindowedStream(javaStream.timeWindowAll(size, slide))

    }

    /**
      * Windows this [[DataStream]] into sliding count windows.
      *
      * Note: This operation can be inherently non-parallel since all elements have to pass through
      * the same operator instance. (Only for special cases, such as aligned time windows is
      * it possible to perform this operation in parallel).
      *
      * @param size  The size of the windows in number of elements.
      * @param slide The slide interval in number of elements.
      */
    def countWindowAll(size: Long, slide: Long): AllWindowedStream[T, GlobalWindow] = {
        new AllWindowedStream(stream.countWindowAll(size, slide))
    }

    /**
      * Windows this [[DataStream]] into tumbling count windows.
      *
      * Note: This operation can be inherently non-parallel since all elements have to pass through
      * the same operator instance. (Only for special cases, such as aligned time windows is
      * it possible to perform this operation in parallel).
      *
      * @param size The size of the windows in number of elements.
      */
    def countWindowAll(size: Long): AllWindowedStream[T, GlobalWindow] = {
        new AllWindowedStream(stream.countWindowAll(size))
    }

    /**
      * Windows this data stream to a [[AllWindowedStream]], which evaluates windows
      * over a key grouped stream. Elements are put into windows by a [[WindowAssigner]]. The grouping
      * of elements is done both by key and by window.
      *
      * A [[org.apache.flink.streaming.api.windowing.triggers.Trigger]] can be defined to specify
      * when windows are evaluated. However, `WindowAssigner` have a default `Trigger`
      * that is used if a `Trigger` is not specified.
      *
      * Note: This operation can be inherently non-parallel since all elements have to pass through
      * the same operator instance. (Only for special cases, such as aligned time windows is
      * it possible to perform this operation in parallel).
      *
      * @param assigner The `WindowAssigner` that assigns elements to windows.
      * @return The trigger windows data stream.
      */
    @PublicEvolving
    def windowAll[W <: Window](assigner: WindowAssigner[_ >: T, W]): AllWindowedStream[T, W] = {
        new AllWindowedStream[T, W](new JavaAllWindowedStream[T, W](stream, assigner))
    }

    /**
      * Extracts a timestamp from an element and assigns it as the internal timestamp of that element.
      * The internal timestamps are, for example, used to to event-time window operations.
      *
      * If you know that the timestamps are strictly increasing you can use an
      * [[AscendingTimestampExtractor]]. Otherwise,
      * you should provide a [[TimestampExtractor]] that also implements
      * [[TimestampExtractor#getCurrentWatermark]] to keep track of watermarks.
      *
      * @see org.apache.flink.streaming.api.watermark.Watermark
      */
    @deprecated
    def assignTimestamps(extractor: TimestampExtractor[T]): DataStream[T] = {
        asScalaStream(stream.assignTimestamps(clean(extractor)))
    }

    /**
      * Assigns timestamps to the elements in the data stream and periodically creates
      * watermarks to signal event time progress.
      *
      * This method creates watermarks periodically (for example every second), based
      * on the watermarks indicated by the given watermark generator. Even when no new elements
      * in the stream arrive, the given watermark generator will be periodically checked for
      * new watermarks. The interval in which watermarks are generated is defined in
      * [[org.apache.flink.api.common.ExecutionConfig#setAutoWatermarkInterval(long)]].
      *
      * Use this method for the common cases, where some characteristic over all elements
      * should generate the watermarks, or where watermarks are simply trailing behind the
      * wall clock time by a certain amount.
      *
      * For the second case and when the watermarks are required to lag behind the maximum
      * timestamp seen so far in the elements of the stream by a fixed amount of time, and this
      * amount is known in advance, use the
      * [[BoundedOutOfOrdernessTimestampExtractor]].
      *
      * For cases where watermarks should be created in an irregular fashion, for example
      * based on certain markers that some element carry, use the
      * [[AssignerWithPunctuatedWatermarks]].
      *
      * @see AssignerWithPeriodicWatermarks
      * @see AssignerWithPunctuatedWatermarks
      * @see #assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks)
      */
    @PublicEvolving
    def assignTimestampsAndWatermarks(assigner: AssignerWithPeriodicWatermarks[T]): DataStream[T] = {
        asScalaStream(stream.assignTimestampsAndWatermarks(assigner))
    }

    /**
      * Assigns timestamps to the elements in the data stream and periodically creates
      * watermarks to signal event time progress.
      *
      * This method creates watermarks based purely on stream elements. For each element
      * that is handled via [[AssignerWithPunctuatedWatermarks#extractTimestamp(Object, long)]],
      * the [[AssignerWithPunctuatedWatermarks#checkAndGetNextWatermark()]] method is called,
      * and a new watermark is emitted, if the returned watermark value is larger than the previous
      * watermark.
      *
      * This method is useful when the data stream embeds watermark elements, or certain elements
      * carry a marker that can be used to determine the current event time watermark.
      * This operation gives the programmer full control over the watermark generation. Users
      * should be aware that too aggressive watermark generation (i.e., generating hundreds of
      * watermarks every second) can cost some performance.
      *
      * For cases where watermarks should be created in a regular fashion, for example
      * every x milliseconds, use the [[AssignerWithPeriodicWatermarks]].
      *
      * @see AssignerWithPunctuatedWatermarks
      * @see AssignerWithPeriodicWatermarks
      * @see #assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks)
      */
    @PublicEvolving
    def assignTimestampsAndWatermarks(assigner: AssignerWithPunctuatedWatermarks[T])
    : DataStream[T] = {

        asScalaStream(stream.assignTimestampsAndWatermarks(assigner))
    }

    /**
      * Assigns timestamps to the elements in the data stream and periodically creates
      * watermarks to signal event time progress.
      *
      * This method is a shortcut for data streams where the element timestamp are known
      * to be monotonously ascending within each parallel stream.
      * In that case, the system can generate watermarks automatically and perfectly
      * by tracking the ascending timestamps.
      *
      * For cases where the timestamps are not monotonously increasing, use the more
      * general methods [[assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks)]]
      * and [[assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks)]].
      */
    @PublicEvolving
    def assignAscendingTimestamps(extractor: T => Long): DataStream[T] = {
        val cleanExtractor = clean(extractor)
        val extractorFunction = new AscendingTimestampExtractor[T] {
            def extractAscendingTimestamp(element: T): Long = {
                cleanExtractor(element)
            }
        }
        asScalaStream(stream.assignTimestampsAndWatermarks(extractorFunction))
    }

    /**
      * 运算符，用于使用OutputSelector将元组定向到特定的命名输出。对运算符调用此方法将创建一个新的[[拆分流]]。
      *
      * Operator used for directing tuples to specific named outputs using an
      * OutputSelector. Calling this method on an operator creates a new
      * [[SplitStream]].
      *
      * @deprecated Please use side output instead.
      */
    @deprecated
    def split(selector: OutputSelector[T]): SplitStream[T] = asScalaStream(stream.split(selector))

    /**
      * 创建一个新的[[拆分流]]，它只包含满足给定输出选择器谓词的元素。
      *
      * Creates a new [[SplitStream]] that contains only the elements satisfying the
      * given output selector predicate.
      *
      * @deprecated Please use side output instead.
      */
    @deprecated
    def split(fun: T => TraversableOnce[String]): SplitStream[T] = {
        if (fun == null) {
            throw new NullPointerException("OutputSelector must not be null.")
        }
        val cleanFun = clean(fun)
        val selector = new OutputSelector[T] {
            def select(in: T): java.lang.Iterable[String] = {
                cleanFun(in).toIterable.asJava
            }
        }
        split(selector)
    }

    /**
      * Creates a co-group operation. See [[CoGroupedStreams]] for an example of how the keys
      * and window can be specified.
      */
    def coGroup[T2](otherStream: DataStream[T2]): CoGroupedStreams[T, T2] = {
        new CoGroupedStreams(this, otherStream)
    }

    /**
      * 创建联接操作。有关如何指定键和窗口的示例，请参见[[JoinedStreams]]。
      *
      * Creates a join operation. See [[JoinedStreams]] for an example of how the keys
      * and window can be specified.
      */
    def join[T2](otherStream: DataStream[T2]): JoinedStreams[T, T2] = {
        new JoinedStreams(this, otherStream)
    }

    /**
      * 将数据流写入标准输出流（stdout）。对于数据流的每个元素，的结果。toString已写入。
      *
      * Writes a DataStream to the standard output stream (stdout). For each
      * element of the DataStream the result of .toString is
      * written.
      *
      */
    @PublicEvolving
    def print(): DataStreamSink[T] = stream.print()

    /**
      * 将数据流写入标准输出流（stderr）。
      *
      * Writes a DataStream to the standard output stream (stderr).
      *
      * For each element of the DataStream the result of
      * [[AnyRef.toString()]] is written.
      *
      * @return The closed DataStream.
      */
    @PublicEvolving
    def printToErr() = stream.printToErr()

    /**
      * 将数据流写入标准输出流（stdout）。对于数据流的每个元素，写入[[AnyRef.toString（）]的结果。
      *
      * Writes a DataStream to the standard output stream (stdout). For each
      * element of the DataStream the result of [[AnyRef.toString ( )]] is
      * written.
      *
      * @param sinkIdentifier The string to prefix the output with.
      * @return The closed DataStream.
      */
    @PublicEvolving
    def print(sinkIdentifier: String): DataStreamSink[T] = stream.print(sinkIdentifier)

    /**
      * 将数据流写入标准输出流（stderr）。
      *
      * Writes a DataStream to the standard output stream (stderr).
      *
      * For each element of the DataStream the result of
      * [[AnyRef.toString()]] is written.
      *
      * @param sinkIdentifier The string to prefix the output with.
      * @return The closed DataStream.
      */
    @PublicEvolving
    def printToErr(sinkIdentifier: String) = stream.printToErr(sinkIdentifier)

    /**
      * 以文本格式将数据流写入path指定的文件。对于数据流的每个元素，的结果。toString已写入。
      *
      * Writes a DataStream to the file specified by path in text format. For
      * every element of the DataStream the result of .toString is written.
      *
      * @param path The path pointing to the location the text file is written to
      * @return The closed DataStream
      */
    @PublicEvolving
    def writeAsText(path: String): DataStreamSink[T] =
        stream.writeAsText(path)


    /**
      * 以文本格式将数据流写入path指定的文件。对于数据流的每个元素，的结果。toString已写入。
      *
      * Writes a DataStream to the file specified by path in text format. For
      * every element of the DataStream the result of .toString is written.
      *
      * @param path      The path pointing to the location the text file is written to
      * @param writeMode Controls the behavior for existing files. Options are NO_OVERWRITE and
      *                  OVERWRITE.
      * @return The closed DataStream
      */
    @PublicEvolving
    def writeAsText(path: String, writeMode: FileSystem.WriteMode): DataStreamSink[T] = {
        if (writeMode != null) {
            stream.writeAsText(path, writeMode)
        } else {
            stream.writeAsText(path)
        }
    }

    /**
      * 将CSV格式的数据流写入路径参数指定的文件。每隔毫秒周期性地执行写入。
      *
      * Writes the DataStream in CSV format to the file specified by the path parameter. The writing
      * is performed periodically every millis milliseconds.
      *
      * @param path Path to the location of the CSV file
      * @return The closed DataStream
      */
    @PublicEvolving
    def writeAsCsv(path: String): DataStreamSink[T] = {
        writeAsCsv(
            path,
            null,
            ScalaCsvOutputFormat.DEFAULT_LINE_DELIMITER,
            ScalaCsvOutputFormat.DEFAULT_FIELD_DELIMITER)
    }

    /**
      * 将CSV格式的数据流写入路径参数指定的文件。每隔毫秒周期性地执行写入。
      *
      * Writes the DataStream in CSV format to the file specified by the path parameter. The writing
      * is performed periodically every millis milliseconds.
      *
      * @param path      Path to the location of the CSV file
      * @param writeMode Controls whether an existing file is overwritten or not
      * @return The closed DataStream
      */
    @PublicEvolving
    def writeAsCsv(path: String, writeMode: FileSystem.WriteMode): DataStreamSink[T] = {
        writeAsCsv(
            path,
            writeMode,
            ScalaCsvOutputFormat.DEFAULT_LINE_DELIMITER,
            ScalaCsvOutputFormat.DEFAULT_FIELD_DELIMITER)
    }

    /**
      * Writes the DataStream in CSV format to the file specified by the path parameter. The writing
      * is performed periodically every millis milliseconds.
      *
      * @param path           Path to the location of the CSV file
      * @param writeMode      Controls whether an existing file is overwritten or not
      * @param rowDelimiter   Delimiter for consecutive rows
      * @param fieldDelimiter Delimiter for consecutive fields
      * @return The closed DataStream
      */
    @PublicEvolving
    def writeAsCsv(
                      path: String,
                      writeMode: FileSystem.WriteMode,
                      rowDelimiter: String,
                      fieldDelimiter: String)
    : DataStreamSink[T] = {
        require(stream.getType.isTupleType, "CSV output can only be used with Tuple DataSets.")
        val of = new ScalaCsvOutputFormat[Product](new Path(path), rowDelimiter, fieldDelimiter)
        if (writeMode != null) {
            of.setWriteMode(writeMode)
        }
        stream.writeUsingOutputFormat(of.asInstanceOf[OutputFormat[T]])
    }

    /**
      * 使用给定的[[OutputFormat]]写入数据流。
      *
      * Writes a DataStream using the given [[OutputFormat]].
      */
    @PublicEvolving
    def writeUsingOutputFormat(format: OutputFormat[T]): DataStreamSink[T] = {
        stream.writeUsingOutputFormat(format)
    }

    /**
      * 将数据流作为字节数组写入套接字。输出的格式由[[SerializationSchema]]指定。
      *
      * Writes the DataStream to a socket as a byte array. The format of the output is
      * specified by a [[SerializationSchema]].
      */
    @PublicEvolving
    def writeToSocket(
                         hostname: String,
                         port: Integer,
                         schema: SerializationSchema[T]): DataStreamSink[T] = {
        stream.writeToSocket(hostname, port, schema)
    }

    /**
      * 将给定接收器添加到此数据流。一旦StreamExecutionEnvironment运行，只有添加了接收器的流才会执行。
      * 执行（…）方法被调用。
      *
      * Adds the given sink to this DataStream. Only streams with sinks added
      * will be executed once the StreamExecutionEnvironment.execute(...)
      * method is called.
      *
      */
    def addSink(sinkFunction: SinkFunction[T]): DataStreamSink[T] =
        stream.addSink(sinkFunction)

    /**
      * 将给定接收器添加到此数据流。调用StreamExecutionEnvironment.execute（…）方法后，将只执行添加了接收器的流。
      *
      * Adds the given sink to this DataStream. Only streams with sinks added
      * will be executed once the StreamExecutionEnvironment.execute(...)
      * method is called.
      *
      */
    def addSink(fun: T => Unit): DataStreamSink[T] = {
        if (fun == null) {
            throw new NullPointerException("Sink function must not be null.")
        }
        val cleanFun = clean(fun)
        val sinkFunction = new SinkFunction[T] {
            override def invoke(in: T) = cleanFun(in)
        }
        this.addSink(sinkFunction)
    }

    /**
      * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
      * is not disabled in the [[org.apache.flink.api.common.ExecutionConfig]].
      */
    private[flink] def clean[F <: AnyRef](f: F): F = {
        new StreamExecutionEnvironment(stream.getExecutionEnvironment).scalaClean(f)
    }

    /**
      * 使用自定义[[OneInputStreamOperator]]转换[[数据流]]。
      *
      * Transforms the [[DataStream]] by using a custom [[OneInputStreamOperator]].
      *
      * @param operatorName name of the operator, for logging purposes
      * @param operator     the object containing the transformation logic
      * @tparam R the type of elements emitted by the operator
      */
    @PublicEvolving
    def transform[R: TypeInformation](
                                         operatorName: String,
                                         operator: OneInputStreamOperator[T, R]): DataStream[R] = {
        asScalaStream(stream.transform(operatorName, implicitly[TypeInformation[R]], operator))
    }
}
