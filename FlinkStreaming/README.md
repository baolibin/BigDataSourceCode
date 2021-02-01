#### FlinkStreaming源码阅读
    FlinkStreaming模块源码阅读，版本1.9.0。

-----
##### 1、api模块源码
> [api模块源码](src/main/java/org/apache/flink/streaming/api)：

-----
##### 2、experimental模块源码
> [experimental模块源码](src/main/java/org/apache/flink/streaming/experimental)：

-----
##### 3、runtime模块源码
> [runtime模块源码](src/main/java/org/apache/flink/streaming/runtime)：

-----
##### 4、util模块源码
> [util模块源码](src/main/java/org/apache/flink/streaming/util)：

-----
##### 5、async模块源码
> [async模块源码](src/main/scala/org/apache/flink/streaming/api/scala/async)：

-----
##### 6、extensions模块源码
> [extensions模块源码](src/main/scala/org/apache/flink/streaming/api/scala/extensions)：

-----
##### 7、function模块源码
> [function模块源码](src/main/scala/org/apache/flink/streaming/api/scala/function)：

-----
##### 8、核心模块源码
> [核心模块源码](src/main/scala/org/apache/flink/streaming/api/scala)：createTypeInformation当使用scalaapi时，我们总是生成类型信息。
* AllWindowedStream：[[AllWindowedStream]]表示一个数据流，其中元素流基于[[WindowAssigner]]. 窗口发射基于[[Trigger]]触发。
* AsyncDataStream：将[[AsyncFunction]]应用于数据流的帮助程序类。
* BroadcastConnectedStream：
* CoGroupedStreams：`CoGroupedStreams`表示已共同分组的两个[[DataStream]]。流式协作组操作在窗口中的元素上进行评估。
* ConnectedStreams：[[ConnectedStreams]]表示两个（可能）不同数据类型的连接流。连接流对于一个流上的操作直接影响另一个流上的操作的情况很有用，通常通过流之间的共享状态。
* DataStream：实时流处理的数据集。
* DataStreamUtils：此类提供用于收集[[DataStream]]的简单实用工具方法，通过[[DataStreamUtils]]封装的功能有效地丰富了它。
* JoinedStreams：`JoinedStreams`表示已联接的两个[[DataStream]]。流连接操作在窗口中的元素上求值。
* KeyedStream：
* OutputTag：[[OutputTag]]是一个类型化和命名的标记，用于标记操作符的side输出。
* SplitStream：SplitStream表示已使用[[org.apache.flink.streaming.api.collector.selector.OutputSelector]].可以使用[[SplitStream#select（）]]函数选择命名输出。要在整个输出上应用转换，只需在此流上调用适当的方法。
* StreamExecutionEnvironment：创建实时流实现环境上下文入口。
* WindowedStream：[[WindowedStream]]表示一个数据流，其中元素按键分组，对于每个键，元素流基于[[WindowAssigner]]. 窗口发射基于[[Trigger]]触发。
