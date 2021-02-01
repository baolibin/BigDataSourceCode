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
* AllWindowedStream：
* AsyncDataStream：
* BroadcastConnectedStream：
* CoGroupedStreams：
* ConnectedStreams：
* DataStream：实时流处理的数据集。
* DataStreamUtils：
* JoinedStreams：
* KeyedStream：
* OutputTag：[[OutputTag]]是一个类型化和命名的标记，用于标记操作符的side输出。
* SplitStream：
* StreamExecutionEnvironment：创建实时流实现环境上下文入口。
* WindowedStream：
