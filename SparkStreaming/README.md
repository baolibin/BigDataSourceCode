#### org.apache.spark streaming源码阅读
    SparkStreaming模块源码阅读，版本2.2。

-----
##### 1、核心SparkStreaming功能模块源码
> [核心SparkStreaming功能模块源码](src/main/scala/org/apache/spark)： Spark流处理功能。[[org.apache.spark.streaming.StreamingContext]]作为Spark流处理的主要入口。[[org.apache.spark.streaming.dstream.DStream]]表示RDD的连续序列的数据类型，表示连续的数据流。
* Checkpoint：
* ContextWaiter：
* DStreamGraph：
* Duration：
* Interval：
* State：用于获取和更新“mapWithState”操作中使用的映射函数中的状态的抽象类。
* StateSpec：表示数据流转换“mapWithState”操作的所有规范的抽象类。
* StreamingContext：Spark流功能的主要入口点。
* StreamingSource：
* Time：这是一个简单的类，表示时间的绝对瞬间。

-----
##### 2、Api功能模块源码
> [api功能模块源码](src/main/scala/org/apache/spark/streaming/api)：Spark streaming的Java与Python API。
* Java：Spark streaming的Java API。
    - JavaDStream：一个有关[[org.apache.spark.streaming.dstream.DStream]]的Java接口，Spark Streaming中表示连续数据流的基本抽象。
* ：

-----
##### 3、Dstream功能模块源码
> [dstream功能模块源码](src/main/scala/org/apache/spark/streaming/dstream)：DStream的各种实现。
* ：
* ：

-----
##### 4、Rdd功能模块源码
> [rdd功能模块源码](src/main/scala/org/apache/spark/streaming/rdd)：
* ：
* ：

-----
##### 5、Receiver功能模块源码
> [receiver功能模块源码](src/main/scala/org/apache/spark/streaming/receiver)：
* ：
* ：

-----
##### 6、Scheduler功能模块源码
> [scheduler功能模块源码](src/main/scala/org/apache/spark/streaming/scheduler)：
* ：
* ：

-----
##### 7、Ui功能模块源码
> [ui功能模块源码](src/main/scala/org/apache/spark/streaming/ui)：
* ：
* ：

-----
##### 8、Util功能模块源码
> [util功能模块源码](src/main/scala/org/apache/spark/streaming/util)：
* ：
* ：

