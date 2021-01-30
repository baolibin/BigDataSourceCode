## **源码阅读**
* [Spark Core-2.2.0 源码阅读](SparkCore)
* [Spark SQL-2.2.0 源码阅读](SparkSqlCore)
* [Spark Streaming-2.2.0 源码阅读](SparkStreaming)
* [Flink Core-1.9.0 源码阅读](FlinkCore)
* [Flink Streaming-1.9.0 源码阅读](FlinkStreaming)
* [Scala Library-2.11.8 源码阅读](ScalaLibrary)
* [Java Src-1.8.0 源码阅读](JavaSrc)

---
### 一、spark core源码阅读
    SparkCore模块源码阅读，版本2.2.0。
    包括部署Deploy模块、执行Executor模块、内存Memory模块、调度Scheduler模块、经典的Shuffle模块、存储Storage模块等等。
##### 1、Deploy模块源码
> [deploy源码地址](SparkCore/src/main/scala/org/apache/spark/deploy)
##### 2、Executor模块源码
> [executor源码地址](SparkCore/src/main/scala/org/apache/spark/executor)：与各种集群管理器一起使用的Executor组件。
##### 3、Memory模块源码
> [memory源码地址](SparkCore/src/main/scala/org/apache/spark/memory)：这个软件包实现了Spark的内存管理系统。这个系统由两个主要组件组成，一个JVM范围的内存管理器和一个每个任务的内存管理器。
##### 4、Scheduler模块源码
> [scheduler源码地址](SparkCore/src/main/scala/org/apache/spark/scheduler)：Spark的调度组件。这包括`DAGScheduler`以及lower level级别的`TaskScheduler`。
##### 5、Shuffle模块源码
> [shuffle源码地址](SparkCore/src/main/scala/org/apache/spark/shuffle)
##### 6、Storage模块源码
> [storage源码地址](SparkCore/src/main/scala/org/apache/spark/storage)
##### 7、Util模块源码
> [util源码地址](SparkCore/src/main/scala/org/apache/spark/util)： Spark实用程序。
##### 8、核心Spark功能模块源码
> [核心Spark功能模块源码地址](SparkCore/src/main/scala/org/apache/spark)： 核心Spark功能，[[org.apache.spark.SparkContext]]是Spark的主要入口，而[[org.apache.spark.rdd。rdd]]表示分布式集合的数据类型，并提供大多数并行操作。
##### 9、RDD源码
> [RDD源码地址](SparkCore/src/main/scala/org/apache/spark/rdd)：提供各种RDD的实现。
##### 10、Rpc通信源码
> [Rpc通信源码地址](SparkCore/src/main/scala/org/apache/spark/rpc)：
##### 11、IO源码
> [IO源码地址](SparkCore/src/main/scala/org/apache/spark/io)：

---
### 二、spark sql源码阅读
    SparkSql模块源码阅读，版本2.2.0。
##### 1、核心SparkSQL功能模块源码
> [核心SparkSQL功能模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql)： 允许执行关系查询，包括使用Spark在SQL中表示的查询。
##### 2、Api模块源码
> [api模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/api)：包含特定于单一语言（即Java）的API类。
##### 3、CataLog模块源码
##### 4、Execution模块源码
##### 5、Expressions模块源码
> [expressions功能模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/expressions):
##### 6、Internal模块源码
##### 7、Jdbc模块源码
##### 8、Sources模块源码
##### 9、Streaming模块源码
##### 10、Util模块源码
> [Util模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/util)：查询异常监听器。

---
### 三、spark streaming源码阅读
    SparkSql模块源码阅读，版本2.2.0。
##### 1、核心SparkStreaming功能模块源码
> [核心SparkStreaming功能模块源码](SparkStreaming/src/main/scala/org/apache/spark)： Spark流处理功能。[[org.apache.spark.streaming.StreamingContext]]作为Spark流处理的主要入口。[[org.apache.spark.streaming.dstream.DStream]]表示RDD的连续序列的数据类型，表示连续的数据流。
##### 2、Api功能模块源码
> [api功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/api)：Spark streaming的Java与Python API。
##### 3、Dstream功能模块源码
> [dstream功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/dstream)：DStream的各种实现。
##### 4、Rdd功能模块源码
> [rdd功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/rdd)：
##### 5、Receiver功能模块源码
> [receiver功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/receiver)：
##### 6、Scheduler功能模块源码
> [scheduler功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/scheduler)：
##### 7、Ui功能模块源码
> [ui功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/ui)：
##### 8、Util功能模块源码
> [util功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/util)：

---
### 四、flink core源码阅读
    FlinkCore模块源码阅读，版本1.9.0。

---
### 五、flink streaming源码阅读
    FlinkStreaming模块源码阅读，版本1.9.0。

---
### 六、scala library源码阅读
    ScalaLibrary模块源码阅读，版本2.11.8。

---
### 七、java src源码阅读
    JavaSrc模块源码阅读，版本1.8.0。

