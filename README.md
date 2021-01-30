## **源码阅读**
* [Spark Core-2.2.0 源码阅读](SparkCore)
* [Spark SQL-2.2.0 源码阅读](SparkSqlCore)
* [Spark Streaming-2.2.0 源码阅读](SparkStreaming)
* [Flink Core-1.9.0 源码阅读](FlinkCore)
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
> [核心SparkSQL功能模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/util)：查询异常监听器。

---
### 三、spark streaming源码阅读
    SparkSql模块源码阅读，版本2.2.0。

---
### 四、flink core源码阅读
    FlinkCore模块源码阅读，版本1.9.0。

---
### 五、scala library源码阅读
    ScalaLibrary模块源码阅读，版本2.11.8。

---
### 六、java src源码阅读
    JavaSrc模块源码阅读，版本1.8.0。

