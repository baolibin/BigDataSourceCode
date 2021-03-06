## 前奏
    本项目主要是大数据相关技术源码阅读。
    随着大数据行情发展，支撑这个体系相关的技术也越来越多。
    目前列出来的是比较核心常用的框架源码，包括：
    基础编程语言：Java、Scala
    数据处理框架：Spark、Flink
    欢迎小伙伴一起加入阅读，夯实自己的技术，体验其中的乐趣。
    持续更新中...

---
## **源码阅读**
* [1、【Scala Library-2.11.8】 源码阅读](ScalaLibrary)
    - [1.0、主要源码类阅读]()
        - [reflect.ClassTag类]()
* [2、【Java Src-1.8.0】 源码阅读](JavaSrcCode)
    - [2.0、主要源码类阅读]()
        - [java.util.HashMap类]()
        - [java.util.HashSet类]()
        - [java.util.ArrayList类]()
        - [java.util.concurrent.ConcurrentHashMap类]()
        - [java.util.LinkedHashMap类]()
        - [java.util.LinkedList类]()
        - [java.lang.Object类]()
        - [java.lang.reflect.Array类]()
        - [java.lang.Class类]()
        - [java.lang.String类]()
    - [2.1、Java集合类汇总](JavaSrcCode/doc/Java集合类汇总.md)
* [3、【Spark Core-2.2.0】 源码阅读](SparkCore)
    - [3.0、主要源码类阅读]()
        - [org.apache.spark.deploy.SparkSubmit类]()
        - [org.apache.spark.SparkContext类]()
        - [org.apache.spark.scheduler.DAGScheduler类]()
    - [3.1、Spark作业提交流程](SparkCore/src/main/doc/Spark作业提交流程.md)
    - [3.2、Spark内存模型](SparkCore/src/main/doc/Spark内存模型.md)
* [4、【Spark SQL-2.2.0】 源码阅读](SparkSqlCore)
    - [4.0、主要源码类阅读]()
        - [org.apache.spark.sql.Dataset类]()
        - [org.apache.spark.sql.SparkSession类]()
        - [org.apache.spark.sql.SQLContext类]()
        - [org.apache.spark.sql.Column类]()
* [5、【Spark Streaming-2.2.0】 源码阅读](SparkStreaming)
    - [5.0、主要源码类阅读]()
        - [org.apache.spark.streaming.StreamingContext类]()
        - [org.apache.spark.streaming.dstream.DStream类]()
        - [org.apache.spark.streaming.State类]()
* [6、【Flink Batch-1.9.0】 源码阅读](FlinkBatch)
    - [6.0、主要源码类阅读]()
        - [org.apache.flink.api.scala.ExecutionEnvironment类]()
        - [org.apache.flink.api.scala.DataSet类]()
* [7、【Flink Streaming-1.9.0】 源码阅读](FlinkStreaming)
    - [7.0、主要源码类阅读]()
        - [org.apache.flink.streaming.api.scala.StreamExecutionEnvironment类]()
        - [org.apache.flink.streaming.api.scala.DataStream类]()
        - [org.apache.flink.streaming.api.scala.OutputTag类]()
* [8、【Flink Core-1.9.0】 源码阅读](FlinkCore)
    - [8.0、主要源码类阅读]()
        - [org.apache.flink.util.OutputTag类]()
        - [org.apache.flink.api.common.typeinfo.TypeInformation类]()
        - [org.apache.flink.api.common.state.State类]()
        - [org.apache.flink.api.common.operators.Operator类]()
        - [org.apache.flink.api.common.functions.FlatMapFunction类]()
        - [org.apache.flink.api.common.functions.RichFlatMapFunction类]()
        - [org.apache.flink.streaming.api.watermark.Watermark类]()

---
### 一、spark core源码阅读
    SparkCore模块源码阅读，版本2.2.0。
    包括部署Deploy模块、执行Executor模块、内存Memory模块、调度Scheduler模块、经典的Shuffle模块、存储Storage模块等等。
##### [1、Deploy模块源码地址](SparkCore/src/main/scala/org/apache/spark/deploy)：Spark作业提交部署运行模块。
##### [2、Executor模块源码地址](SparkCore/src/main/scala/org/apache/spark/executor)：与各种集群管理器一起使用的Executor组件。
##### [3、Memory模块源码](SparkCore/src/main/scala/org/apache/spark/memory)：这个软件包实现了Spark的内存管理系统。这个系统由两个主要组件组成，一个JVM范围的内存管理器和一个每个任务的内存管理器。
##### [4、Scheduler模块源码地址](SparkCore/src/main/scala/org/apache/spark/scheduler)：Spark的调度组件。这包括`DAGScheduler`以及lower level级别的`TaskScheduler`。
##### [5、Shuffle模块源码地址](SparkCore/src/main/scala/org/apache/spark/shuffle)：map任务到reduce任务数据重新分发模块。
##### [6、Storage模块源码地址](SparkCore/src/main/scala/org/apache/spark/storage)：提供RDD的数据存储模块。
##### [7、Util模块源码地址](SparkCore/src/main/scala/org/apache/spark/util)： Spark实用程序。
##### [8、核心Spark功能模块源码地址](SparkCore/src/main/scala/org/apache/spark)： 核心Spark功能，[[org.apache.spark.SparkContext]]是Spark的主要入口，而[[org.apache.spark.rdd。rdd]]表示分布式集合的数据类型，并提供大多数并行操作。
##### [9、RDD源码地址](SparkCore/src/main/scala/org/apache/spark/rdd)：提供各种RDD的实现。
##### [10、Rpc通信源码地址](SparkCore/src/main/scala/org/apache/spark/rpc)：通信模块。
##### [11、IO源码地址](SparkCore/src/main/scala/org/apache/spark/io)：用于压缩的IO编解码器。

---
### 二、spark sql源码阅读
    SparkSql模块源码阅读，版本2.2.0。
##### [1、核心SparkSQL功能模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql)： 允许执行关系查询，包括使用Spark在SQL中表示的查询。
##### [2、Api模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/api)：包含特定于单一语言（即Java）的API类。
##### [3、CataLog模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/catalog)：Spark的目录接口。要访问请使用`SparkSession.catalog`。
##### [4、Execution模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/execution):sparksql的物理执行组件。请注意，这是一个私有包。catalyst中的所有类都被认为是激发SQL的内部API，并且在不同的小版本之间会发生变化。
##### [5、Expressions模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/expressions):包含UDAF、Window等操作的表达计算类。
##### [6、Internal模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/internal):这个包中的所有类都被认为是Spark的内部API，并且在小版本之间可能会发生更改。
##### [7、Jdbc模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/jdbc):数据库操作相关类。
##### [8、Sources模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/sources):一组用于向sparksql添加数据源的api。
##### [9、Streaming模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/streaming):
##### [10、Util模块源码](SparkSqlCore/src/main/scala/org/apache/spark/sql/util)：查询异常监听器。

---
### 三、spark streaming源码阅读
    SparkSql模块源码阅读，版本2.2.0。
##### [1、核心SparkStreaming功能模块源码](SparkStreaming/src/main/scala/org/apache/spark)： Spark流处理功能。[[org.apache.spark.streaming.StreamingContext]]作为Spark流处理的主要入口。[[org.apache.spark.streaming.dstream.DStream]]表示RDD的连续序列的数据类型，表示连续的数据流。
##### [2、Api功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/api)：Spark streaming的Java与Python API。
##### [3、Dstream功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/dstream)：DStream的各种实现。
##### [4、Rdd功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/rdd)：RDD存储“mapWithState”操作的键控状态和相应的映射数据。
##### [5、Receiver功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/receiver)：可在工作节点上运行以接收外部数据的接收器的抽象类。
##### [6、Scheduler功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/scheduler)：作业调度模块。
##### [7、Ui功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/ui)：实时流UI模块。
##### [8、Util功能模块源码](SparkStreaming/src/main/scala/org/apache/spark/streaming/util)：相关操作工具类。

---
### 四、flink core源码阅读
    FlinkCore模块源码阅读，版本1.9.0。
##### [1、api模块源码](FlinkCore/src/main/java/org/apache/flink/api)：
##### [2、configuration模块源码](FlinkCore/src/main/java/org/apache/flink/configuration)：
##### [3、core模块源码](FlinkCore/src/main/java/org/apache/flink/core)：
##### [4、types模块源码](FlinkCore/src/main/java/org/apache/flink/types)：
##### [5、util模块源码](FlinkCore/src/main/java/org/apache/flink/util)：

---
### 五、flink streaming源码阅读
    FlinkStreaming模块源码阅读，版本1.9.0。
##### [1、api模块源码](FlinkStreaming/src/main/java/org/apache/flink/streaming/api)：包括API操作类。
##### [2、experimental模块源码](FlinkStreaming/src/main/java/org/apache/flink/streaming/experimental)：这个包包含实验类。
##### [3、runtime模块源码](FlinkStreaming/src/main/java/org/apache/flink/streaming/runtime)：包含实现流运行时的类。
##### [4、util模块源码](FlinkStreaming/src/main/java/org/apache/flink/streaming/util)：流式计算相关操作工具类。
##### [5、async模块源码](FlinkStreaming/src/main/scala/org/apache/flink/streaming/api/scala/async)：触发异步I/O操作的函数。
##### [6、extensions模块源码](FlinkStreaming/src/main/scala/org/apache/flink/streaming/api/scala/extensions)：这个扩展包括对所有数据流表示的几个隐式转换，这些数据流表示可以从这个特性中获得。要使用这组扩展方法，用户必须通过导入acceptPartialFunctions。
##### [7、function模块源码](FlinkStreaming/src/main/scala/org/apache/flink/streaming/api/scala/function)：主要包括一些窗口操作等方法类。
##### [8、核心模块源码](FlinkStreaming/src/main/scala/org/apache/flink/streaming/api/scala)：流式计算相关主要操作类，包括DataStream、StreamExecutionEnvironment等。

---
### 六、flink batch源码阅读
    FlinkBatch模块源码阅读，版本1.9.0。
##### [1、operators模块源码](FlinkBatch/FlinkScalaBatch/src/main/java/org/apache/flink/api/scala/operators)：对数据的一些操作。
##### [2、typeutils模块源码](FlinkBatch/FlinkScalaBatch/src/main/java/org/apache/flink/api/scala/typeutils)：一些序列化类型工具类。
##### [3、codegen模块源码](FlinkBatch/FlinkScalaBatch/src/main/scala/org/apache/flink/api/scala/codegen)：
##### [4、extensions模块源码](FlinkBatch/FlinkScalaBatch/src/main/scala/org/apache/flink/api/scala/extensions)：扩展包括对所有数据集表示的几个隐式转换，这些数据集表示可以从这个特性中获得。
##### [5、metrics模块源码](FlinkBatch/FlinkScalaBatch/src/main/scala/org/apache/flink/api/scala/metrics)：这个类允许使用函数引用从Scala简明地定义一个规范。
##### [6、typeutils模块源码](FlinkBatch/FlinkScalaBatch/src/main/scala/org/apache/flink/api/scala/typeutils)：
##### [7、utils模块源码](FlinkBatch/FlinkScalaBatch/src/main/scala/org/apache/flink/api/scala/utils)：此类提供了简单的实用程序方法，用于使用索引或唯一标识符压缩数据集中的元素，从数据集中采样元素。
##### [8、核心模块源码](FlinkBatch/FlinkScalaBatch/src/main/java/org/apache/flink/api/scala)：Flink Scala API。

---
### 七、scala library源码阅读
    ScalaLibrary模块源码阅读，版本2.11.8。
##### [1、核心Scala Library-Aux功能模块源码](ScalaLibrary/src/main/scala/libiary-aux)：
##### [2、核心Scala Library功能模块源码](ScalaLibrary/src/main/scala)： 核心Scala类型。它们在没有显式导入的情况下始终可用。
##### [3、Annotation功能模块源码](ScalaLibrary/src/main/scala/annotation)： 
##### [4、Beans功能模块源码](ScalaLibrary/src/main/scala/beans)： 
##### [5、Collection功能模块源码](ScalaLibrary/src/main/scala/collection)： 包含使用和扩展Scala集合库所需的基本特性和对象。
##### [6、Compat功能模块源码](ScalaLibrary/src/main/scala/compat)： 
##### [7、Concurrent功能模块源码](ScalaLibrary/src/main/scala/concurrent)： 包含用于并发和并行编程的原语。
##### [8、Io功能模块源码](ScalaLibrary/src/main/scala/io)： 
##### [9、Math功能模块源码](ScalaLibrary/src/main/scala/math)： 
##### [10、Ref功能模块源码](ScalaLibrary/src/main/scala/ref)： 
##### [11、Reflect功能模块源码](ScalaLibrary/src/main/scala/reflect)： 
##### [12、Runtime功能模块源码](ScalaLibrary/src/main/scala/runtime)： 
##### [13、Sys功能模块源码](ScalaLibrary/src/main/scala/sys)： 
##### [14、Text功能模块源码](ScalaLibrary/src/main/scala/text)： 
##### [15、Util功能模块源码](ScalaLibrary/src/main/scala/util)： 

---
### 八、java src源码阅读
    JavaSrc模块源码阅读，版本1.8.0。
##### [1、applet模块源码]()
##### [2、awt模块源码]()
##### [3、beans模块源码]()
##### [4、io模块源码]()
##### [5、lang模块源码]()
##### [6、math模块源码]()
##### [7、net模块源码]()
##### [8、nio模块源码]()
##### [9、rmi模块源码]()
##### [10、security模块源码]()
##### [11、sql模块源码]()
##### [12、text模块源码]()
##### [13、time模块源码]()
##### [14、util模块源码]()
