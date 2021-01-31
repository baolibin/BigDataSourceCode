#### Flink Scala源码阅读
    FlinkScala模块源码阅读，版本1.9.0。
    Flink离线处理模块，Scala Api使用接口。

-----
##### 1、operators模块源码
> [operators模块源码](src/main/java/org/apache/flink/api/scala/operators)：

-----
##### 2、typeutils模块源码
> [typeutils模块源码](src/main/java/org/apache/flink/api/scala/typeutils)：

-----
##### 3、codegen模块源码
> [codegen模块源码](src/main/scala/org/apache/flink/api/scala/codegen)：

-----
##### 4、extensions模块源码
> [extensions模块源码](src/main/scala/org/apache/flink/api/scala/extensions)：

-----
##### 5、metrics模块源码
> [metrics模块源码](src/main/scala/org/apache/flink/api/scala/metrics)：

-----
##### 6、typeutils模块源码
> [typeutils模块源码](src/main/scala/org/apache/flink/api/scala/typeutils)：

-----
##### 7、utils模块源码
> [utils模块源码](src/main/scala/org/apache/flink/api/scala/utils)：

-----
##### 8、核心模块源码
> [核心模块源码](src/main/java/org/apache/flink/api/scala)：Flink Scala API。
        [[org.apache.flink.api.scala.ExecutionEnvironment]]是任何Flink项目的起点。它可以用来读取本地文件、hdf或其他源。
        [[org.apache.flink.api.scala.DataSet]]是Flink中数据的主要抽象。它提供通过转换创建新数据集的操作。
        [[org.apache.flink.api.scala.GroupedDataSet]]提供对来自的分组数据的操作[[org.apache.flink.api.scala.DataSet.groupBy()]]。
        使用[[org.apache.flink.api.scala.ExecutionEnvironment.getExecutionEnvironment]]以获取执行环境。这将创建本地环境或远程环境，具体取决于程序执行的上下文。
* AggregateDataSet：[[DataSet.aggregate]]的结果. 这可用于将多个聚合链接到一个聚合运算符。
* ClosureCleaner：此代码最初来自apache spark项目。
* CoGroupDataSet：由“coGroup”操作产生的特定[[DataSet]]。
* CrossDataSet：
* DataSet：
* ExecutionEnvironment：
* GroupedDataSet：
* JoinDataSet：
* PartitionSortedDataSet：
* SelectByMaxFunction：
* SelectByMinFunction：
* UnfinishedCoGroupOperation：
* UnfinishedKeyPairOperation：
