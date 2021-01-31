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
> [utils模块源码](src/main/scala/org/apache/flink/api/scala/utils)：此类提供了简单的实用程序方法，用于使用索引或唯一标识符压缩数据集中的元素，从数据集中采样元素。

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
* CrossDataSet：由“交叉”操作产生的特定[[DataSet]]。默认交叉的结果是一个元组，包含笛卡尔积两边的两个值。
* DataSet：数据集，Flink的基本抽象。这表示特定类型“T”的元素集合。此类中的操作可用于创建新数据集和合并两个数据集。[[ExecutionEnvironment]]的方法可用于从外部源（如HDFS中的文件）创建数据集。“write*`方法可用于将元素写入存储器。
* ExecutionEnvironment：ExecutionEnvironment是执行程序的上下文。本地环境将导致在当前JVM中执行，远程环境将导致在远程集群安装上执行。
* GroupedDataSet：添加了分组键的[[DataSet]]。对具有相同键（“aggregate”、“reduce”和“reduceGroup”）的元素组进行操作。
* JoinDataSet：由“join”操作产生的特定[[DataSet]]。默认联接的结果是一个元组，其中包含联接两侧的两个值。可以通过使用“apply”方法指定自定义联接函数或提供[[RichFlatJoinFunction]]来更改联接的结果。
* PartitionSortedDataSet：结果[[DataSet.sortPartition]]. 这可用于将其他排序字段附加到“一个排序分区”操作符。
* SelectByMaxFunction：选择ByMaxFunction可使用Scala元组。
* SelectByMinFunction：选择ByMinFunction可使用Scala元组。
* UnfinishedCoGroupOperation：一个未完成的coGroup操作，由[[DataSet.coGroup]]必须首先使用“where”然后使用“equalTo”指定左右两侧的键。
* UnfinishedKeyPairOperation：这是为了处理需要键和使用流畅接口（现在是join和coGroup）的操作。对于每个操作，我们需要一个实现“finish”的子类来使用提供的键创建实际操作。
