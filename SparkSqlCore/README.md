#### org.apache.spark sql源码阅读
    SparkSQL模块源码阅读，版本2.2。

-----
##### 1、核心SparkSQL功能模块源码
> [核心SparkSQL功能模块源码](src/main/scala/org/apache/spark/sql)： 允许执行关系查询，包括使用Spark在SQL中表示的查询。
* Column：基于“DataFrame”中的数据计算的列。
* DataFrameNaFunctions：用于处理“DataFrame”中缺少的数据的功能。
* DataFrameReader：用于从外部存储系统（如文件系统、键值存储等）加载[[Dataset]]的接口。
* DataFrameStatFunctions：“DataFrame”的统计函数。
* DataFrameWriter：用于将[[Dataset]]写入外部存储系统（如文件系统、键值存储等）的接口。
* Dataset：Dataset是特定领域的对象的强类型集合，可以使用函数或关系操作进行并行转换。每个数据集还有一个称为“DataFrame”的非类型化视图，它是[[Row]]的数据集。
* DatasetHolder：[[Dataset]]的容器，用于Scala中的隐式转换。
* ExperimentalMethods：最勇敢的实验方法的持有者。我们不能保证方法的二进制兼容性和源代码兼容性的稳定性。
* ForeachWriter：使用“StreamingQuery”生成的数据的类。通常，这用于将生成的数据发送到外部系统。
* functions：可用于DataFrame操作的函数。
* KeyValueGroupedDataset：[[Dataset]]已按用户指定的分组键进行逻辑分组。
* RelationalGroupedDataset：在“DataFrame”上进行聚合的一组方法，由`Dataset.groupBy`。
* RuntimeConfig：Spark的运行时配置接口。要访问此文件，请使用`SparkSession.conf文件`。
* SparkSession：使用Dataset和DataFrameAPI编程Spark的入口点。
* SparkSessionExtensions：
* SQLContext：Spark 1.x中处理结构化数据（行和列）的入口点。
* SQLImplicits：用于将常见Scala对象转换为[[Dataset]]的隐式方法的集合。
* UDFRegistration：用于注册用户定义函数的函数。

-----
##### 2、Util模块源码
> [核心SparkSQL功能模块源码](src/main/scala/org/apache/spark/sql/util)：查询异常监听器。
* QueryExecutionListener：查询执行监听器的接口，可用于分析执行度量。

-----
##### 3、Expressions功能模块源码
> [expressions功能模块源码](src/main/scala/org/apache/spark/sql/expressions):
* scalalang
    - typed：Scala中可用于“Dataset”操作的类型安全函数。
* Aggregator：于用户定义聚合的基类，可在“数据集”操作中使用，以获取组中的所有元素并将其缩减为单个值。
* ReduceAggregator：一种聚合器，它使用一个结合的和可交换的归约函数。此reduce函数可用于遍历所有输入值，并将它们缩减为单个值。
* UserDefinedAggregateFunction(udaf)：实现用户定义聚合函数UDAF(user-defined aggregate functions)的基类。
* UserDefinedFunction：用户定义的函数。要创建一个，请使用“functions”中的“udf”函数。
* Window：用于在DataFrames中定义窗口的实用函数。
* WindowSpec：定义分区、排序和frame边界的窗口规范。



