
#### org.apache.spark sql源码阅读
    SparkSQL模块源码阅读，版本2.2。

-----
##### 1、核心SparkSQL功能模块源码
> [核心SparkSQL功能模块源码](src/main/scala/org/apache/spark)： 允许执行关系查询，包括使用Spark在SQL中表示的查询。
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
* ：
