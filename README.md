
#### spark core源码阅读
##### 1、Deploy模块源码
> [deploy源码](src/main/scala/org/apache/spark/deploy)

-----
##### 2、Executor模块源码
> [executor源码](src/main/scala/org/apache/spark/executor)

-----
##### 3、Memory模块源码
> [memory源码](src/main/scala/org/apache/spark/memory)：
>**这个软件包实现了Spark的内存管理系统。这个系统由两个主要组件组成，一个JVM范围的内存管理器和一个每个任务的内存管理器。**
* MemoryManager：一种抽象内存管理器，用于施行execution和storage之间如何共享内存。
    - StaticMemoryManager：静态内存管理，它将堆空间静态地划分为不相交的区域。一种[[MemoryManager]]。
    - UnifiedMemoryManager：同一内存管理，它在执行和存储之间强制一个软边界，这样任何一方都可以从另一方借用内存。一种[[MemoryManager]]。
* MemoryPool：管理可调整大小的内存区域的簿记。此类是[[MemoryManager]]的内部类。有关详细信息，请参见子类。
    - StorageMemoryPool：为管理用于存储（缓存）的可调整大小的内存池执行簿记。
    - ExecutionMemoryPool：实现策略和簿记，以便在任务之间共享大小可调的内存池。
    
-----
##### 4、Scheduler模块源码
> [deploy源码](src/main/scala/org/apache/spark/scheduler)

-----
##### 5、Shuffle模块源码
> [shuffle源码](src/main/scala/org/apache/spark/shuffle)

* ShuffleManager：shuffle系统的可插拔接口。在SparkEnv中，在driver和每个executor上创建一个ShuffleManager。
基于spark.shuffle.manager设置，driver向它注册shuffle，executors（或在driver中本地运行的任务）可以请求读写数据。
    - SortShuffleManager：在基于排序的shuffle中，传入的记录根据其目标分区ID进行排序，然后写入单个映射输出文件。
Reducers获取此文件的连续区域，以便读取其映射输出部分。
在map输出数据太大而无法放入内存的情况下，经过排序的输出子集可能会溢出到磁盘，而磁盘文件上的子集则会合并以生成最终的输出文件。
* ShuffleWriter：在一个map任务中向shuffle系统写出记录。
    - SortShuffleWriter：
    - UnsafeShuffleWriter：
    - BypassMergeSortShuffleWriter：
* ShuffleReader：在reduce任务中获得，用于从映射器读取组合记录。
    - BlockStoreShuffleReader：通过从其他节点的块存储请求，从shuffle中获取并读取范围[startPartition，endPartition]中的分区。
* ShuffleHandle：shuffle的一种不透明句柄，洗牌管理器使用它将信息传递给任务。
    -BaseShuffleHandle：一个基本的shufleHandle实现，它只捕获registerShuffle的参数。
* ShuffleBlockResolver：这个特性的实现者理解如何为逻辑shuffle块标识符（即map、reduce和shuffle）检索块数据。
    - IndexShuffleBlockResolver：创建并维护逻辑块和物理文件位置之间的无序块映射。
    来自同一映射任务的随机块的数据存储在单个合并数据文件中。数据文件中数据块的偏移量存储在单独的索引文件中。
* FetchFailedException：无法获取shuffle块。执行器捕获这个异常并将其传播回DAGScheduler（通过TaskEndReason），以便我们重新提交上一阶段。

-----
##### 6、Storage模块源码
> [storage源码](src/main/scala/org/apache/spark/storage)



