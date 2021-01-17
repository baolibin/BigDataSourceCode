
#### spark core源码阅读
    SparkCore模块源码阅读，版本2.2。
    包括部署Deploy模块、执行Executor模块、内存Memory模块、调度Scheduler模块、经典的Shuffle模块、存储Storage模块等等。
##### 1、Deploy模块源码
> [deploy源码](src/main/scala/org/apache/spark/deploy)
* Client：在standalone cluster模式中，启动和终止执行程序的驱动器。
* ClientArguments：驱动程序客户端的命令行解析器。
* DeployMessages：包含在调度程序终结点节点之间发送的消息。
* ExecutorDescription：用于将Executors的状态从Worker发送到Master。此状态足以让Master在故障转移期间重建其内部数据结构。
* ExecutorState：executor状态
* ExternalShuffleService：提供Executors可以从中读取shuffle文件的服务器（而不是直接互相读取）。
* ExternalShuffleServiceSource：为外部shuffle服务提供度量源。
* FaultToleranceTest：这个套件测试Spark独立调度器的容错性。
* LocalSparkCluster：在集群中创建Spark独立进程的测试类。
* PythonRunner：用于启动Python应用程序的主类。它将python作为子进程执行，然后将其连接回JVM以访问系统属性等。
* RRunner：用于使用spark submit启动SparkR应用程序的主类。它将R作为子进程执行，然后将其连接回JVM以访问系统属性等。
* SparkCuratorUtil：Spark监护管理工具类。
* SparkHadoopUtil：包含从Spark与Hadoop交互的util方法。
* SparkSubmit：启动Spark应用程序的主要网关。这个程序负责设置具有相关Spark依赖项的类路径，并在Spark支持的不同集群管理器和部署模式上提供一个layer。
* SparkSubmitArguments：解析和封装spark submit脚本中的参数。env参数用于测试。

-----
##### 2、Executor模块源码
> [executor源码](src/main/scala/org/apache/spark/executor)：与各种集群管理器一起使用的Executor组件。
* CoarseGrainedExecutorBackend：在worker中为app启动executor的jvm进程
* CommitDeniedException：当任务尝试将输出提交到HDFS但被驱动程序拒绝时引发异常。
* Executor：Spark executor，由线程池支持以运行任务。
* ExecutorBackend：Executor用来向集群调度程序发送更新的可插入接口。
* ExecutorExitCode：假设集群管理框架可以捕获退出代码，那么这些退出代码应该被executor用来向主机提供关于执行者失败的信息。
* ExecutorSource：ExecutorSource统计Executor各性能的使用指标。
* InputMetrics：表示从外部系统读取数据的度量的累加器集合。
* OutputMetrics：一组累加器，表示向外部系统写入数据的度量。
* ShuffleReadMetrics：表示有关读取shuffle数据的度量的累加器集合。
* ShuffleWriteMetrics：一组累加器，表示有关写入shuffle数据的度量。
* TaskMetrics：此类是表示与任务关联的度量的内部累加器集合的包装器。

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
> [scheduler源码](src/main/scala/org/apache/spark/scheduler)

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
* MemoryStore：将块存储在内存中，可以是反序列化Java对象的数组，也可以是序列化ByteBuffers。
* BlockId：标识特定的数据块，通常与单个文件关联。
* BlockManager：在每个节点（驱动程序和执行程序）上运行的管理器，它提供接口，用于在本地和远程将块放入和检索到各种存储（内存、磁盘和堆外）。
请注意，在BlockManager可用之前必须调用[[initialize（）]]。
* BlockManagerId：此类表示块管理器的唯一标识符。
* BlockManagerManagedBuffer：此[[ManagedBuffer]]包装从[[BlockManager]]检索的[[BlockData]]实例，以便在释放此缓冲区的引用后，可以释放相应块的读锁。
* BlockInfoManager：[[BlockManager]]的组件，用于跟踪块的元数据并管理块锁定。
* BlockManagerMaster：
* BlockManagerMasterEndpoint：BlockManagerMasterEndpoint是主节点上的一个[[ThreadSafeRpcEndpoint]]，用于跟踪所有从节点的块管理器的状态。
* BlockManagerMessages：
* BlockManagerSlaveEndpoint：一个RpcEndpoint，从主控机接收命令以执行选项。例如，这用于从slave程序的BlockManager中删除块。
* BlockManagerSource：
* BlockReplicationPolicy：BlockReplicationPrioritization提供了为复制块的对等序列设置优先级的逻辑。
* BlockStatusListener：
* BlockUpdatedInfo：在块管理器中存储有关块状态的信息。
* DiskBlockManager：创建并维护逻辑块和磁盘上物理位置之间的逻辑映射。一个块映射到一个文件，其名称由其BlockId给定。
* DiskBlockObjectWriter：用于将JVM对象直接写入磁盘上的文件的类。此类允许将数据附加到现有块。
* DiskStore：在磁盘上存储BlockManager块。
* FileSegment：根据偏移量和长度引用文件的特定段（可能是整个文件）。
* RDDInfo：
* ShuffleBlockFetcherIterator：获取多个块的迭代器。对于本地块，它从本地块管理器获取。对于远程块，它使用提供的BlockTransferService获取它们。
* StorageLevel：用于控制RDD存储的标志。每个StorageLevel记录是使用内存还是ExternalBlockStore，如果RDD没有内存或ExternalBlockStore，
是否将数据以序列化格式保存在内存中，以及是否在多个节点上复制RDD分区。
* StorageStatusListener：维护executor存储状态的SparkListener。
* StorageStatus：每个BlockManager的存储信息。这个类假设BlockId和BlockStatus是不可变的，这样这个类的使用者就不能改变信息的来源。访问不是线程安全的。
* TopologyMapper：TopologyMapper提供给定主机的拓扑信息。

-----
##### 7、Util模块源码
> [storage源码](src/main/scala/org/apache/spark/util)： Spark实用程序。
1. util
    1. collection
        1. Sorter：Java实现[[TimSort]]上的简单包装器。Java实现是包私有的，因此不能在包外调用它org.apache.spark网站.使用集合. 这是一个可用于spark的简单包装。
        2. Utils：集合的实用函数。
    2. io
        1. ChunkedByteBuffer：只读字节缓冲区，物理上存储为多个块而不是单个连续数组。
    3. logging
        1. FileAppender：连续地将输入流中的数据附加到给定的文件中。
    4. random
        1. Pseudorandom：具有伪随机行为的类。

-----
##### 8、核心Spark功能模块源码
> [核心Spark功能模块源码](src/main/scala/org/apache/spark)： 核心Spark功能，[[org.apache.spark.SparkContext]]是Spark的主要入口，而[[org.apache.spark.rdd。rdd]]表示分布式集合的数据类型，并提供大多数并行操作。
* Accumulable：一种可以累加的数据类型，即有一个可交换的和相联的“加法”运算，但结果类型“R”可能与所加的元素类型“T”不同。该操作不是线程安全的。 
* Accumulator：一个更简单的值[[Accumulable]]，其中累加的结果类型与合并的元素类型相同，即仅通过关联和交换操作“添加”到的变量，因此可以有效地并行支持。
* Aggregator：用于聚合数据的一组函数。

