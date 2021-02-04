#### org.apache.spark core源码阅读
    SparkCore模块源码阅读，版本2.2。
    包括部署Deploy模块、执行Executor模块、内存Memory模块、调度Scheduler模块、经典的Shuffle模块、存储Storage模块等等。

-----
##### 1、核心Spark功能模块源码
> [核心Spark功能模块源码](src/main/scala/org/apache/spark)： 核心Spark功能，[[org.apache.org.apache.spark.SparkContext]]是Spark的主要入口，而[[org.apache.org.apache.spark.rdd。rdd]]表示分布式集合的数据类型，并提供大多数并行操作。
* Accumulable：一种可以累加的数据类型，即有一个可交换的和相联的“加法”运算，但结果类型“R”可能与所加的元素类型“T”不同。该操作不是线程安全的。 
* Accumulator：一个更简单的值[[Accumulable]]，其中累加的结果类型与合并的元素类型相同，即仅通过关联和交换操作“添加”到的变量，因此可以有效地并行支持。
* Aggregator：用于聚合数据的一组函数。
* ContextCleaner：用于RDD、shuffle和广播状态的异步清理器。
* Dependency：依赖项的基类。
* ExecutorAllocationClient：与集群管理器通信以请求或终止executors的客户端。目前只支持YARN模式。
* ExecutorAllocationManager：基于工作负载动态分配和删除executors的代理。
* FutureAction：支持取消action结果的future。这是Scala Future接口的扩展，支持取消。
* HeartbeatReceiver：driver内部从executor端接受心跳信息。
* InternalAccumulator：与表示任务级度量的内部累加器有关的字段和方法的集合。
* InterruptibleIterator：围绕现有迭代器提供任务终止功能的迭代器。它通过检查[[TaskContext]]中的中断标志来工作。
* MapOutputStatistics：保存有关map阶段中输出大小的统计信息。将来可能成为DeveloperApi。
* MapOutputTracker：用于跟踪stage的map输出位置的类。这是抽象的，因为不同版本的MapOutputTracker（驱动程序和执行程序）使用不同的HashMap来存储其元数据。
* Partition：RDD中分区的标识符。
* Partitioner：定义了键值对RDD中的元素如何按键进行分区的一种对象。将每个键映射到一个分区ID，从0到“numPartitions-1”。
* SecurityManager：负责spark安全的类。一般来说，这个类应该由SparkEnv实例化，大多数组件应该从SparkEnv访问它。
* SparkConf：Spark应用程序的配置。用于将各种Spark参数设置为键值对。
* SparkContext：Spark功能的主要入口点。SparkContext表示到Spark集群的连接，可用于在该集群上创建RDD、累加器和广播变量。
* SparkEnv：保存正在运行的Spark实例（主实例或工作实例）的所有运行时环境对象，包括序列化程序、RpcEnv、块管理器、映射输出跟踪器等。
* SparkFiles：解析通过`SparkContext.addFile文件()`添加的文件的路径。
* SparkStatusTracker：用于监视job和stage进度的Low-level状态报告API。
* SSLOptions： SSLOptions类是SSL配置选项的通用容器。它提供了生成特定对象的方法，以便为不同的通信协议配置SSL。
* TaskContext：Task的上下文信息。
* TaskContextImpl：[[TaskContext]]实现。
* TaskEndReason： 任务结束的各种可能原因。low-level的TaskScheduler应该为“短暂”的失败重试几次任务，并且只报告需要重新提交一些旧阶段的失败。
* TaskKilledException：当任务被显式终止（即，预期任务失败）时引发异常。
* TaskNotSerializableException：无法序列化任务时引发异常。
* TaskState：task生命周期的6个状态。
* TestUtils：测试实用程序。包含在主代码库中，因为它被多个项目使用。

-----
##### 2、api模块源码
> [api模块源码](src/main/scala/org/apache/spark/api)：
* java：Spark Java编程API。
* python：Spark Python编程API。
* r：Spark R编程API。

-----
##### 3、broadcast模块源码
> [broadcast模块源码](src/main/scala/org/apache/spark/broadcast)：Spark的广播变量，用于向所有节点广播不可变的数据集。
* Broadcast：广播变量。广播变量允许程序员在每台机器上缓存一个只读变量，而不是将其副本与任务一起发送。例如，它们可以有效地为每个节点提供一个大型输入数据集的副本。Spark还尝试使用高效的广播算法来分配广播变量，以降低通信成本。
* BroadcastFactory：Spark中所有广播实现的接口（允许多个广播实现）。SparkContext使用BroadcastFactory实现为整个Spark作业实例化特定广播。
* BroadcastManager：
* TorrentBroadcast：BitTorrent实现类似[[org.apache.spark.broadcast.Broadcast]].驱动程序将序列化对象分成小块，并将这些小块存储在驱动程序的BlockManager中。
* TorrentBroadcastFactory：A[[org.apache.spark.broadcast.Broadcast]]使用类似BitTorrent的协议将广播数据分布式传输到执行器的实现。

##### 4、Deploy模块源码
> [deploy源码](src/main/scala/org/apache/spark/deploy)
* client：
    - StandaloneAppClient：
    - StandaloneAppClientListener：
* history：
* master：
    - ApplicationInfo：
    - ApplicationSource：
    - ApplicationState：
    - DriverInfo：
    - DriverState：
    - ExecutorDesc：
    - FileSystemPersistenceEngine：
    - LeaderElectionAgent：
    - Master：
    - MasterArguments：
    - MasterMessages：
    - MasterSource：
    - PersistenceEngine：
    - RecoveryModeFactory：
    - RecoveryState：
    - WorkerInfo：
    - WorkerState：
    - ZooKeeperLeaderElectionAgent：
    - ZooKeeperPersistenceEngine：
* rest：
* worker：
    - CommandUtils：
    - DriverRunner：
    - DriverWrapper：
    - ExecutorRunner：
    - Worker：
    - WorkerArguments：
    - WorkerSource：
    - WorkerWatcher：
* 
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
##### 5、Executor模块源码
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
##### 6、input模块源码
> [input模块源码](src/main/scala/org/apache/spark/input)：

-----
##### 7、internal模块源码
> [internal模块源码](src/main/scala/org/apache/spark/internal)：

-----
##### 8、io源码
> [io源码](src/main/scala/org/apache/spark/io)：

-----
##### 9、launcher源码
> [launcher源码](src/main/scala/org/apache/spark/launcher)：

-----
##### 10、mapred源码
> [mapred源码](src/main/scala/org/apache/spark/mapred)：

-----
##### 11、Memory模块源码
> [memory源码](src/main/scala/org/apache/spark/memory)：
>**这个软件包实现了Spark的内存管理系统。这个系统由两个主要组件组成，一个JVM范围的内存管理器和一个每个任务的内存管理器。**
* MemoryManager：一种抽象内存管理器，用于施行execution和storage之间如何共享内存。
    - StaticMemoryManager：静态内存管理，它将堆空间静态地划分为不相交的区域。一种[[MemoryManager]]。
    - UnifiedMemoryManager：同一内存管理，它在执行和存储之间强制一个软边界，这样任何一方都可以从另一方借用内存。一种[[MemoryManager]]。
* MemoryPool：管理可调整大小的内存区域的簿记。此类是[[MemoryManager]]的内部类。有关详细信息，请参见子类。
    - StorageMemoryPool：为管理用于存储（缓存）的可调整大小的内存池执行簿记。
    - ExecutionMemoryPool：实现策略和簿记，以便在任务之间共享大小可调的内存池。

-----
##### 12、metrics源码
> [metrics源码](src/main/scala/org/apache/spark/metrics)：

-----
##### 13、network源码
> [network源码](src/main/scala/org/apache/spark/network)：

-----
##### 14、partial源码
> [partial源码](src/main/scala/org/apache/spark/partial)：

-----
##### 15、RDD源码
> [RDD源码](src/main/scala/org/apache/spark/rdd)：提供各种RDD的实现。
* util：工具类
    - PeriodicRDDCheckpointer：这个类帮助持久化和检查点rdd。
* AsyncRDDActions：通过隐式转换提供的一组异步RDD操作。
* BinaryFileRDD：二进制文件RDD。
* BlockRDD：数据块RDD。
* CartesianRDD：笛卡尔积RDD。
* CheckpointRDD：从存储器中恢复检查点数据的RDD。
* PartitionCoalescer：PartitionCoalescer定义如何合并给定RDD的分区。
* CoalescedRDD：表示合并的RDD，该RDD的分区数少于其父RDD。
* CoGroupedRDD：把它的双亲组合在一起的RDD。对于父RDD中的每个键k，生成的RDD包含一个元组，其中包含该键的值列表。
* DoubleRDDFunctions：通过隐式转换在双精度RDD上提供的额外函数。
* EmptyRDD：没有分区和元素的RDD。
* HadoopRDD：一种RDD，提供使用旧的MapReduceAPI读取Hadoop中存储的数据。
* InputFileBlockHolder：它保存当前Spark任务的文件名。
* JdbcRDD：对JDBC连接执行SQL查询并读取结果的RDD。
* LocalCheckpointRDD：一种虚拟的检查点RDD，用于在故障期间提供信息性的错误消息。
* LocalRDDCheckpointData：在Spark的缓存层上实现的检查点的实现。
* MapPartitionsRDD：将提供的函数应用于父RDD的每个分区的RDD。
* NewHadoopRDD：一种RDD，使用新的MapReduceAPI提供读取存储在Hadoop中的数据。
* OrderedRDDFunctions：（键，值）对的RDD上可用的额外函数，其中键可通过隐式转换进行排序。
* PairRDDFunctions：通过隐式转换在（键，值）对的RDD上提供的额外函数。
* ParallelCollectionRDD：
* PartitionerAwareUnionRDD：类表示一个RDD，该RDD可以接受由同一个分区器分区的多个RDD，并在保留分区器的同时将它们统一为单个RDD。
* PartitionPruningRDD：一个RDD，用于修剪RDD分区，这样我们就可以避免在所有分区上启动任务。
* PartitionwiseSampledRDD：从其父RDD分区中采样的RDD。
* PipedRDD：一种RDD，它通过外部命令（每行打印一个）将每个父分区的内容通过管道传输，并将输出作为字符串集合返回。
* RDD：一个弹性分布式数据集（RDD），Spark中的基本抽象。表示可以并行操作的不可变的、分区的元素集合。
* RDDCheckpointData：此类包含与RDD检查点相关的所有信息。这个类的每个实例都与一个RDD相关联。它管理关联RDD的检查点过程，并通过提供检查点RDD的更新分区、迭代器和首选位置来管理检查点后状态。
* RDDOperationScope：表示实例化RDD的操作的通用命名代码块。
* ReliableCheckpointRDD：从以前写入可靠存储器的检查点文件中读取数据的一种RDD。
* ReliableRDDCheckpointData：将RDD数据写入可靠存储器的检查点实现。这允许驱动程序在先前计算的状态出现故障时重新启动。
* SequenceFileRDDFunctions：（键，值）对的RDD上提供了额外的函数，可以通过隐式转换创建Hadoop SequenceFile。
* ShuffledRDD：shuffle产生的RDD（例如数据的重新分区）。
* SubtractedRDD：集差/减法的cogroup优化版本。
* UnionRDD：并集RDD。
* WholeTextFileRDD：读入一堆文本文件的RDD，每个文本文件变成一条记录。
* ZippedPartitionsBaseRDD：
* ZippedWithIndexRDD：表示用元素索引压缩的RDD。排序首先基于分区索引，然后是每个分区内项目的排序。


-----
##### 16、rpc通信源码
> [rpc通信源码](src/main/scala/org/apache/spark/rpc)：
* netty：
    - Dispatcher：
    - Inbox：
    - NettyRpcCallContext：
    - NettyRpcEnv：
    - NettyStreamManager：
    - Outbox：
    - RpcEndpointVerifier：
* RpcAddress：
* RpcCallContext：
* RpcEndpoint：
* RpcEndpointAddress：
* RpcEndpointNotFoundException：
* RpcEndpointRef：
* RpcEnv：
* RpcEnvStoppedException：
* RpcTimeout：

-----
##### 17、scheduler模块源码
> [scheduler源码](src/main/scala/org/apache/spark/scheduler)：Spark的调度组件。这包括`DAGScheduler`以及lower level级别的`TaskScheduler`。
* cluster：
    - CoarseGrainedClusterMessage：粗粒度的集群消息。
    - CoarseGrainedSchedulerBackend：等待粗粒度executors连接的调度程序后端。
    - ExecutorData：粗粒度SchedulerBackend使用的executor的数据分组。
    - ExecutorInfo：存储有关要从调度程序传递到侦听器的执行器的信息。
    - StandaloneSchedulerBackend：Spark的独立群集管理器的[[SchedulerBackend]]实现。
* local：
    - LocalSchedulerBackend：运行Spark的本地版本时使用，其中executor、backend和master都在同一JVM中运行。它位于[[TaskSchedulerImpl]]后面，在本地运行的单个执行器（由[[LocalSchedulerBackend]]创建）上处理启动任务。
* AccumulableInfo：关于一个[[org.apache.org.apache.spark.Accumulable]]在task或stage中信息被修改。
* ActiveJob：DAGScheduler中正在运行的作业。作业可以有两种类型：结果作业（计算ResultState以执行操作）或map阶段作业（在提交任何下游阶段之前计算ShuffleMapStage的映射输出）。
* ApplicationEventListener：应用程序事件的简单侦听器。
* BlacklistTracker：BlacklistTracker设计用于跟踪有问题的执行者和节点。它支持将整个应用程序中的执行者和节点列入黑名单（定期过期）。
* DAGScheduler：实现面向stages调度的高级调度层。它为每个作业计算stages的DAG，跟踪哪些rdd和阶段输出被具体化，并找到运行作业的最小调度。
* DAGSchedulerEvent：DAGScheduler可以处理的事件类型。
* EventLoggingListener：将事件记录到持久存储的SparkListener。
* ExecutorFailuresInTaskSet：用于跟踪失败任务以将其列入黑名单的小助手。一个任务集中一个Executor上所有失败的信息。
* ExecutorLossReason：表示对executor或整个从属服务器失败或退出的解释。
* ExternalClusterManager：一个集群管理器接口用于插件外部调度程序。
* InputFormatInfo：解析并保存有关指定参数的inputFormat（和文件）的信息。
* JobListener：用于在将作业提交给调度程序后侦听作业完成或失败事件的接口。
* JobResult：DAGScheduler调度作业返回的结果。
* JobWaiter：等待调度程序作业完成的对象。当任务完成时，它将结果传递给给定的处理函数。
* LiveListenerBus：异步地将SparkListenerEvents传递给已注册的SparkListener。
* MapStatus：ShuffleMapTask返回给调度程序的结果。包括运行任务的块管理器地址以及每个reducer的输出大小，以便传递给reduce任务。
* OutputCommitCoordinator：决定任务是否可以将输出提交到HDFS的权限。使用“第一个提交者获胜”策略。
* ReplayListenerBus：一种SparkListenerBus，可用于从序列化事件数据重播事件。
* ResultStage：ResultStages在RDD的某些分区上应用函数来计算操作的结果。
* ResultTask：将输出发送回驱动程序应用程序的任务。
* Schedulable：可调度实体的接口。有两种类型的可调度实体（Pools和TaskSetManagers）。
* SchedulableBuilder：构建可调度tree的接口：构建树节点（池）addTaskSetManager：构建叶节点（TaskSetManagers）。
* SchedulerBackend：用于调度系统的后端接口，允许在TaskSchedulerImpl下插入不同的系统。
* SchedulingAlgorithm：排序算法接口，FIFO:TaskSetManagers之间的FIFO算法，FS：池之间的FS算法，以及池内的FIFO或FS。
* SchedulingMode：“FAIR”和“FIFO”确定哪个策略用于在可调度的子队列中排序任务，当可调度的子队列没有子队列时使用“NONE”。
* ShuffleMapStage：ShuffleMapStages是执行DAG中的中间阶段，它为一次shuffle操作生成数据。
* ShuffleMapTask：ShuffleMapTask将RDD的元素划分为多个存储桶（基于ShuffleDependency中指定的分区器）。
* SparkListener：“SparkListenerInterface”的默认实现，没有针对所有回调的op实现。请注意，这是一个内部接口，可能会在不同的Spark发布中发生变化。
* SparkListenerBus：将[[SparkListenerEvent]]中继到侦听器的[[SparkListenerEvent]]s总线。
* SplitInfo：有关特定拆分实例的信息：处理两个拆分实例。
* Stage：stage是一组并行任务，所有任务都需要计算运行相同的函数,作为Spark作业的一部分。其中所有任务都具有相同的shuffle依赖项。调度程序运行的每一个DAG任务在发生shuffle的边界处被分为多个阶段，然后DAGScheduler按拓扑顺序运行这些阶段。
* StageInfo：存储有关要从调度程序传递到侦听器的阶段的信息。
* StatsReportListener：简单的SparkListener，在每个阶段完成时记录一些摘要统计信息。
* Task：Task是执行的基本单元, 包括两种,ShuffleMapTask和ResultTask。
* TaskDescription：对传递给要执行的executors任务的描述，通常由`TaskSetManager.resourceOffer`。
* TaskInfo：有关任务集中正在运行的任务尝试的信息。
* TaskResultGetter：运行反序列化和远程获取（如果需要）任务结果的线程池。
* TaskScheduler：Low-level任务调度器接口，目前由[[TaskSchedulerImpl]]独家实施。
* TaskSchedulerImpl：通过SchedulerBackend为多种类型的集群调度任务。它还可以通过使用“LocalSchedulerBackend”并将isLocal设置为true来使用本地设置。它处理常见的逻辑，如确定作业之间的调度顺序、唤醒以启动推测性任务等。
* TaskSet：一起提交给low-level任务调度器的一组任务，通常表示特定阶段缺少的分区。
* TaskSetBlacklist：处理任务集中的黑名单执行者和节点。这包括黑名单特定（任务，执行者）/（任务，节点）对，也完全黑名单执行者和整个任务集的节点。
* TaskSetManager：在TaskSchedulerImpl的单个任务集中安排任务。此类跟踪每个任务，在任务失败时重试任务（次数有限），并通过延迟调度处理此任务集的位置感知调度。
* WorkerOffer：表示executor上可用的免费资源。

-----
##### 18、security源码
> [security源码](src/main/scala/org/apache/spark/security)：

-----
##### 19、serializer源码
> [serializer源码](src/main/scala/org/apache/spark/serializer)：

-----
##### 20、Shuffle模块源码
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
##### 21、status.api.v1源码
> [status.api.v1源码](src/main/scala/org/apache/spark/status/api/v1)：

-----
##### 22、Storage模块源码
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
##### 23、ui源码
> [ui源码](src/main/scala/org/apache/spark/ui)：

-----
##### 24、Util模块源码
> [util源码](src/main/scala/org/apache/spark/util)： Spark实用程序。
* util：
    - collection：
        - Sorter：Java实现[[TimSort]]上的简单包装器。Java实现是包私有的，因此不能在包外调用它org.apache.spark网站.使用集合. 这是一个可用于spark的简单包装。
        - Utils：集合的实用函数。
    - io：
        - ChunkedByteBuffer：只读字节缓冲区，物理上存储为多个块而不是单个连续数组。
    - logging：
        - FileAppender：连续地将输入流中的数据附加到给定的文件中。
    - random：
       - Pseudorandom：具有伪随机行为的类。



