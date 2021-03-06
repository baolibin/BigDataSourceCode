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
    - StandaloneAppClient：允许应用程序与Spark独立群集管理器通信的接口。
    - StandaloneAppClientListener：在发生各种事件时由部署客户端调用的回调。
* history：
    - ApplicationCache：应用程序缓存。
    - ApplicationHistoryProvider：调用“getAppUI（）”返回的所有信息：新UI和任何必需的更新状态。
    - FsHistoryProvider：从文件系统中存储的事件日志中提供应用程序历史记录的类。此提供程序定期在后台检查新完成的应用程序，并通过解析关联的事件日志来呈现历史应用程序UI。
    - HistoryPage：
    - HistoryServer：一种web服务器，用于呈现已完成应用程序的SparkUIs。
    - HistoryServerArguments：HistoryServer命令行解析。
* master：
    - ApplicationInfo：
    - ApplicationSource：
    - ApplicationState：
    - DriverInfo：
    - DriverState：
    - ExecutorDesc：
    - FileSystemPersistenceEngine：将数据存储在单个磁盘目录中，每个应用程序和辅助进程有一个文件。删除应用程序和辅助进程时，文件将被删除。
    - LeaderElectionAgent：LeaderElectionAgent跟踪当前主代理，是所有选举代理的公共接口。
    - Master：
    - MasterArguments：主程序的命令行解析器。
    - MasterMessages：包含仅由主机及其关联实体看到的消息。
    - MasterSource：
    - PersistenceEngine：允许主服务器保持从故障中恢复所需的任何状态。
    - RecoveryModeFactory：这个类的实现可以作为Spark的独立模式的恢复模式替代。
    - RecoveryState：
    - WorkerInfo：
    - WorkerState：
    - ZooKeeperLeaderElectionAgent：
    - ZooKeeperPersistenceEngine：
* rest：
    - RestSubmissionClient：向[[RestSubmissionServer]]提交应用程序的客户端。
    - RestSubmissionServer：响应[[RestSubmissionClient]]提交的请求的服务器。
    - StandaloneRestServer：响应[[RestSubmissionClient]]提交的请求的服务器。
    - SubmitRestProtocolException：REST应用程序提交协议中引发异常。
    - SubmitRestProtocolMessage：在REST应用程序提交协议中交换的抽象消息。
    - SubmitRestProtocolRequest：在REST应用程序提交协议中从客户端发送的抽象请求。
    - SubmitRestProtocolResponse：在REST应用程序提交协议中从服务器发送的抽象响应。
* worker：
    - CommandUtils：使用spark类路径运行命令的实用工具。
    - DriverRunner：管理一个驱动程序的执行，包括在发生故障时自动重新启动驱动程序。
    - DriverWrapper：用于启动驱动程序以便它们与工作进程共享命运的实用对象。
    - ExecutorRunner：管理一个executor进程的执行。
    - Worker：
    - WorkerArguments：worker进程的命令行分析器。
    - WorkerSource：
    - WorkerWatcher：连接到工作进程并在连接断开时终止JVM的端点。
* ApplicationDescription：应用程序描述。
* Client：在standalone cluster模式中，启动和终止执行程序的驱动器。
* ClientArguments：驱动程序客户端的命令行解析器。
* Command：
* DeployMessages：包含在调度程序终结点节点之间发送的消息。
* DriverDescription：
* ExecutorDescription：用于将Executors的状态从Worker发送到Master。此状态足以让Master在故障转移期间重建其内部数据结构。
* ExecutorState：executor状态
* ExternalShuffleService：提供Executors可以从中读取shuffle文件的服务器（而不是直接互相读取）。
* ExternalShuffleServiceSource：为外部shuffle服务提供度量源。
* FaultToleranceTest：这个套件测试Spark独立调度器的容错性。
* JsonProtocol：
* LocalSparkCluster：在集群中创建Spark独立进程的测试类。
* PythonRunner：用于启动Python应用程序的主类。它将python作为子进程执行，然后将其连接回JVM以访问系统属性等。
* RPackageUtils：
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
* FixedLengthBinaryInputFormat：用于读取和拆分包含记录的平面二进制文件的自定义输入格式，每个记录的大小以字节为单位固定。固定记录大小是通过Hadoop配置中的参数recordLength指定的。
* FixedLengthBinaryRecordReader：FixedLengthBinaryRecordReader由FixedLengthBinaryInputFormat返回。它使用FixedLengthBinaryInputFormat中设置的记录长度从给定的InputSplit一次读取一条记录。
* PortableDataStream：一个类，通过在需要读取数据流之前不创建数据流，可以对数据流进行序列化和移动。
* WholeTextFileInputFormat：用于读取全文文件。每个文件都作为键-值对读取，其中键是文件路径，值是文件的整个内容。
* WholeTextFileRecordReader：A[[org.apache.hadoop.mapreduce.RecordReader RecordReader]]用于以键-值对的形式读取单个全文文件，其中键是文件路径，值是文件的整个内容。

-----
##### 7、internal模块源码
> [internal模块源码](src/main/scala/org/apache/spark/internal)：
* config：
    - ConfigBuilder：Spark配置的基本生成器。提供用于创建特定类型生成器的方法。
    - ConfigEntry：一个entry包含配置的所有元信息。
    - ConfigProvider：配置值的来源。
    - ConfigReader：用于读取配置项和执行变量替换的帮助程序类。
* io：
    - FileCommitProtocol：定义单个Spark作业如何提交其输出的接口。
    - HadoopMapReduceCommitProtocol：由底层Hadoop OutputCommitter支持的[[FileCommitProtocol]]实现。
    - SparkHadoopMapReduceWriter：使用Hadoop OutputFormat保存RDD的helper对象。
    - SparkHadoopWriter：使用Hadoop OutputFormat保存RDD的内部帮助程序类。
    - SparkHadoopWriterUtils：一个helper对象，提供使用Hadoop OutputFormat保存RDD时使用的公共util（来自旧的mapredapi和新的mapreduceapi）。
* Logging：要记录数据的类的实用特性。为类创建SLF4J记录器，并允许使用仅在启用日志级别时才延迟计算参数的方法在不同级别记录消息。

-----
##### 8、io源码
> [io源码](src/main/scala/org/apache/spark/io)：用于压缩的IO编解码器。
* CompressionCodec：CompressionCodec允许定制选择块存储中使用的不同压缩实现。

-----
##### 9、launcher源码
> [launcher源码](src/main/scala/org/apache/spark/launcher)：
* LauncherBackend：
* SparkSubmitArgumentsParser：
* WorkerCommandBuilder：

-----
##### 10、mapred源码
> [mapred源码](src/main/scala/org/apache/spark/mapred)：
* SparkHadoopMapRedUtil：提交任务输出。在提交任务输出之前，我们需要知道是否有其他任务试图快速提交相同的输出分区。因此，与驱动程序协调，以确定此尝试是否可以提交（有关详细信息，请参阅SPARK-4879）。

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
* netty：
    - NettyBlockRpcServer：只需为每个请求的块注册一个块，就可以为打开块的请求提供服务。处理打开和上传任意BlockManager块。
    - NettyBlockTransferService：一种BlockTransferService，使用Netty一次获取一组块。
    - SparkTransportConf：提供一个实用程序，用于将Spark JVM（例如，执行器、驱动程序或独立的shuffle服务）中的SparkConf转换为TransportConf，其中包含有关环境的详细信息，如分配给此JVM的内核数。
* BlockDataManager：块数据管理。
* BlockTransferService：块传输服务

-----
##### 14、partial源码
> [partial源码](src/main/scala/org/apache/spark/partial)：支持近似结果。这为近似计算提供了方便的api和实现。
* ApproximateActionListener：用于近似单个结果操作的JobListener，例如count（）或non-parallel reduce（）。此侦听器最多等待超时毫秒，并将返回部分答案，即使此时无法获得完整答案。
* ApproximateEvaluator：一种对象，通过合并多个任务的U型结果来递增地计算函数。通过调用currentResult（）允许在任意点进行部分求值。
* BoundedDouble：带有误差线和相关置信度的双精度值。
* CountEvaluator：计数的近似值。
* GroupedCountEvaluator：按键计数的近似值。返回key到置信区间的映射。
* MeanEvaluator：平均数的近似估计者。
* PartialResult：部分结果。
* SumEvaluator：求和的近似值。它估计平均数和计数并将它们相乘，然后使用两个独立随机变量的方差公式得到结果的方差并计算置信区间。

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
    - Dispatcher：消息调度器，负责将RPC消息路由到适当的端点。
    - Inbox：存储[[RpcEndpoint]]消息并安全地将消息发布到该线程的收件箱。
    - NettyRpcCallContext：
    - NettyRpcEnv：
    - NettyStreamManager：StreamManager实现，用于从nettyrpconv提供文件服务。
    - Outbox：
    - RpcEndpointVerifier：用于远程[[RpcEnv]]查询“RpcEndpoint”是否存在的[[RpcEndpoint]]。
* RpcAddress：RPC环境的地址，带有主机名和端口。
* RpcCallContext：[[RpcEndpoint]]可以用来发回消息或失败的回调。它是线程安全的，可以在任何线程中调用。
* RpcEndpoint：RPC的一个端点，用于定义给定消息要触发的函数。
* RpcEndpointAddress：RPC终结点的地址标识符。
* RpcEndpointNotFoundException：Cannot find endpoint Exception。
* RpcEndpointRef：远程[[RpcEndpoint]]的引用。[[RpcEndpointRef]]是线程安全的。
* RpcEnv：RpcEnv实现必须具有具有空构造函数的[[RpcEnvFactory]]实现，以便可以通过反射创建它。
* RpcEnvStoppedException：RpcEnv stopped Exception。
* RpcTimeout：将超时与描述相关联，以便在发生TimeoutException时，可以将有关超时的其他上下文修改为异常消息。

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
* CryptoStreamUtils：用于操作IO加密和解密流的util类。
* GroupMappingServiceProvider：这个Spark特性用于将给定的用户名映射到它所属的一组。
* ShellBasedGroupsMappingProvider：此类负责在基于Unix的环境中获取特定用户的组。

-----
##### 19、serializer源码
> [serializer源码](src/main/scala/org/apache/spark/serializer)：用于RDD和无序数据的可插入序列化程序。
* GenericAvroSerializer：用于通用Avro记录的自定义序列化程序。
* JavaSerializer：使用Java内置序列化的Spark序列化程序。
* KryoSerializer：使用Kryo序列化库的Spark序列化程序。
* SerializationDebugger：
* Serializer：序列化程序。因为有些序列化库不是线程安全的，所以这个类用于创建[[org.apache.spark.serializer.SerializerInstance]]执行实际序列化并保证一次只能从一个线程调用的对象。
* SerializerManager：为各种Spark组件配置序列化、压缩和加密的组件，包括自动选择要用于无序排列的[[Serializer]]。

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
        - AppendOnlyMap：一个简单的开放哈希表，针对只附加的用例进行了优化，其中键永远不会被删除，但每个键的值可能会被更改。
        - BitSet：一个简单的，固定大小的位集实现。这种实现速度很快，因为它避免了安全/绑定检查。
        - CompactBuffer：一种仅附加的缓冲区，类似于ArrayBuffer，但是对于小的缓冲区内存效率更高。
        - ExternalAppendOnlyMap：一种仅附加的映射，当磁盘空间不足时将已排序的内容溢出到磁盘。
        - ExternalSorter：排序并潜在地合并（K，V）类型的多个键值对，以生成（K，C）类型的键组合器对。
        - MedianHeap：MedianHeap设计用于快速跟踪可能包含重复项的一组数字的中间值。插入一个新数字的时间复杂度为O（logn），而确定中值的时间复杂度为O（1）。
        - OpenHashMap：可为空键的快速哈希map实现。此哈希映射支持插入和更新，但不支持删除。这个map比java.util.HashMap快5倍，同时使用更少的空间开销。
        - OpenHashSet：一个简单、快速的哈希集，针对非空插入用例进行了优化，其中键永远不会被删除。
        - PartitionedAppendOnlyMap：writeablepartitionedpaircollection的实现，它包装了一个映射，其中键是（partition ID，K）的元组。
        - PartitionedPairBuffer：只追加键值对的缓冲区，每个缓冲区都有一个相应的分区ID，用于跟踪其估计大小（字节）。
        - PrimitiveKeyOpenHashMap：一个快速的散列映射实现原语，非空键。此哈希映射支持插入和更新，但不支持删除。
        - PrimitiveVector：一个仅附加的、非线程安全的、支持数组的向量，它针对基元类型进行了优化。
        - SizeTracker：集合的通用接口，用于跟踪以字节为单位的估计大小。
        - SizeTrackingAppendOnlyMap：一种仅附加的映射，它以字节为单位跟踪其估计大小。
        - SizeTrackingVector：一种仅附加的缓冲区，用于跟踪以字节为单位的估计大小。
        - SortDataFormat：对数据的任意输入缓冲区进行排序的抽象。此接口需要确定给定元素索引的排序键，以及交换元素和将数据从一个缓冲区移动到另一个缓冲区。
        - Sorter：Java实现[[TimSort]]上的简单包装器。Java实现是包私有的，因此不能在包外调用它org.apache.spark网站.使用集合. 这是一个可用于spark的简单包装。
        - Spillable：当超过内存阈值时，将内存中集合的内容溢出到磁盘。
        - Utils：集合的实用函数。
        - WritablePartitionedPairCollection：用于跟踪键值对集合大小的公共接口。
    - io：
        - ChunkedByteBuffer：只读字节缓冲区，物理上存储为多个块而不是单个连续数组。
        - ChunkedByteBufferOutputStream：写入固定大小字节数组块的输出流。
    - logging：
        - FileAppender：连续地将输入流中的数据附加到给定的文件中。
        - RollingFileAppender：连续地将输入流中的数据追加到给定的文件中，并在给定的时间间隔后对该文件进行滚动。滚动文件是基于给定的模式命名的。
        - RollingPolicy：定义所基于的策略[[org.apache.spark.util.logging.RollingFileAppender]]将生成滚动文件。
    - random：随机数生成实用程序。
       - Pseudorandom：具有伪随机行为的类。
        - RandomSampler：伪随机采样器。可以更改采样项目类型。例如，我们可能希望为分层抽样或重要性抽样添加权重。应该只使用绑定到采样器并且不能在采样后应用的转换。
        - SamplingUtils：
        - StratifiedSamplingUtils：PairRDDFunctions中sampleByKey方法的辅助函数和数据结构。
        - XORShiftRandom：这个类实现了一个XORShift随机数生成器算法。
* AccumulatorV2：累加器的基类，它可以累加“IN”类型的输入，并产生“OUT”类型的输出。
* Benchmark：用于基准组件的实用程序类。
* BoundedPriorityQueue：有界优先级队列。这个类包装了原始的PriorityQueue类并对其进行了修改，以便只保留前K个元素。前K个元素由隐式排序[A]定义。
* ByteBufferInputStream：从ByteBuffer读取数据。
* ByteBufferOutputStream：提供零拷贝方式将ByteArrayOutputStream中的数据转换为ByteBuffer。
* CausedBy：用于提取错误根本原因的提取器对象。
* Clock：表示时钟的接口，以便在单元测试中模拟它们。
* ClosureCleaner：一种使闭包可序列化（如果可以安全地进行）的清理程序。
* CollectionsUtils：
* CommandLineUtils：包含基本的命令行解析功能和方法来解析一些常见的Spark CLI选项。
* CompletionIterator：一个迭代器的包装器，它在成功遍历所有元素后调用一个完成方法。
* Distribution：用于从数值的小样本中获取一些统计信息工具，并带有一些方便的摘要函数。
* EventLoop：从调用者接收事件并处理事件线程中所有事件的事件循环。它将启动一个独占事件线程来处理所有事件。
* IdGenerator：用于获取唯一生成ID的util。这是围绕Java的AtomicInteger的包装。一个示例用法是在BlockManager中，每个BlockManager实例将启动一个RpcEndpoint，我们使用这个实用程序来分配RpcEndpoints的唯一名称。
* IntParam：用于将字符串解析为整数的提取器对象。
* JsonProtocol：将SparkListener事件序列化到JSON或从JSON序列化。
* ListenerBus：向侦听器发布事件的事件总线。
* ManualClock：一种可以手动设置和修改时间的时钟。它报告的时间不会随着时间的流逝而改变，只会随着调用者修改它的时间而改变。这主要用于测试。
* MemoryParam：用于将JVM内存字符串（如“10g”）解析为表示兆字节数的Int的提取器对象。支持与相同的格式实用程序。内存字符串。
* MutablePair：由两个元素组成的元组。当我们想要最小化对象分配时，这可以作为Scala的Tuple2的替代方案。
* MutableURLClassLoader：公开URLClassLoader中“addURL”和“geturl”方法的URL类装入器。
* NextIterator：提供基本/样板迭代器实现。
* ParentClassLoader：使类加载器中的某些受保护方法可访问的类加载器。
* PeriodicCheckpointer：这种抽象有助于持久化和检查点rdd以及从rdd派生的类型（如图形和数据帧）。
* RpcUtils：
* SerializableBuffer：一个包裹java.nio.ByteBuffer文件它可以通过Java序列化来序列化，以便在case类消息中更容易地传递ByteBuffers。
* SerializableConfiguration：
* SerializableJobConf：
* ShutdownHookManager：Spark使用的各种实用方法。
* SignalUtils：包含用于处理posix信号的实用程序。
* SizeEstimator：估计Java对象的大小（它们占用的内存字节数），以便在内存感知缓存中使用。
* SparkExitCode：
* SparkUncaughtExceptionHandler：执行器的默认未捕获异常处理程序终止整个进程，以避免无限期地进入不良状态。由于执行器相对较轻，所以在出现问题时最好快速失败。
* StatCounter：一个类，用于跟踪一组数字（计数、均值和方差）的统计信息。包括对合并两个statcounter的支持。
* TaskListeners：侦听器提供了一个回调函数，以便在任务执行完成时调用。
* ThreadStackTrace：用于将每个线程的堆栈跟踪从执行器传送到驱动程序。
* ThreadUtils：
* TimeStampedHashMap：这是的自定义实现scala.collection.mutable.Map，它将插入时间戳与每个键值对一起存储。
* UninterruptibleThread：一种特殊的线程，它提供“不间断地运行”以允许运行代码而不被中断`线程中断()`. 如果`线程中断（）`在运行期间被调用不间断地运行，它不会设置中断状态。相反，设置中断状态将被延迟，直到它从“rununtinruptibly”返回。
* Utils：Spark使用的各种实用方法。
* VersionUtils：用于处理Spark版本字符串的实用程序。
