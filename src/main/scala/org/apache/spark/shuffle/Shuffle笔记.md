
##### Spark中的Shuffle简介？
    Spark作业主要的性能消耗在shuffle阶段，因为该环节包含大量的磁盘IO、序列化、网络传输等操作。
    
    Shuffle Write阶段：
    主要在一个stage计算结束后，为后续依赖该stage生成的新stage而发生的数据重新分配问题。
    在Spark 1.2以前，默认的shuffle计算引擎是 HashShuffleManager，会将每个map task处理生成的数据按照key来进行分类。
    对key执行hash算法，将相同的key写入同一个磁盘文件中，而每一个磁盘文件都只属于下游stage的一个reduce task。
    在将数据写入磁盘之前，会先将数据写入到环形内存缓冲区，达到阈值之后再溢写到磁盘。

    Shuffle Read阶段：
    生成一个新stage之前，该stage中每一个task会先将上一个stage的计算结果中的所有相同key，从各个节点通过网络拉取到自己节点上，
    然后对key进行聚合或链接操作，每个reduce task只需要从上游stage的所有task中，拉取属于自己的那一份磁盘文件即可。

##### ShuffleManager发展概述？
    ShuffleManager是在SparkContext上创建的驱动程序，可插拔。
    在每个executor上，基于spark.shuffle.manager设置。Driver使用它注册shuffle，executors可以请求读写数据。
    在Spark的源码中，负责shuffle过程的执行、计算和处理的组件主要就是ShuffleManager，也即shuffle管理器。
    
    在Spark 1.2以前，默认的shuffle计算引擎是HashShuffleManager。
    该ShuffleManager而HashShuffleManager有着一个非常严重的弊端，就是会产生大量的中间磁盘文件，进而由大量的磁盘IO操作影响了性能。
    
    在Spark 1.2以后的版本中，默认的ShuffleManager改成了SortShuffleManager。
    SortShuffleManager相较于HashShuffleManager来说，有了一定的改进。主要就在于，每个Task在进行shuffle操作时，
    虽然也会产生较多的临时磁盘文件，但是最后会将所有的临时文件合并（merge）成一个磁盘文件，因此每个Task就只有一个磁盘文件。
    在下一个stage的shuffle read task拉取自己的数据时，只要根据索引读取每个磁盘文件中的部分数据即可。

##### Spark2.2中的Shuffle Writer？
    Spark中的3种Shuffle写，以及对应的Shuffle Handle分别是：
    BypassMergeSortShuffleWriter：BypassMergeSortShuffleHandle
    UnsafeShuffleWriter：SerializedShuffleHandle
    SortShuffleWriter：BaseShuffleHandle


