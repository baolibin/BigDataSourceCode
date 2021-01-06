
##### Spark中的Shuffle简介？
    Spark作业主要的性能消耗在shuffle阶段，因为该环节包含大量的磁盘IO、序列化、网络传输等操作。
    
    Shuffle Write阶段：
    主要在一个stage计算结束后，为后续依赖该stage生成的新stage而发生的数据重新分配问题。
    会将每个map task处理生成的数据按照key来进行分类。


##### Spark2.2中的Shuffle Writer？
    Spark中的3种Shuffle写，以及对应的Shuffle Handle分别是：
    BypassMergeSortShuffleWriter：BypassMergeSortShuffleHandle
    UnsafeShuffleWriter：SerializedShuffleHandle
    SortShuffleWriter：BaseShuffleHandle


