
#### spark core源码阅读
##### 1、Deploy模块源码
> [deploy](src/main/scala/org/apache/spark/deploy)

##### 2、Executor模块源码
> [executor](src/main/scala/org/apache/spark/executor)

##### 3、Memory模块源码
> [memory](src/main/scala/org/apache/spark/memory)  
>> 这个软件包实现了Spark的内存管理系统。这个系统由两个主要组件组成，一个JVM范围的内存管理器和一个每个任务的内存管理器。
* MemoryManager：一种抽象内存管理器，用于施行execution和storage之间如何共享内存。
* StaticMemoryManager：静态内存管理，它将堆空间静态地划分为不相交的区域。一种[[MemoryManager]]。
* UnifiedMemoryManager：同一内存管理，它在执行和存储之间强制一个软边界，这样任何一方都可以从另一方借用内存。一种[[MemoryManager]]。
* MemoryPool：管理可调整大小的内存区域的簿记。此类是[[MemoryManager]]的内部类。有关详细信息，请参见子类。
* StorageMemoryPool：为管理用于存储（缓存）的可调整大小的内存池执行簿记。
* ExecutionMemoryPool：实现策略和簿记，以便在任务之间共享大小可调的内存池。

##### 4、Scheduler模块源码
> [deploy](src/main/scala/org/apache/spark/scheduler)

##### 5、Shuffle模块源码
> [shuffle](src/main/scala/org/apache/spark/shuffle)

##### 6、Storage模块源码
> [storage](src/main/scala/org/apache/spark/storage)



