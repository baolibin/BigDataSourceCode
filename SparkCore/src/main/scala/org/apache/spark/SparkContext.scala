/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io._
import java.lang.reflect.Constructor
import java.net.URI
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.{Arrays, Locale, Properties, ServiceLoader, UUID}

import com.google.common.collect.MapMaker
import org.apache.commons.lang3.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.{Utils => _, _}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, Job => NewHadoopJob}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.{LocalSparkCluster, SparkHadoopUtil}
import org.apache.spark.input.{FixedLengthBinaryInputFormat, PortableDataStream, StreamInputFormat, WholeTextFileInputFormat}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.partial.{ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, StandaloneSchedulerBackend}
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.storage.BlockManagerMessages.TriggerThreadDump
import org.apache.spark.storage._
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.ui.{ConsoleProgressBar, SparkUI}
import org.apache.spark.util._

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.generic.Growable
import scala.collection.mutable.HashMap
import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

/**
  * Spark功能的主要入口点。SparkContext表示与Spark群集的连接，可用于在该群集上创建RDD、累加器和广播变量。
  * 每个JVM只能有一个SparkContext处于活动状态。在创建新的SparkContext之前，必须“stop（）”激活SparkContext。
  *
  * Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
  * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
  *
  * Only one SparkContext may be active per JVM.  You must `stop()` the active SparkContext before
  * creating a new one.  This limitation may eventually be removed; see SPARK-2243 for more details.
  *
  * 描述应用程序配置的Spark Config对象。此配置中的任何设置都会覆盖默认配置以及系统属性。
  *
  * @param config a Spark Config object describing the application configuration. Any settings in
  *               this config overrides the default configs as well as system properties.
  */
class SparkContext(config: SparkConf) extends Logging {

    val startTime = System.currentTimeMillis()
    // 为运行SparkContext的用户设置SPARK USER。
    // Set SPARK_USER for user who is running SparkContext.
    val sparkUser = Utils.getCurrentUserName()

    // 为了防止多个SparkContext同时处于活动状态，请将此上下文标记为已开始构造。
    // In order to prevent multiple SparkContexts from being active at the same time, mark this
    // context as having started construction.
    // NOTE: this must be placed at the beginning of the SparkContext constructor.
    SparkContext.markPartiallyConstructed(this, allowMultipleContexts)
    // 线程局部变量，用户可以使用它将信息传递到堆栈中
    // Thread Local variable that can be used by users to pass information down the stack
    protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
        override protected def childValue(parent: Properties): Properties = {
            // Note: make a clone such that changes in the parent properties aren't reflected in
            // the those of the children threads, which has confusing semantics (SPARK-10563).
            SerializationUtils.clone(parent)
        }

        override protected def initialValue(): Properties = new Properties()
    }
    // 构建此SparkContext的调用站点。
    // The call site where this SparkContext was constructed.
    private val creationSite: CallSite = Utils.getCallSite()
    // 如果为true，则在多个SparkContext处于活动状态时记录警告，而不是引发异常
    // If true, log warnings instead of throwing exceptions when multiple SparkContexts are active
    private val allowMultipleContexts: Boolean =
    config.getBoolean("org.apache.spark.driver.allowMultipleContexts", false)
    private[spark] val stopped: AtomicBoolean = new AtomicBoolean(false)
    // Spark事件的异步侦听器总线
    // An asynchronous listener bus for Spark events
    private[spark] val listenerBus = new LiveListenerBus(this)
    // 用于存储每个静态文件/jar的URL以及文件的本地时间戳
    // Used to store a URL for each static file/jar together with the file's local timestamp
    private[spark] val addedFiles = new ConcurrentHashMap[String, Long]().asScala

    // 可以使用默认参数合并以下构造函数。
    // NOTE: The below constructors could be consolidated using default arguments. Due to
    // Scala bug SI-8479, however, this causes the compile step to fail when generating docs.
    // Until we have a good workaround for that bug the constructors remain broken out.
    private[spark] val addedJars = new ConcurrentHashMap[String, Long]().asScala
    // 跟踪所有持久化的RDD
    // Keeps track of all persisted RDDs
    private[spark] val persistentRdds = {
        val map: ConcurrentMap[Int, RDD[_]] = new MapMaker().weakValues().makeMap[Int, RDD[_]]()
        map.asScala
    }
    // 要传递给executors的环境变量。
    // Environment variables to pass to our executors.
    private[spark] val executorEnvs = HashMap[String, String]()

    // 在Spark驱动程序日志中注销Spark版本
    // log out Spark Version in Spark driver log
    logInfo(s"Running Spark version $SPARK_VERSION")

    warnDeprecatedVersions()

    // 私有变量。这些变量保持上下文的内部状态，外部世界无法访问它们。
    // 它们是可变的，因为我们希望提前将它们全部初始化为某个中立值，所以在构造函数仍在运行时调用“stop（）”是安全的。
    /* ------------------------------------------------------------------------------------- *
     | Private variables. These variables keep the internal state of the context, and are    |
     | not accessible by the outside world. They're mutable since we want to initialize all  |
     | of them to some neutral value ahead of time, so that calling "stop()" while the       |
     | constructor is still running is safe.                                                 |
     * ------------------------------------------------------------------------------------- */
    private val nextShuffleId = new AtomicInteger(0)
    private val nextRddId = new AtomicInteger(0)
    private var _conf: SparkConf = _
    private var _eventLogDir: Option[URI] = None
    private var _eventLogCodec: Option[String] = None
    private var _env: SparkEnv = _
    private var _jobProgressListener: JobProgressListener = _
    private var _statusTracker: SparkStatusTracker = _
    private var _progressBar: Option[ConsoleProgressBar] = None
    private var _ui: Option[SparkUI] = None
    private var _hadoopConfiguration: Configuration = _
    private var _executorMemory: Int = _
    private var _schedulerBackend: SchedulerBackend = _
    private var _taskScheduler: TaskScheduler = _
    private var _heartbeatReceiver: RpcEndpointRef = _
    @volatile private var _dagScheduler: DAGScheduler = _
    private var _applicationId: String = _
    private var _applicationAttemptId: Option[String] = None
    private var _eventLogger: Option[EventLoggingListener] = None
    private var _executorAllocationManager: Option[ExecutorAllocationManager] = None
    private var _cleaner: Option[ContextCleaner] = None
    private var _listenerBusStarted: Boolean = false
    private var _jars: Seq[String] = _

    // 访问器和公共字段。它们提供了对上下文内部状态的访问。
    /* ------------------------------------------------------------------------------------- *
     | Accessors and public fields. These provide access to the internal state of the        |
     | context.                                                                              |
     * ------------------------------------------------------------------------------------- */
    private var _files: Seq[String] = _
    private var _shutdownHookRef: AnyRef = _
    private[spark] var checkpointDir: Option[String] = None

    /**
      * 创建从系统属性加载设置的SparkContext。
      *
      * Create a SparkContext that loads settings from system properties (for instance, when
      * launching with ./bin/org.apache.spark-submit).
      */
    def this() = this(new SparkConf())

    /**
      * 允许直接设置公共Spark属性的替代构造函数
      *
      * Alternative constructor that allows setting common Spark properties directly
      *
      * @param master  Cluster URL to connect to (e.g. mesos://host:port, org.apache.spark://host:port, local[4]).
      * @param appName A name for your application, to display on the cluster web UI
      * @param conf    a [[org.apache.spark.SparkConf]] object specifying other Spark parameters
      */
    def this(master: String, appName: String, conf: SparkConf) =
        this(SparkContext.updatedConf(conf, master, appName))

    /**
      * 允许直接设置公共Spark属性的替代构造函数
      *
      * Alternative constructor that allows setting common Spark properties directly
      *
      * @param master      Cluster URL to connect to (e.g. mesos://host:port, org.apache.spark://host:port, local[4]).
      * @param appName     A name for your application, to display on the cluster web UI.
      * @param sparkHome   Location where Spark is installed on cluster nodes.
      * @param jars        Collection of JARs to send to the cluster. These can be paths on the local file
      *                    system or HDFS, HTTP, HTTPS, or FTP URLs.
      * @param environment Environment variables to set on worker nodes.
      */
    def this(
                master: String,
                appName: String,
                sparkHome: String = null,
                jars: Seq[String] = Nil,
                environment: Map[String, String] = Map()) = {
        this(SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment))
    }

    /**
      * 返回此SparkContext配置的副本。无法在运行时更改配置“”。
      *
      * Return a copy of this SparkContext's configuration. The configuration ''cannot'' be
      * changed at runtime.
      */
    def getConf: SparkConf = conf.clone()

    def jars: Seq[String] = _jars

    def files: Seq[String] = _files

    def deployMode: String = _conf.getOption("org.apache.spark.submit.deployMode").getOrElse("client")

    /**
      * 如果上下文已停止或正在停止，则返回true。
      *
      * @return true if context is stopped or in the midst of stopping.
      */
    def isStopped: Boolean = stopped.get()

    def statusTracker: SparkStatusTracker = _statusTracker

    def uiWebUrl: Option[String] = _ui.map(_.webUrl)

    /**
      * 控制我们的日志水平。这将覆盖任何用户定义的日志设置。
      *
      * Control our logLevel. This overrides any user-defined log settings.
      *
      * @param logLevel The desired log level as a string.
      *                 Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
      */
    def setLogLevel(logLevel: String) {
        // let's allow lowercase or mixed case too
        val upperCased = logLevel.toUpperCase(Locale.ROOT)
        require(SparkContext.VALID_LOG_LEVELS.contains(upperCased),
            s"Supplied level $logLevel did not match one of:" +
                s" ${SparkContext.VALID_LOG_LEVELS.mkString(",")}")
        Utils.setLogLevel(org.apache.log4j.Level.toLevel(upperCased))
    }

    /**
      * 设置当前作业的可读描述
      *
      * Set a human readable description of the current job.
      */
    def setJobDescription(value: String) {
        setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, value)
    }

    /**
      * 将组ID分配给此线程启动的所有作业，直到将组ID设置为其他值或清除为止。
      *
      * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
      * different value or cleared.
      *
      * Often, a unit of execution in an application consists of multiple Spark actions or jobs.
      * Application programmers can use this method to group all those jobs together and give a
      * group description. Once set, the Spark web UI will associate such jobs with this group.
      *
      * The application can also use `org.apache.org.apache.spark.SparkContext.cancelJobGroup` to cancel all
      * running jobs in this group. For example,
      * {{{
      * // In the main thread:
      * sc.setJobGroup("some_job_to_cancel", "some job description")
      * sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
      *
      * // In a separate thread:
      * sc.cancelJobGroup("some_job_to_cancel")
      * }}}
      *
      * @param interruptOnCancel If true, then job cancellation will result in `Thread.interrupt()`
      *                          being called on the job's executor threads. This is useful to help ensure that the tasks
      *                          are actually stopped in a timely manner, but is off by default due to HDFS-1208, where HDFS
      *                          may respond to Thread.interrupt() by marking nodes as dead.
      */
    def setJobGroup(groupId: String, description: String, interruptOnCancel: Boolean = false) {
        setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, description)
        setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, groupId)
        // Note: Specifying interruptOnCancel in setJobGroup (rather than cancelJobGroup) avoids
        // changing several public APIs and allows Spark cancellations outside of the cancelJobGroup
        // APIs to also take advantage of this property (e.g., internal job failures or canceling from
        // JobProgressTab UI) on a per-job basis.
        setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, interruptOnCancel.toString)
    }

    /**
      * 清除当前线程的作业组ID及其描述。
      *
      * Clear the current thread's job group ID and its description.
      */
    def clearJobGroup() {
        setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, null)
        setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, null)
        setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, null)
    }

    /**
      * 创建一个新的RDD[Long]，其中包含从“start”到“end”（独占）的元素，每个元素都增加“step”。
      *
      * Creates a new RDD[Long] containing elements from `start` to `end`(exclusive), increased by
      * `step` every element.
      *
      * @note if we need to cache this RDD, we should make sure each partition does not exceed limit.
      * @param start     the start value.
      * @param end       the end value.
      * @param step      the incremental step
      * @param numSlices number of partitions to divide the collection into
      * @return RDD representing distributed range
      */
    def range(
                 start: Long,
                 end: Long,
                 step: Long = 1,
                 numSlices: Int = defaultParallelism): RDD[Long] = withScope {
        assertNotStopped()
        // when step is 0, range will run infinitely
        require(step != 0, "step cannot be 0")
        val numElements: BigInt = {
            val safeStart = BigInt(start)
            val safeEnd = BigInt(end)
            if ((safeEnd - safeStart) % step == 0 || (safeEnd > safeStart) != (step > 0)) {
                (safeEnd - safeStart) / step
            } else {
                // the remainder has the same sign with range, could add 1 more
                (safeEnd - safeStart) / step + 1
            }
        }
        parallelize(0 until numSlices, numSlices).mapPartitionsWithIndex { (i, _) =>
            val partitionStart = (i * numElements) / numSlices * step + start
            val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start

            def getSafeMargin(bi: BigInt): Long =
                if (bi.isValidLong) {
                    bi.toLong
                } else if (bi > 0) {
                    Long.MaxValue
                } else {
                    Long.MinValue
                }

            val safePartitionStart = getSafeMargin(partitionStart)
            val safePartitionEnd = getSafeMargin(partitionEnd)

            new Iterator[Long] {
                private[this] var number: Long = safePartitionStart
                private[this] var overflow: Boolean = false

                override def hasNext =
                    if (!overflow) {
                        if (step > 0) {
                            number < safePartitionEnd
                        } else {
                            number > safePartitionEnd
                        }
                    } else false

                override def next() = {
                    val ret = number
                    number += step
                    if (number < ret ^ step < 0) {
                        // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
                        // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a step
                        // back, we are pretty sure that we have an overflow.
                        overflow = true
                    }
                    ret
                }
            }
        }
    }

    private[spark] def assertNotStopped(): Unit = {
        if (stopped.get()) {
            val activeContext = SparkContext.activeContext.get()
            val activeCreationSite =
                if (activeContext == null) {
                    "(No active SparkContext.)"
                } else {
                    activeContext.creationSite.longForm
                }
            throw new IllegalStateException(
                s"""Cannot call methods on a stopped SparkContext.
                   |This stopped SparkContext was created at:
                   |
                   |${creationSite.longForm}
                   |
                   |The currently active SparkContext was created at:
                   |
                   |$activeCreationSite
         """.stripMargin)
        }
    }

    /**
      * 在一个作用域中执行一个代码块，这样在这个主体中创建的所有新RDD都将是同一作用域的一部分。
      *
      * Execute a block of code in a scope such that all new RDDs created in this body will
      * be part of the same scope. For more detail, see {{org.apache.org.apache.spark.rdd.RDDOperationScope}}.
      *
      * @note Return statements are NOT allowed in the given body.
      */
    private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](this)(body)

    /**
      * 分发本地Scala集合以形成RDD。
      *
      * Distribute a local Scala collection to form an RDD.
      *
      * @note Parallelize acts lazily. If `seq` is a mutable collection and is altered after the call
      *       to parallelize and before the first action on the RDD, the resultant RDD will reflect the
      *       modified collection. Pass a copy of the argument to avoid this.
      * @note avoid using `parallelize(Seq())` to create an empty `RDD`. Consider `emptyRDD` for an
      *       RDD with no partitions, or `parallelize(Seq[T]())` for an RDD of `T` with empty partitions.
      * @param seq       Scala collection to distribute
      * @param numSlices number of partitions to divide the collection into
      * @return RDD representing distributed collection
      */
    def parallelize[T: ClassTag](
                                    seq: Seq[T],
                                    numSlices: Int = defaultParallelism): RDD[T] = withScope {
        assertNotStopped()
        new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
    }

    /**
      * 分发本地Scala集合以形成RDD。
      *
      * Distribute a local Scala collection to form an RDD.
      *
      * This method is identical to `parallelize`.
      *
      * @param seq       Scala collection to distribute
      * @param numSlices number of partitions to divide the collection into
      * @return RDD representing distributed collection
      */
    def makeRDD[T: ClassTag](
                                seq: Seq[T],
                                numSlices: Int = defaultParallelism): RDD[T] = withScope {
        parallelize(seq, numSlices)
    }

    /**
      * 分发一个本地Scala集合以形成一个RDD，每个对象有一个或多个位置首选项（Spark节点的主机名）。为每个集合项创建一个新分区。
      *
      * Distribute a local Scala collection to form an RDD, with one or more
      * location preferences (hostnames of Spark nodes) for each object.
      * Create a new partition for each collection item.
      *
      * @param seq list of tuples of data and location preferences (hostnames of Spark nodes)
      * @return RDD representing data partitioned according to location preferences
      */
    def makeRDD[T: ClassTag](seq: Seq[(T, Seq[String])]): RDD[T] = withScope {
        assertNotStopped()
        val indexToPrefs = seq.zipWithIndex.map(t => (t._2, t._1._2)).toMap
        new ParallelCollectionRDD[T](this, seq.map(_._1), math.max(seq.size, 1), indexToPrefs)
    }

    /**
      * 从HDFS、本地文件系统（在所有节点上都可用）或任何Hadoop支持的文件系统URI读取文本文件，并将其作为字符串的RDD返回。
      *
      * Read a text file from HDFS, a local file system (available on all nodes), or any
      * Hadoop-supported file system URI, and return it as an RDD of Strings.
      *
      * @param path          path to the text file on a supported file system
      * @param minPartitions suggested minimum number of partitions for the resulting RDD
      * @return RDD of lines of the text file
      */
    def textFile(
                    path: String,
                    minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
        assertNotStopped()
        hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
            minPartitions).map(pair => pair._2.toString).setName(path)
    }

    /**
      * 从HDFS、本地文件系统（在所有节点上都可用）或任何Hadoop支持的文件系统URI读取文本文件目录。
      *
      * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
      * Hadoop-supported file system URI. Each file is read as a single record and returned in a
      * key-value pair, where the key is the path of each file, the value is the content of each file.
      *
      * <p> For example, if you have the following files:
      * {{{
      *   hdfs://a-hdfs-path/part-00000
      *   hdfs://a-hdfs-path/part-00001
      *   ...
      *   hdfs://a-hdfs-path/part-nnnnn
      * }}}
      *
      * Do `val rdd = sparkContext.wholeTextFile("hdfs://a-hdfs-path")`,
      *
      * <p> then `rdd` contains
      * {{{
      *   (a-hdfs-path/part-00000, its content)
      *   (a-hdfs-path/part-00001, its content)
      *   ...
      *   (a-hdfs-path/part-nnnnn, its content)
      * }}}
      *
      * @note Small files are preferred, large file is also allowable, but may cause bad performance.
      * @note On some filesystems, `.../path/&#42;` can be a more efficient way to read all files
      *       in a directory rather than `.../path/` or `.../path`
      * @note Partitioning is determined by data locality. This may result in too few partitions
      *       by default.
      * @param path          Directory to the input data files, the path can be comma separated paths as the
      *                      list of inputs.
      * @param minPartitions A suggestion value of the minimal splitting number for input data.
      * @return RDD representing tuples of file path and the corresponding file content
      */
    def wholeTextFiles(
                          path: String,
                          minPartitions: Int = defaultMinPartitions): RDD[(String, String)] = withScope {
        assertNotStopped()
        val job = NewHadoopJob.getInstance(hadoopConfiguration)
        // 使用setInputPaths使wholeTextFiles与hadoopFile/textFile对齐
        // Use setInputPaths so that wholeTextFiles aligns with hadoopFile/textFile in taking
        // comma separated files as input. (see SPARK-7155)
        NewFileInputFormat.setInputPaths(job, path)
        val updateConf = job.getConfiguration
        new WholeTextFileRDD(
            this,
            classOf[WholeTextFileInputFormat],
            classOf[Text],
            classOf[Text],
            updateConf,
            minPartitions).map(record => (record._1.toString, record._2.toString)).setName(path)
    }

    /**
      * 获取Hadoop可读数据集的RDD作为每个文件的PortableDataStream
      *
      * Get an RDD for a Hadoop-readable dataset as PortableDataStream for each file
      * (useful for binary data)
      *
      * For example, if you have the following files:
      * {{{
      *   hdfs://a-hdfs-path/part-00000
      *   hdfs://a-hdfs-path/part-00001
      *   ...
      *   hdfs://a-hdfs-path/part-nnnnn
      * }}}
      *
      * Do
      * `val rdd = sparkContext.binaryFiles("hdfs://a-hdfs-path")`,
      *
      * then `rdd` contains
      * {{{
      *   (a-hdfs-path/part-00000, its content)
      *   (a-hdfs-path/part-00001, its content)
      *   ...
      *   (a-hdfs-path/part-nnnnn, its content)
      * }}}
      *
      * @note Small files are preferred; very large files may cause bad performance.
      * @note On some filesystems, `.../path/&#42;` can be a more efficient way to read all files
      *       in a directory rather than `.../path/` or `.../path`
      * @note Partitioning is determined by data locality. This may result in too few partitions
      *       by default.
      * @param path          Directory to the input data files, the path can be comma separated paths as the
      *                      list of inputs.
      * @param minPartitions A suggestion value of the minimal splitting number for input data.
      * @return RDD representing tuples of file path and corresponding file content
      */
    def binaryFiles(
                       path: String,
                       minPartitions: Int = defaultMinPartitions): RDD[(String, PortableDataStream)] = withScope {
        assertNotStopped()
        val job = NewHadoopJob.getInstance(hadoopConfiguration)
        // Use setInputPaths so that binaryFiles aligns with hadoopFile/textFile in taking
        // comma separated files as input. (see SPARK-7155)
        NewFileInputFormat.setInputPaths(job, path)
        val updateConf = job.getConfiguration
        new BinaryFileRDD(
            this,
            classOf[StreamInputFormat],
            classOf[String],
            classOf[PortableDataStream],
            updateConf,
            minPartitions).setName(path)
    }

    /**
      * 从平面二进制文件加载数据，假设每条记录的长度不变。
      *
      * Load data from a flat binary file, assuming the length of each record is constant.
      *
      * @note We ensure that the byte array for each record in the resulting RDD
      *       has the provided record length.
      * @param path         Directory to the input data files, the path can be comma separated paths as the
      *                     list of inputs.
      * @param recordLength The length at which to split the records
      * @param conf         Configuration for setting up the dataset.
      * @return An RDD of data with values, represented as byte arrays
      */
    def binaryRecords(
                         path: String,
                         recordLength: Int,
                         conf: Configuration = hadoopConfiguration): RDD[Array[Byte]] = withScope {
        assertNotStopped()
        conf.setInt(FixedLengthBinaryInputFormat.RECORD_LENGTH_PROPERTY, recordLength)
        val br = newAPIHadoopFile[LongWritable, BytesWritable, FixedLengthBinaryInputFormat](path,
            classOf[FixedLengthBinaryInputFormat],
            classOf[LongWritable],
            classOf[BytesWritable],
            conf = conf)
        br.map { case (k, v) =>
            val bytes = v.copyBytes()
            assert(bytes.length == recordLength, "Byte array does not have correct length")
            bytes
        }
    }

    /**
      * 根据Hadoop JobConf的InputFormat和其他必要信息，从Hadoop JobConf获取Hadoop可读数据集的RDD
      *
      * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf given its InputFormat and other
      * necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable),
      * using the older MapReduce API (`org.apache.hadoop.mapred`).
      *
      * @param conf             JobConf for setting up the dataset. Note: This will be put into a Broadcast.
      *                         Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
      *                         sure you won't modify the conf. A safe approach is always creating a new conf for
      *                         a new RDD.
      * @param inputFormatClass storage format of the data to be read
      * @param keyClass         `Class` of the key associated with the `inputFormatClass` parameter
      * @param valueClass       `Class` of the value associated with the `inputFormatClass` parameter
      * @param minPartitions    Minimum number of Hadoop Splits to generate.
      * @return RDD of tuples of key and corresponding value
      * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
      *       record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      *       operation will create many references to the same object.
      *       If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      *       copy them using a `map` function.
      */
    def hadoopRDD[K, V](
                           conf: JobConf,
                           inputFormatClass: Class[_ <: InputFormat[K, V]],
                           keyClass: Class[K],
                           valueClass: Class[V],
                           minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
        assertNotStopped()

        // This is a hack to enforce loading hdfs-site.xml.
        // See SPARK-11227 for details.
        FileSystem.getLocal(conf)

        // Add necessary security credentials to the JobConf before broadcasting it.
        SparkHadoopUtil.get.addCredentials(conf)
        new HadoopRDD(this, conf, inputFormatClass, keyClass, valueClass, minPartitions)
    }

    /**
      * hadoopFile（）的一个更智能的版本，它使用类标记来计算键、值和InputFormat的类，这样用户就不需要直接传递它们。
      *
      * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
      * values and the InputFormat so that users don't need to pass them directly. Instead, callers
      * can just write, for example,
      * {{{
      * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
      * }}}
      *
      * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
      *       record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      *       operation will create many references to the same object.
      *       If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      *       copy them using a `map` function.
      * @param path directory to the input data files, the path can be comma separated paths as
      *             a list of inputs
      * @return RDD of tuples of key and corresponding value
      */
    def hadoopFile[K, V, F <: InputFormat[K, V]](path: String)
                                                (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
        hadoopFile[K, V, F](path, defaultMinPartitions)
    }

    /**
      * hadoopFile（）的一个更智能的版本，它使用类标记来计算键、值和InputFormat的类，这样用户就不需要直接传递它们。
      *
      * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
      * values and the InputFormat so that users don't need to pass them directly. Instead, callers
      * can just write, for example,
      * {{{
      * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path, minPartitions)
      * }}}
      *
      * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
      *       record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      *       operation will create many references to the same object.
      *       If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      *       copy them using a `map` function.
      * @param path          directory to the input data files, the path can be comma separated paths
      *                      as a list of inputs
      * @param minPartitions suggested minimum number of partitions for the resulting RDD
      * @return RDD of tuples of key and corresponding value
      */
    def hadoopFile[K, V, F <: InputFormat[K, V]]
    (path: String, minPartitions: Int)
    (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
        hadoopFile(path,
            fm.runtimeClass.asInstanceOf[Class[F]],
            km.runtimeClass.asInstanceOf[Class[K]],
            vm.runtimeClass.asInstanceOf[Class[V]],
            minPartitions)
    }

    /**
      * 更智能的“newApiHadoopFile”版本，它使用类标记来计算键、值和“org.apache.hadoop.mapreduce.InputFormat”（新的mapreduce API）的类，
      * 这样用户就不需要直接传递它们。
      *
      * Smarter version of `newApiHadoopFile` that uses class tags to figure out the classes of keys,
      * values and the `org.apache.hadoop.mapreduce.InputFormat` (new MapReduce API) so that user
      * don't need to pass them directly. Instead, callers can just write, for example:
      * ```
      * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
      * ```
      *
      *
      * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
      *       record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      *       operation will create many references to the same object.
      *       If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      *       copy them using a `map` function.
      * @param path directory to the input data files, the path can be comma separated paths
      *             as a list of inputs
      * @return RDD of tuples of key and corresponding value
      */
    def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]]
    (path: String)
    (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = withScope {
        newAPIHadoopFile(
            path,
            fm.runtimeClass.asInstanceOf[Class[F]],
            km.runtimeClass.asInstanceOf[Class[K]],
            vm.runtimeClass.asInstanceOf[Class[V]])
    }

    /**
      * 获取给定Hadoop文件的RDD，其中包含任意新的API InputFormat和要传递到输入格式的额外配置选项。
      *
      * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
      * and extra configuration options to pass to the input format.
      *
      * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
      *       record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      *       operation will create many references to the same object.
      *       If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      *       copy them using a `map` function.
      * @param path   directory to the input data files, the path can be comma separated paths
      *               as a list of inputs
      * @param fClass storage format of the data to be read
      * @param kClass `Class` of the key associated with the `fClass` parameter
      * @param vClass `Class` of the value associated with the `fClass` parameter
      * @param conf   Hadoop configuration
      * @return RDD of tuples of key and corresponding value
      */
    def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
                                                             path: String,
                                                             fClass: Class[F],
                                                             kClass: Class[K],
                                                             vClass: Class[V],
                                                             conf: Configuration = hadoopConfiguration): RDD[(K, V)] = withScope {
        assertNotStopped()

        // This is a hack to enforce loading hdfs-site.xml.
        // See SPARK-11227 for details.
        FileSystem.getLocal(hadoopConfiguration)

        // The call to NewHadoopJob automatically adds security credentials to conf,
        // so we don't need to explicitly add them ourselves
        val job = NewHadoopJob.getInstance(conf)
        // Use setInputPaths so that newAPIHadoopFile aligns with hadoopFile/textFile in taking
        // comma separated files as input. (see SPARK-7155)
        NewFileInputFormat.setInputPaths(job, path)
        val updatedConf = job.getConfiguration
        new NewHadoopRDD(this, fClass, kClass, vClass, updatedConf).setName(path)
    }

    /**
      * 我们重用的Hadoop代码（例如文件系统）的默认Hadoop配置。
      *
      * A default Hadoop Configuration for the Hadoop code (e.g. file systems) that we reuse.
      *
      * @note As it will be reused in all Hadoop RDDs, it's better not to modify it unless you
      *       plan to set some global configurations for all Hadoop RDDs.
      */
    def hadoopConfiguration: Configuration = _hadoopConfiguration

    /**
      * 获取给定Hadoop文件的RDD，其中包含任意新的API InputFormat和要传递到输入格式的额外配置选项。
      *
      * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
      * and extra configuration options to pass to the input format.
      *
      * @param conf   Configuration for setting up the dataset. Note: This will be put into a Broadcast.
      *               Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
      *               sure you won't modify the conf. A safe approach is always creating a new conf for
      *               a new RDD.
      * @param fClass storage format of the data to be read
      * @param kClass `Class` of the key associated with the `fClass` parameter
      * @param vClass `Class` of the value associated with the `fClass` parameter
      * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
      *       record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      *       operation will create many references to the same object.
      *       If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      *       copy them using a `map` function.
      */
    def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
                                                            conf: Configuration = hadoopConfiguration,
                                                            fClass: Class[F],
                                                            kClass: Class[K],
                                                            vClass: Class[V]): RDD[(K, V)] = withScope {
        assertNotStopped()

        // This is a hack to enforce loading hdfs-site.xml.
        // See SPARK-11227 for details.
        FileSystem.getLocal(conf)

        // Add necessary security credentials to the JobConf. Required to access secure HDFS.
        val jconf = new JobConf(conf)
        SparkHadoopUtil.get.addCredentials(jconf)
        new NewHadoopRDD(this, fClass, kClass, vClass, jconf)
    }

    /**
      * 获取具有给定键和值类型的Hadoop SequenceFile的RDD。
      *
      * Get an RDD for a Hadoop SequenceFile with given key and value types.
      *
      * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
      *       record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      *       operation will create many references to the same object.
      *       If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      *       copy them using a `map` function.
      * @param path       directory to the input data files, the path can be comma separated paths
      *                   as a list of inputs
      * @param keyClass   `Class` of the key associated with `SequenceFileInputFormat`
      * @param valueClass `Class` of the value associated with `SequenceFileInputFormat`
      * @return RDD of tuples of key and corresponding value
      */
    def sequenceFile[K, V](
                              path: String,
                              keyClass: Class[K],
                              valueClass: Class[V]): RDD[(K, V)] = withScope {
        assertNotStopped()
        sequenceFile(path, keyClass, valueClass, defaultMinPartitions)
    }

    /**
      * 获取具有给定键和值类型的Hadoop SequenceFile的RDD。
      *
      * Get an RDD for a Hadoop SequenceFile with given key and value types.
      *
      * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
      *       record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      *       operation will create many references to the same object.
      *       If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      *       copy them using a `map` function.
      * @param path          directory to the input data files, the path can be comma separated paths
      *                      as a list of inputs
      * @param keyClass      `Class` of the key associated with `SequenceFileInputFormat`
      * @param valueClass    `Class` of the value associated with `SequenceFileInputFormat`
      * @param minPartitions suggested minimum number of partitions for the resulting RDD
      * @return RDD of tuples of key and corresponding value
      */
    def sequenceFile[K, V](path: String,
                           keyClass: Class[K],
                           valueClass: Class[V],
                           minPartitions: Int
                          ): RDD[(K, V)] = withScope {
        assertNotStopped()
        val inputFormatClass = classOf[SequenceFileInputFormat[K, V]]
        hadoopFile(path, inputFormatClass, keyClass, valueClass, minPartitions)
    }

    /**
      * 获取具有任意输入格式的Hadoop文件的RDD
      *
      * Get an RDD for a Hadoop file with an arbitrary InputFormat
      *
      * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
      *       record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      *       operation will create many references to the same object.
      *       If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      *       copy them using a `map` function.
      * @param path             directory to the input data files, the path can be comma separated paths
      *                         as a list of inputs
      * @param inputFormatClass storage format of the data to be read
      * @param keyClass         `Class` of the key associated with the `inputFormatClass` parameter
      * @param valueClass       `Class` of the value associated with the `inputFormatClass` parameter
      * @param minPartitions    suggested minimum number of partitions for the resulting RDD
      * @return RDD of tuples of key and corresponding value
      */
    def hadoopFile[K, V](
                            path: String,
                            inputFormatClass: Class[_ <: InputFormat[K, V]],
                            keyClass: Class[K],
                            valueClass: Class[V],
                            minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
        assertNotStopped()

        // This is a hack to enforce loading hdfs-site.xml.
        // See SPARK-11227 for details.
        FileSystem.getLocal(hadoopConfiguration)

        // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
        val confBroadcast = broadcast(new SerializableConfiguration(hadoopConfiguration))
        val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
        new HadoopRDD(
            this,
            confBroadcast,
            Some(setInputPathsFunc),
            inputFormatClass,
            keyClass,
            valueClass,
            minPartitions).setName(path)
    }

    /**
      * 向集群广播一个只读变量，返回一个org.apache.spark.Broadcast.Broadcast对象，以便在分布式函数中读取它。
      * 变量将只发送到每个集群一次。
      *
      * Broadcast a read-only variable to the cluster, returning a
      * [[org.apache.spark.broadcast.Broadcast]] object for reading it in distributed functions.
      * The variable will be sent to each cluster only once.
      *
      * @param value value to broadcast to the Spark nodes
      * @return `Broadcast` object, a read-only variable cached on each machine
      */
    def broadcast[T: ClassTag](value: T): Broadcast[T] = {
        assertNotStopped()
        require(!classOf[RDD[_]].isAssignableFrom(classTag[T].runtimeClass),
            "Can not directly broadcast RDDs; instead, call collect() and broadcast the result.")
        val bc = env.broadcastManager.newBroadcast[T](value, isLocal)
        val callSite = getCallSite
        logInfo("Created broadcast " + bc.id + " from " + callSite.shortForm)
        cleaner.foreach(_.registerBroadcastForCleanup(bc))
        bc
    }

    def isLocal: Boolean = Utils.isLocalMaster(_conf)

    /* ------------------------------------------------------------------------------------- *
     | Initialization. This code initializes the context in a manner that is exception-safe. |
     | All internal fields holding state are initialized here, and any error prompts the     |
     | stop() method to be called.                                                           |
     * ------------------------------------------------------------------------------------- */

    private[spark] def cleaner: Option[ContextCleaner] = _cleaner

    /**
      * 用户未给定时Hadoop RDD的默认最小分区数
      *
      * Default min number of partitions for Hadoop RDDs when not given by user
      * Notice that we use math.min so the "defaultMinPartitions" cannot be higher than 2.
      * The reasons for this are discussed in https://github.com/mesos/org.apache.spark/pull/718
      */
    def defaultMinPartitions: Int = math.min(defaultParallelism, 2)

    /**
      * 当用户未给定时使用的默认并行级别（例如parallelize和makeRDD）。
      *
      * Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD).
      */
    def defaultParallelism: Int = {
        assertNotStopped()
        taskScheduler.defaultParallelism
    }

    /**
      * 初始化SparkContext代码
      */
    try {
        _conf = config.clone()
        _conf.validateSettings() // 检查非法或不推荐的配置设置。
        // 检查App Master和Name是否设置
        if (!_conf.contains("org.apache.spark.master")) throw new SparkException("A master URL must be set in your configuration")
        if (!_conf.contains("org.apache.spark.app.name")) {
            throw new SparkException("An application name must be set in your configuration")
        }
        // 在spark驱动程序日志中打印org.apache.spark.app.name
        // log out org.apache.spark.app.name in the Spark driver logs
        logInfo(s"Submitted application: $appName")
        // 系统属性org.apache.spark.yarn.app.id如果AM在YARN集群上运行用户代码，则必须设置。
        // System property org.apache.spark.yarn.app.id must be set if user code ran by AM on a YARN cluster
        if (master == "yarn" && deployMode == "cluster" && !_conf.contains("org.apache.spark.yarn.app.id")) {
            throw new SparkException("Detected yarn cluster mode, but isn't running on a cluster. " +
                "Deployment to YARN is not supported directly by SparkContext. Please use org.apache.spark-submit.")
        }
        if (_conf.getBoolean("org.apache.spark.logConf", false)) {
            logInfo("Spark configuration:\n" + _conf.toDebugString)
        }
        // 设置Spark驱动程序主机和端口系统属性。这将显式地设置配置，而不是依赖于config常量的默认值。
        // Set Spark driver host and port system properties. This explicitly sets the configuration
        // instead of relying on the default value of the config constant.
        _conf.set(DRIVER_HOST_ADDRESS, _conf.get(DRIVER_HOST_ADDRESS))
        _conf.setIfMissing("org.apache.spark.driver.port", "0")
        _conf.set("org.apache.spark.executor.id", SparkContext.DRIVER_IDENTIFIER) // 配置driver运行节点
        _jars = Utils.getUserJars(_conf)
        _files = _conf.getOption("org.apache.spark.files").map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
        _eventLogDir = if (isEventLogEnabled) {
            val unresolvedDir = conf.get("org.apache.spark.eventLog.dir", EventLoggingListener.DEFAULT_LOG_DIR).stripSuffix("/")
            Some(Utils.resolveURI(unresolvedDir))
        } else {
            None
        }
        _eventLogCodec = {
            val compress = _conf.getBoolean("org.apache.spark.eventLog.compress", false)
            if (compress && isEventLogEnabled) {
                Some(CompressionCodec.getCodecName(_conf)).map(CompressionCodec.getShortName)
            } else {
                None
            }
        }
        if (master == "yarn" && deployMode == "client") System.setProperty("SPARK_YARN_MODE", "true")
        // "_jobProgressListener"应该在创建SparkEnv之前设置，因为在创建"SparkEnv",一些信息将被发布到“listenerBus”，我们不应该错过它们。
        // "_jobProgressListener" should be set up before creating SparkEnv because when creating
        // "SparkEnv", some messages will be posted to "listenerBus" and we should not miss them.
        _jobProgressListener = new JobProgressListener(_conf)
        listenerBus.addListener(jobProgressListener)
        // 创建Spark执行环境（缓存、map输出跟踪器等）
        // Create the Spark execution environment (cache, map output tracker, etc)
        _env = createSparkEnv(_conf, isLocal, listenerBus)
        SparkEnv.set(_env)
        // 如果运行REPL，请向文件服务器注册REPL的输出目录。
        // If running the REPL, register the repl's output dir with the file server.
        _conf.getOption("org.apache.spark.repl.class.outputDir").foreach { path =>
            val replUri = _env.rpcEnv.fileServer.addDirectory("/classes", new File(path))
            _conf.set("org.apache.spark.repl.class.uri", replUri)
        }

        // 用于监视job和stage进度的Low-level状态报告API。
        _statusTracker = new SparkStatusTracker(this)

        _progressBar =
            if (_conf.getBoolean("org.apache.spark.ui.showConsoleProgress", true) && !log.isInfoEnabled) {
                Some(new ConsoleProgressBar(this))
            } else {
                None
            }

        _ui =
            if (conf.getBoolean("org.apache.spark.ui.enabled", true)) {
                Some(SparkUI.createLiveUI(this, _conf, listenerBus, _jobProgressListener,
                    _env.securityManager, appName, startTime = startTime))
            } else {
                // For tests, do not enable the UI
                None
            }
        // 在启动任务调度器之前绑定UI，以便将绑定的端口正确地与集群管理器通信。
        // Bind the UI before starting the task scheduler to communicate
        // the bound port to the cluster manager properly
        _ui.foreach(_.bind())

        _hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(_conf)

        // 添加通过构造函数给出的每个JAR。
        // Add each JAR given through the constructor
        if (jars != null) {
            jars.foreach(addJar)
        }

        if (files != null) {
            files.foreach(addFile)
        }

        _executorMemory = _conf.getOption("org.apache.spark.executor.memory")
            .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
            .orElse(Option(System.getenv("SPARK_MEM"))
                .map(warnSparkMem))
            .map(Utils.memoryStringToMb)
            .getOrElse(1024)

        // 将java选项转换为env vars是一种解决方法，因为我们不能在sbt中直接设置env vars。
        // Convert java options to env vars as a work around
        // since we can't set env vars directly in sbt.
        for {(envKey, propKey) <- Seq(("SPARK_TESTING", "org.apache.spark.testing"))
             value <- Option(System.getenv(envKey)).orElse(Option(System.getProperty(propKey)))} {
            executorEnvs(envKey) = value
        }
        Option(System.getenv("SPARK_PREPEND_CLASSES")).foreach { v =>
            executorEnvs("SPARK_PREPEND_CLASSES") = v
        }
        // Mesos调度器后端依赖于此环境变量来设置执行器内存。
        // The Mesos scheduler backend relies on this environment variable to set executor memory.
        // TODO: Set this only in the Mesos scheduler.
        executorEnvs("SPARK_EXECUTOR_MEMORY") = executorMemory + "m"
        executorEnvs ++= _conf.getExecutorEnv
        executorEnvs("SPARK_USER") = sparkUser

        // 我们需要在“createTaskScheduler”之前注册“HeartbeatReceiver”，因为Executor将在构造函数中检索“HeartbeatReceiver”。
        // We need to register "HeartbeatReceiver" before "createTaskScheduler" because Executor will
        // retrieve "HeartbeatReceiver" in the constructor. (SPARK-6640)
        _heartbeatReceiver = env.rpcEnv.setupEndpoint(
            HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))

        // 创建并启动计划程序
        // Create and start the scheduler
        val (sched, ts) = SparkContext.createTaskScheduler(this, master, deployMode)
        _schedulerBackend = sched
        _taskScheduler = ts
        _dagScheduler = new DAGScheduler(this)
        _heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet)

        // TaskScheduler在DAGScheduler的构造函数中设置DAGScheduler引用后，启动TaskScheduler。
        // start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's
        // constructor
        _taskScheduler.start()

        _applicationId = _taskScheduler.applicationId()
        _applicationAttemptId = taskScheduler.applicationAttemptId()
        _conf.set("org.apache.spark.app.id", _applicationId)
        if (_conf.getBoolean("org.apache.spark.ui.reverseProxy", false)) {
            System.setProperty("org.apache.spark.ui.proxyBase", "/proxy/" + _applicationId)
        }
        _ui.foreach(_.setAppId(_applicationId))
        _env.blockManager.initialize(_applicationId)

        // 需要为Driver程序设置度量系统org.apache.spark.app.id到应用程序ID。
        // 所以它应该在我们从任务调度器获取app ID并设置org.apache.spark.app.id.
        // The metrics system for Driver need to be set org.apache.spark.app.id to app ID.
        // So it should start after we get app ID from the task scheduler and set org.apache.spark.app.id.
        _env.metricsSystem.start()
        // 在度量系统启动后，将驱动程序度量servlet处理程序附加到web ui。
        // Attach the driver metrics servlet handler to the web ui after the metrics system is started.
        _env.metricsSystem.getServletHandlers.foreach(handler => ui.foreach(_.attachHandler(handler)))

        _eventLogger =
            if (isEventLogEnabled) {
                val logger =
                    new EventLoggingListener(_applicationId, _applicationAttemptId, _eventLogDir.get,
                        _conf, _hadoopConfiguration)
                logger.start()
                listenerBus.addListener(logger)
                Some(logger)
            } else {
                None
            }

        // 可以根据工作负载动态扩展执行器的数量。暴露测试。
        // Optionally scale number of executors dynamically based on workload. Exposed for testing.
        val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(_conf)
        _executorAllocationManager =
            if (dynamicAllocationEnabled) {
                schedulerBackend match {
                    case b: ExecutorAllocationClient =>
                        Some(new ExecutorAllocationManager(
                            schedulerBackend.asInstanceOf[ExecutorAllocationClient], listenerBus, _conf))
                    case _ =>
                        None
                }
            } else {
                None
            }
        _executorAllocationManager.foreach(_.start())

        _cleaner =
            if (_conf.getBoolean("org.apache.spark.cleaner.referenceTracking", true)) {
                Some(new ContextCleaner(this))
            } else {
                None
            }
        _cleaner.foreach(_.start())

        setupAndStartListenerBus()
        postEnvironmentUpdate()
        postApplicationStart()

        // Post init
        _taskScheduler.postStartHook()
        _env.metricsSystem.registerSource(_dagScheduler.metricsSource)
        _env.metricsSystem.registerSource(new BlockManagerSource(_env.blockManager))
        _executorAllocationManager.foreach { e =>
            _env.metricsSystem.registerSource(e.executorAllocationManagerSource)
        }

        // 如果用户忘记了上下文，请确保上下文已停止。这避免了在JVM干净地退出后留下未完成的事件日志。
        // 但是，如果JVM被杀死，这也没有什么帮助。
        // Make sure the context is stopped if the user forgets about it. This avoids leaving
        // unfinished event logs around after the JVM exits cleanly. It doesn't help if the JVM
        // is killed, though.
        logDebug("Adding shutdown hook") // force eager creation of logger
        _shutdownHookRef = ShutdownHookManager.addShutdownHook(
            ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY) { () =>
            logInfo("Invoking stop() from shutdown hook")
            stop()
        }
    } catch {
        case NonFatal(e) =>
            logError("Error initializing SparkContext.", e)
            try {
                stop()
            } catch {
                case NonFatal(inner) =>
                    logError("Error stopping SparkContext after init error.", inner)
            } finally {
                throw e
            }
    }

    /**
      * Version of sequenceFile() for types implicitly convertible to Writables through a
      * WritableConverter. For example, to access a SequenceFile where the keys are Text and the
      * values are IntWritable, you could simply write
      * {{{
      * sparkContext.sequenceFile[String, Int](path, ...)
      * }}}
      *
      * WritableConverters are provided in a somewhat strange way (by an implicit function) to support
      * both subclasses of Writable and types for which we define a converter (e.g. Int to
      * IntWritable). The most natural thing would've been to have implicit objects for the
      * converters, but then we couldn't have an object for every subclass of Writable (you can't
      * have a parameterized singleton object). We use functions instead to create a new converter
      * for the appropriate type. In addition, we pass the converter a ClassTag of its type to
      * allow it to figure out the Writable class to use in the subclass case.
      *
      * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
      *       record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
      *       operation will create many references to the same object.
      *       If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
      *       copy them using a `map` function.
      * @param path          directory to the input data files, the path can be comma separated paths
      *                      as a list of inputs
      * @param minPartitions suggested minimum number of partitions for the resulting RDD
      * @return RDD of tuples of key and corresponding value
      */
    def sequenceFile[K, V]
    (path: String, minPartitions: Int = defaultMinPartitions)
    (implicit km: ClassTag[K], vm: ClassTag[V],
     kcf: () => WritableConverter[K], vcf: () => WritableConverter[V]): RDD[(K, V)] = {
        withScope {
            assertNotStopped()
            val kc = clean(kcf)()
            val vc = clean(vcf)()
            val format = classOf[SequenceFileInputFormat[Writable, Writable]]
            val writables = hadoopFile(path, format,
                kc.writableClass(km).asInstanceOf[Class[Writable]],
                vc.writableClass(vm).asInstanceOf[Class[Writable]], minPartitions)
            writables.map { case (k, v) => (kc.convert(k), vc.convert(v)) }
        }
    }

    /**
      * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
      * BytesWritable values that contain a serialized partition. This is still an experimental
      * storage format and may not be supported exactly as is in future Spark releases. It will also
      * be pretty slow if you use the default serializer (Java serialization),
      * though the nice thing about it is that there's very little effort required to save arbitrary
      * objects.
      *
      * @param path          directory to the input data files, the path can be comma separated paths
      *                      as a list of inputs
      * @param minPartitions suggested minimum number of partitions for the resulting RDD
      * @return RDD representing deserialized data from the file(s)
      */
    def objectFile[T: ClassTag](
                                   path: String,
                                   minPartitions: Int = defaultMinPartitions): RDD[T] = withScope {
        assertNotStopped()
        sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
            .flatMap(x => Utils.deserialize[Array[T]](x._2.getBytes, Utils.getContextOrSparkClassLoader))
    }

    /** Build the union of a list of RDDs passed as variable-length arguments. */
    def union[T: ClassTag](first: RDD[T], rest: RDD[T]*): RDD[T] = withScope {
        union(Seq(first) ++ rest)
    }

    /**
      * 构建RDD列表的并集。
      * Build the union of a list of RDDs.
      */
    def union[T: ClassTag](rdds: Seq[RDD[T]]): RDD[T] = withScope {
        val partitioners = rdds.flatMap(_.partitioner).toSet
        if (rdds.forall(_.partitioner.isDefined) && partitioners.size == 1) {
            new PartitionerAwareUnionRDD(this, rdds)
        } else {
            new UnionRDD(this, rdds)
        }
    }

    /**
      * 获取一个没有分区或元素的RDD。
      * Get an RDD that has no partitions or elements.
      */
    def emptyRDD[T: ClassTag]: RDD[T] = new EmptyRDD[T](this)

    /**
      * Create an [[org.apache.spark.Accumulator]] variable of a given type, which tasks can "add"
      * values to using the `+=` method. Only the driver can access the accumulator's `value`.
      */
    @deprecated("use AccumulatorV2", "2.0.0")
    def accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]): Accumulator[T] = {
        val acc = new Accumulator(initialValue, param)
        cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
        acc
    }

    /**
      * Create an [[org.apache.spark.Accumulator]] variable of a given type, with a name for display
      * in the Spark UI. Tasks can "add" values to the accumulator using the `+=` method. Only the
      * driver can access the accumulator's `value`.
      */
    @deprecated("use AccumulatorV2", "2.0.0")
    def accumulator[T](initialValue: T, name: String)(implicit param: AccumulatorParam[T])
    : Accumulator[T] = {
        val acc = new Accumulator(initialValue, param, Option(name))
        cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
        acc
    }

    /**
      * Create an [[org.apache.spark.Accumulable]] shared variable, to which tasks can add values
      * with `+=`. Only the driver can access the accumulable's `value`.
      *
      * @tparam R accumulator result type
      * @tparam T type that can be added to the accumulator
      */
    @deprecated("use AccumulatorV2", "2.0.0")
    def accumulable[R, T](initialValue: R)(implicit param: AccumulableParam[R, T])
    : Accumulable[R, T] = {
        val acc = new Accumulable(initialValue, param)
        cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
        acc
    }

    /**
      * Create an [[org.apache.spark.Accumulable]] shared variable, with a name for display in the
      * Spark UI. Tasks can add values to the accumulable using the `+=` operator. Only the driver can
      * access the accumulable's `value`.
      *
      * @tparam R accumulator result type
      * @tparam T type that can be added to the accumulator
      */
    @deprecated("use AccumulatorV2", "2.0.0")
    def accumulable[R, T](initialValue: R, name: String)(implicit param: AccumulableParam[R, T])
    : Accumulable[R, T] = {
        val acc = new Accumulable(initialValue, param, Option(name))
        cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
        acc
    }

    // Methods for creating RDDs

    /**
      * 从“可变集合”类型创建累加器。
      *
      * Create an accumulator from a "mutable collection" type.
      *
      * Growable and TraversableOnce are the standard APIs that guarantee += and ++=, implemented by
      * standard mutable collections. So you can use this with mutable Map, Set, etc.
      */
    @deprecated("use AccumulatorV2", "2.0.0")
    def accumulableCollection[R <% Growable[T] with TraversableOnce[T] with Serializable : ClassTag, T]
    (initialValue: R): Accumulable[R, T] = {
        val param = new GrowableAccumulableParam[R, T]
        val acc = new Accumulable(initialValue, param)
        cleaner.foreach(_.registerAccumulatorForCleanup(acc.newAcc))
        acc
    }

    /**
      * Create and register a long accumulator, which starts with 0 and accumulates inputs by `add`.
      */
    def longAccumulator: LongAccumulator = {
        val acc = new LongAccumulator
        register(acc)
        acc
    }

    /**
      * Create and register a long accumulator, which starts with 0 and accumulates inputs by `add`.
      */
    def longAccumulator(name: String): LongAccumulator = {
        val acc = new LongAccumulator
        register(acc, name)
        acc
    }

    /**
      * Register the given accumulator with given name.
      *
      * @note Accumulators must be registered before use, or it will throw exception.
      */
    def register(acc: AccumulatorV2[_, _], name: String): Unit = {
        acc.register(this, name = Option(name))
    }

    /**
      * Create and register a double accumulator, which starts with 0 and accumulates inputs by `add`.
      */
    def doubleAccumulator: DoubleAccumulator = {
        val acc = new DoubleAccumulator
        register(acc)
        acc
    }

    /**
      * Register the given accumulator.
      *
      * @note Accumulators must be registered before use, or it will throw exception.
      */
    def register(acc: AccumulatorV2[_, _]): Unit = {
        acc.register(this)
    }

    /**
      * Create and register a double accumulator, which starts with 0 and accumulates inputs by `add`.
      */
    def doubleAccumulator(name: String): DoubleAccumulator = {
        val acc = new DoubleAccumulator
        register(acc, name)
        acc
    }

    /**
      * Create and register a `CollectionAccumulator`, which starts with empty list and accumulates
      * inputs by adding them into the list.
      */
    def collectionAccumulator[T]: CollectionAccumulator[T] = {
        val acc = new CollectionAccumulator[T]
        register(acc)
        acc
    }

    /**
      * Create and register a `CollectionAccumulator`, which starts with empty list and accumulates
      * inputs by adding them into the list.
      */
    def collectionAccumulator[T](name: String): CollectionAccumulator[T] = {
        val acc = new CollectionAccumulator[T]
        register(acc, name)
        acc
    }

    /**
      * Add a file to be downloaded with this Spark job on every node.
      *
      * @param path can be either a local file, a file in HDFS (or other Hadoop-supported
      *             filesystems), or an HTTP, HTTPS or FTP URI. To access the file in Spark jobs,
      *             use `SparkFiles.get(fileName)` to find its download location.
      */
    def addFile(path: String): Unit = {
        addFile(path, false)
    }

    /**
      * Add a file to be downloaded with this Spark job on every node.
      *
      * @param path      can be either a local file, a file in HDFS (or other Hadoop-supported
      *                  filesystems), or an HTTP, HTTPS or FTP URI. To access the file in Spark jobs,
      *                  use `SparkFiles.get(fileName)` to find its download location.
      * @param recursive if true, a directory can be given in `path`. Currently directories are
      *                  only supported for Hadoop-supported filesystems.
      */
    def addFile(path: String, recursive: Boolean): Unit = {
        val uri = new Path(path).toUri
        val schemeCorrectedPath = uri.getScheme match {
            case null | "local" => new File(path).getCanonicalFile.toURI.toString
            case _ => path
        }

        val hadoopPath = new Path(schemeCorrectedPath)
        val scheme = new URI(schemeCorrectedPath).getScheme
        if (!Array("http", "https", "ftp").contains(scheme)) {
            val fs = hadoopPath.getFileSystem(hadoopConfiguration)
            val isDir = fs.getFileStatus(hadoopPath).isDirectory
            if (!isLocal && scheme == "file" && isDir) {
                throw new SparkException(s"addFile does not support local directories when not running " +
                    "local mode.")
            }
            if (!recursive && isDir) {
                throw new SparkException(s"Added file $hadoopPath is a directory and recursive is not " +
                    "turned on.")
            }
        } else {
            // SPARK-17650: Make sure this is a valid URL before adding it to the list of dependencies
            Utils.validateURL(uri)
        }

        val key = if (!isLocal && scheme == "file") {
            env.rpcEnv.fileServer.addFile(new File(uri.getPath))
        } else {
            schemeCorrectedPath
        }
        val timestamp = System.currentTimeMillis
        if (addedFiles.putIfAbsent(key, timestamp).isEmpty) {
            logInfo(s"Added file $path at $key with timestamp $timestamp")
            // Fetch the file locally so that closures which are run on the driver can still use the
            // SparkFiles API to access files.
            Utils.fetchFile(uri.toString, new File(SparkFiles.getRootDirectory()), conf,
                env.securityManager, hadoopConfiguration, timestamp, useCache = false)
            postEnvironmentUpdate()
        }
    }

    /**
      * Returns a list of file paths that are added to resources.
      */
    def listFiles(): Seq[String] = addedFiles.keySet.toSeq

    /**
      * :: DeveloperApi ::
      * Register a listener to receive up-calls from events that happen during execution.
      */
    @DeveloperApi
    def addSparkListener(listener: SparkListenerInterface) {
        listenerBus.addListener(listener)
    }

    /**
      * :: DeveloperApi ::
      * Deregister the listener from Spark's listener bus.
      */
    @DeveloperApi
    def removeSparkListener(listener: SparkListenerInterface): Unit = {
        listenerBus.removeListener(listener)
    }

    /**
      * Update the cluster manager on our scheduling needs. Three bits of information are included
      * to help it make decisions.
      *
      * @param numExecutors         The total number of executors we'd like to have. The cluster manager
      *                             shouldn't kill any running executor to reach this number, but,
      *                             if all existing executors were to die, this is the number of executors
      *                             we'd want to be allocated.
      * @param localityAwareTasks   The number of tasks in all active stages that have a locality
      *                             preferences. This includes running, pending, and completed tasks.
      * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
      *                             that would like to like to run on that host.
      *                             This includes running, pending, and completed tasks.
      * @return whether the request is acknowledged by the cluster manager.
      */
    @DeveloperApi
    def requestTotalExecutors(
                                 numExecutors: Int,
                                 localityAwareTasks: Int,
                                 hostToLocalTaskCount: scala.collection.immutable.Map[String, Int]
                             ): Boolean = {
        schedulerBackend match {
            case b: CoarseGrainedSchedulerBackend =>
                b.requestTotalExecutors(numExecutors, localityAwareTasks, hostToLocalTaskCount)
            case _ =>
                logWarning("Requesting executors is only supported in coarse-grained mode")
                false
        }
    }

    /**
      * :: DeveloperApi ::
      * Request an additional number of executors from the cluster manager.
      *
      * @return whether the request is received.
      */
    @DeveloperApi
    def requestExecutors(numAdditionalExecutors: Int): Boolean = {
        schedulerBackend match {
            case b: CoarseGrainedSchedulerBackend =>
                b.requestExecutors(numAdditionalExecutors)
            case _ =>
                logWarning("Requesting executors is only supported in coarse-grained mode")
                false
        }
    }

    /**
      * :: DeveloperApi ::
      * Request that the cluster manager kill the specified executor.
      *
      * @note This is an indication to the cluster manager that the application wishes to adjust
      *       its resource usage downwards. If the application wishes to replace the executor it kills
      *       through this method with a new one, it should follow up explicitly with a call to
      *       {{SparkContext#requestExecutors}}.
      * @return whether the request is received.
      */
    @DeveloperApi
    def killExecutor(executorId: String): Boolean = killExecutors(Seq(executorId))

    /**
      * :: DeveloperApi ::
      * Request that the cluster manager kill the specified executors.
      *
      * @note This is an indication to the cluster manager that the application wishes to adjust
      *       its resource usage downwards. If the application wishes to replace the executors it kills
      *       through this method with new ones, it should follow up explicitly with a call to
      *       {{SparkContext#requestExecutors}}.
      * @return whether the request is received.
      */
    @DeveloperApi
    def killExecutors(executorIds: Seq[String]): Boolean = {
        schedulerBackend match {
            case b: CoarseGrainedSchedulerBackend =>
                b.killExecutors(executorIds, replace = false, force = true).nonEmpty
            case _ =>
                logWarning("Killing executors is only supported in coarse-grained mode")
                false
        }
    }

    /** The version of Spark on which this application is running. */
    def version: String = SPARK_VERSION

    /**
      * Return a map from the slave to the max memory available for caching and the remaining
      * memory available for caching.
      */
    def getExecutorMemoryStatus: Map[String, (Long, Long)] = {
        assertNotStopped()
        env.blockManager.master.getMemoryStatus.map { case (blockManagerId, mem) =>
            (blockManagerId.host + ":" + blockManagerId.port, mem)
        }
    }

    /**
      * :: DeveloperApi ::
      * Return information about what RDDs are cached, if they are in mem or on disk, how much space
      * they take, etc.
      */
    @DeveloperApi
    def getRDDStorageInfo: Array[RDDInfo] = {
        getRDDStorageInfo(_ => true)
    }

    private[spark] def getRDDStorageInfo(filter: RDD[_] => Boolean): Array[RDDInfo] = {
        assertNotStopped()
        val rddInfos = persistentRdds.values.filter(filter).map(RDDInfo.fromRdd).toArray
        StorageUtils.updateRddInfo(rddInfos, getExecutorStorageStatus)
        rddInfos.filter(_.isCached)
    }

    /**
      * :: DeveloperApi ::
      * Return information about blocks stored in all of the slaves
      */
    @DeveloperApi
    @deprecated("This method may change or be removed in a future release.", "2.2.0")
    def getExecutorStorageStatus: Array[StorageStatus] = {
        assertNotStopped()
        env.blockManager.master.getStorageStatus
    }

    // Methods for creating shared variables

    /**
      * Returns an immutable map of RDDs that have marked themselves as persistent via cache() call.
      *
      * @note This does not necessarily mean the caching or computation was successful.
      */
    def getPersistentRDDs: Map[Int, RDD[_]] = persistentRdds.toMap

    /**
      * :: DeveloperApi ::
      * Return pools for fair scheduler
      */
    @DeveloperApi
    def getAllPools: Seq[Schedulable] = {
        assertNotStopped()
        // TODO(xiajunluan): We should take nested pools into account
        taskScheduler.rootPool.schedulableQueue.asScala.toSeq
    }

    /**
      * :: DeveloperApi ::
      * Return the pool associated with the given name, if one exists
      */
    @DeveloperApi
    def getPoolForName(pool: String): Option[Schedulable] = {
        assertNotStopped()
        Option(taskScheduler.rootPool.schedulableNameToSchedulable.get(pool))
    }

    /**
      * Adds a JAR dependency for all tasks to be executed on this `SparkContext` in the future.
      *
      * @param path can be either a local file, a file in HDFS (or other Hadoop-supported filesystems),
      *             an HTTP, HTTPS or FTP URI, or local:/path for a file on every worker node.
      */
    def addJar(path: String) {
        def addJarFile(file: File): String = {
            try {
                if (!file.exists()) {
                    throw new FileNotFoundException(s"Jar ${file.getAbsolutePath} not found")
                }
                if (file.isDirectory) {
                    throw new IllegalArgumentException(
                        s"Directory ${file.getAbsoluteFile} is not allowed for addJar")
                }
                env.rpcEnv.fileServer.addJar(file)
            } catch {
                case NonFatal(e) =>
                    logError(s"Failed to add $path to Spark environment", e)
                    null
            }
        }

        if (path == null) {
            logWarning("null specified as parameter to addJar")
        } else {
            val key = if (path.contains("\\")) {
                // For local paths with backslashes on Windows, URI throws an exception
                addJarFile(new File(path))
            } else {
                val uri = new URI(path)
                // SPARK-17650: Make sure this is a valid URL before adding it to the list of dependencies
                Utils.validateURL(uri)
                uri.getScheme match {
                    // A JAR file which exists only on the driver node
                    case null | "file" => addJarFile(new File(uri.getPath))
                    // A JAR file which exists locally on every worker node
                    case "local" => "file:" + uri.getPath
                    case _ => path
                }
            }
            if (key != null) {
                val timestamp = System.currentTimeMillis
                if (addedJars.putIfAbsent(key, timestamp).isEmpty) {
                    logInfo(s"Added JAR $path at $key with timestamp $timestamp")
                    postEnvironmentUpdate()
                }
            }
        }
    }

    /**
      * 任务计划程序就绪后，发布环境更新事件
      *
      * Post the environment update event once the task scheduler is ready
      */
    private def postEnvironmentUpdate() {
        if (taskScheduler != null) {
            val schedulingMode = getSchedulingMode.toString
            val addedJarPaths = addedJars.keys.toSeq
            val addedFilePaths = addedFiles.keys.toSeq
            val environmentDetails = SparkEnv.environmentDetails(conf, schedulingMode, addedJarPaths,
                addedFilePaths)
            val environmentUpdate = SparkListenerEnvironmentUpdate(environmentDetails)
            listenerBus.post(environmentUpdate)
        }
    }

    private[spark] def conf: SparkConf = _conf

    /**
      * 返回当前调度模式
      *
      * Return current scheduling mode
      */
    def getSchedulingMode: SchedulingMode.SchedulingMode = {
        assertNotStopped()
        taskScheduler.schedulingMode
    }

    private[spark] def taskScheduler: TaskScheduler = _taskScheduler

    private[spark] def taskScheduler_=(ts: TaskScheduler): Unit = {
        _taskScheduler = ts
    }

    /**
      * 返回添加到资源的jar文件列表。
      *
      * Returns a list of jar files that are added to resources.
      */
    def listJars(): Seq[String] = addedJars.keySet.toSeq

    /**
      * Set the thread-local property for overriding the call sites
      * of actions and RDDs.
      */
    def setCallSite(shortCallSite: String) {
        setLocalProperty(CallSite.SHORT_FORM, shortCallSite)
    }

    /**
      * Clear the thread-local property for overriding the call sites
      * of actions and RDDs.
      */
    def clearCallSite() {
        setLocalProperty(CallSite.SHORT_FORM, null)
        setLocalProperty(CallSite.LONG_FORM, null)
    }

    /**
      * Set a local property that affects jobs submitted from this thread, such as the Spark fair
      * scheduler pool. User-defined properties may also be set here. These properties are propagated
      * through to worker tasks and can be accessed there via
      * [[org.apache.spark.TaskContext#getLocalProperty]].
      *
      * These properties are inherited by child threads spawned from this thread. This
      * may have unexpected consequences when working with thread pools. The standard java
      * implementation of thread pools have worker threads spawn other worker threads.
      * As a result, local properties may propagate unpredictably.
      */
    def setLocalProperty(key: String, value: String) {
        if (value == null) {
            localProperties.get.remove(key)
        } else {
            localProperties.get.setProperty(key, value)
        }
    }

    /**
      * Run a job on all partitions in an RDD and return the results in an array. The function
      * that is run against each partition additionally takes `TaskContext` argument.
      *
      * @param rdd  target RDD to run tasks on
      * @param func a function to run on each partition of the RDD
      * @return in-memory collection with a result of the job (each collection element will contain
      *         a result from one partition)
      */
    def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
        runJob(rdd, func, 0 until rdd.partitions.length)
    }

    /**
      * Run a function on a given set of partitions in an RDD and return the results as an array.
      * The function that is run against each partition additionally takes `TaskContext` argument.
      *
      * @param rdd        target RDD to run tasks on
      * @param func       a function to run on each partition of the RDD
      * @param partitions set of partitions to run on; some jobs may not want to compute on all
      *                   partitions of the target RDD, e.g. for operations like `first()`
      * @return in-memory collection with a result of the job (each collection element will contain
      *         a result from one partition)
      */
    def runJob[T, U: ClassTag](
                                  rdd: RDD[T],
                                  func: (TaskContext, Iterator[T]) => U,
                                  partitions: Seq[Int]): Array[U] = {
        val results = new Array[U](partitions.size)
        runJob[T, U](rdd, func, partitions, (index, res) => results(index) = res)
        results
    }

    /**
      * Run a function on a given set of partitions in an RDD and pass the results to the given
      * handler function. This is the main entry point for all actions in Spark.
      *
      * @param rdd           target RDD to run tasks on
      * @param func          a function to run on each partition of the RDD
      * @param partitions    set of partitions to run on; some jobs may not want to compute on all
      *                      partitions of the target RDD, e.g. for operations like `first()`
      * @param resultHandler callback to pass each result to
      */
    def runJob[T, U: ClassTag](
                                  rdd: RDD[T],
                                  func: (TaskContext, Iterator[T]) => U,
                                  partitions: Seq[Int],
                                  resultHandler: (Int, U) => Unit): Unit = {
        if (stopped.get()) {
            throw new IllegalStateException("SparkContext has been shutdown")
        }
        val callSite = getCallSite
        val cleanedFunc = clean(func)
        logInfo("Starting job: " + callSite.shortForm)
        if (conf.getBoolean("org.apache.spark.logLineage", false)) {
            logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
        }
        dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
        progressBar.foreach(_.finishAll())
        rdd.doCheckpoint()
    }

    private[spark] def progressBar: Option[ConsoleProgressBar] = _progressBar

    /**
      * Clean a closure to make it ready to serialized and send to tasks
      * (removes unreferenced variables in $outer's, updates REPL variables)
      * If <tt>checkSerializable</tt> is set, <tt>clean</tt> will also proactively
      * check to see if <tt>f</tt> is serializable and throw a <tt>SparkException</tt>
      * if not.
      *
      * @param f                 the closure to clean
      * @param checkSerializable whether or not to immediately check <tt>f</tt> for serializability
      * @throws SparkException if <tt>checkSerializable</tt> is set but <tt>f</tt> is not
      *                        serializable
      * @return the cleaned closure
      */
    private[spark] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
        ClosureCleaner.clean(f, checkSerializable)
        f
    }

    /**
      * Run a job on all partitions in an RDD and return the results in an array.
      *
      * @param rdd  target RDD to run tasks on
      * @param func a function to run on each partition of the RDD
      * @return in-memory collection with a result of the job (each collection element will contain
      *         a result from one partition)
      */
    def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
        runJob(rdd, func, 0 until rdd.partitions.length)
    }

    /**
      * Run a function on a given set of partitions in an RDD and return the results as an array.
      *
      * @param rdd        target RDD to run tasks on
      * @param func       a function to run on each partition of the RDD
      * @param partitions set of partitions to run on; some jobs may not want to compute on all
      *                   partitions of the target RDD, e.g. for operations like `first()`
      * @return in-memory collection with a result of the job (each collection element will contain
      *         a result from one partition)
      */
    def runJob[T, U: ClassTag](
                                  rdd: RDD[T],
                                  func: Iterator[T] => U,
                                  partitions: Seq[Int]): Array[U] = {
        val cleanedFunc = clean(func)
        runJob(rdd, (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it), partitions)
    }

    /**
      * Run a job on all partitions in an RDD and pass the results to a handler function. The function
      * that is run against each partition additionally takes `TaskContext` argument.
      *
      * @param rdd              target RDD to run tasks on
      * @param processPartition a function to run on each partition of the RDD
      * @param resultHandler    callback to pass each result to
      */
    def runJob[T, U: ClassTag](
                                  rdd: RDD[T],
                                  processPartition: (TaskContext, Iterator[T]) => U,
                                  resultHandler: (Int, U) => Unit) {
        runJob[T, U](rdd, processPartition, 0 until rdd.partitions.length, resultHandler)
    }

    /**
      * Run a job on all partitions in an RDD and pass the results to a handler function.
      *
      * @param rdd              target RDD to run tasks on
      * @param processPartition a function to run on each partition of the RDD
      * @param resultHandler    callback to pass each result to
      */
    def runJob[T, U: ClassTag](
                                  rdd: RDD[T],
                                  processPartition: Iterator[T] => U,
                                  resultHandler: (Int, U) => Unit) {
        val processFunc = (context: TaskContext, iter: Iterator[T]) => processPartition(iter)
        runJob[T, U](rdd, processFunc, 0 until rdd.partitions.length, resultHandler)
    }

    /**
      * :: DeveloperApi ::
      * Run a job that can return approximate results.
      *
      * @param rdd       target RDD to run tasks on
      * @param func      a function to run on each partition of the RDD
      * @param evaluator `ApproximateEvaluator` to receive the partial results
      * @param timeout   maximum time to wait for the job, in milliseconds
      * @return partial result (how partial depends on whether the job was finished before or
      *         after timeout)
      */
    @DeveloperApi
    def runApproximateJob[T, U, R](
                                      rdd: RDD[T],
                                      func: (TaskContext, Iterator[T]) => U,
                                      evaluator: ApproximateEvaluator[U, R],
                                      timeout: Long): PartialResult[R] = {
        assertNotStopped()
        val callSite = getCallSite
        logInfo("Starting job: " + callSite.shortForm)
        val start = System.nanoTime
        val cleanedFunc = clean(func)
        val result = dagScheduler.runApproximateJob(rdd, cleanedFunc, evaluator, callSite, timeout,
            localProperties.get)
        logInfo(
            "Job finished: " + callSite.shortForm + ", took " + (System.nanoTime - start) / 1e9 + " s")
        result
    }

    /**
      * 提交一个作业以执行，并返回一个包含结果的FutureJob。
      *
      * Submit a job for execution and return a FutureJob holding the result.
      *
      * @param rdd              target RDD to run tasks on
      * @param processPartition a function to run on each partition of the RDD
      * @param partitions       set of partitions to run on; some jobs may not want to compute on all
      *                         partitions of the target RDD, e.g. for operations like `first()`
      * @param resultHandler    callback to pass each result to
      * @param resultFunc       function to be executed when the result is ready
      */
    def submitJob[T, U, R](
                              rdd: RDD[T],
                              processPartition: Iterator[T] => U,
                              partitions: Seq[Int],
                              resultHandler: (Int, U) => Unit,
                              resultFunc: => R): SimpleFutureAction[R] = {
        assertNotStopped()
        val cleanF = clean(processPartition)
        val callSite = getCallSite
        val waiter = dagScheduler.submitJob(
            rdd,
            (context: TaskContext, iter: Iterator[T]) => cleanF(iter),
            partitions,
            callSite,
            resultHandler,
            localProperties.get)
        new SimpleFutureAction(waiter, resultFunc)
    }

    /**
      * Cancel active jobs for the specified group. See `org.apache.org.apache.spark.SparkContext.setJobGroup`
      * for more information.
      */
    def cancelJobGroup(groupId: String) {
        assertNotStopped()
        dagScheduler.cancelJobGroup(groupId)
    }

    /** Cancel all jobs that have been scheduled or are running. */
    def cancelAllJobs() {
        assertNotStopped()
        dagScheduler.cancelAllJobs()
    }

    /**
      * Cancel a given job if it's scheduled or running.
      *
      * @param jobId  the job ID to cancel
      * @param reason optional reason for cancellation
      * @note Throws `InterruptedException` if the cancel message cannot be sent
      */
    def cancelJob(jobId: Int, reason: String): Unit = {
        dagScheduler.cancelJob(jobId, Option(reason))
    }

    /**
      * Cancel a given job if it's scheduled or running.
      *
      * @param jobId the job ID to cancel
      * @note Throws `InterruptedException` if the cancel message cannot be sent
      */
    def cancelJob(jobId: Int): Unit = {
        dagScheduler.cancelJob(jobId, None)
    }

    /**
      * Cancel a given stage and all jobs associated with it.
      *
      * @param stageId the stage ID to cancel
      * @param reason  reason for cancellation
      * @note Throws `InterruptedException` if the cancel message cannot be sent
      */
    def cancelStage(stageId: Int, reason: String): Unit = {
        dagScheduler.cancelStage(stageId, Option(reason))
    }

    /**
      * Cancel a given stage and all jobs associated with it.
      *
      * @param stageId the stage ID to cancel
      * @note Throws `InterruptedException` if the cancel message cannot be sent
      */
    def cancelStage(stageId: Int): Unit = {
        dagScheduler.cancelStage(stageId, None)
    }

    /**
      * Kill and reschedule the given task attempt. Task ids can be obtained from the Spark UI
      * or through SparkListener.onTaskStart.
      *
      * @param taskId          the task ID to kill. This id uniquely identifies the task attempt.
      * @param interruptThread whether to interrupt the thread running the task.
      * @param reason          the reason for killing the task, which should be a short string. If a task
      *                        is killed multiple times with different reasons, only one reason will be reported.
      * @return Whether the task was successfully killed.
      */
    def killTaskAttempt(
                           taskId: Long,
                           interruptThread: Boolean = true,
                           reason: String = "killed via SparkContext.killTaskAttempt"): Boolean = {
        dagScheduler.killTaskAttempt(taskId, interruptThread, reason)
    }

    def getCheckpointDir: Option[String] = checkpointDir

    /**
      * Set the directory under which RDDs are going to be checkpointed.
      *
      * @param directory path to the directory where checkpoint files will be stored
      *                  (must be HDFS path if running in cluster)
      */
    def setCheckpointDir(directory: String) {

        // If we are running on a cluster, log a warning if the directory is local.
        // Otherwise, the driver may attempt to reconstruct the checkpointed RDD from
        // its own local file system, which is incorrect because the checkpoint files
        // are actually on the executor machines.
        if (!isLocal && Utils.nonLocalPaths(directory).isEmpty) {
            logWarning("Spark is not running in local mode, therefore the checkpoint directory " +
                s"must not be on the local filesystem. Directory '$directory' " +
                "appears to be on the local filesystem.")
        }

        checkpointDir = Option(directory).map { dir =>
            val path = new Path(dir, UUID.randomUUID().toString)
            val fs = path.getFileSystem(hadoopConfiguration)
            fs.mkdirs(path)
            fs.getFileStatus(path).getPath.toString
        }
    }

    protected[spark] def checkpointFile[T: ClassTag](path: String): RDD[T] = withScope {
        new ReliableCheckpointRDD[T](this, path)
    }

    /**
      * Alternative constructor that allows setting common Spark properties directly
      *
      * @param master  Cluster URL to connect to (e.g. mesos://host:port, org.apache.spark://host:port, local[4]).
      * @param appName A name for your application, to display on the cluster web UI.
      */
    private[spark] def this(master: String, appName: String) =
        this(master, appName, null, Nil, Map())

    /**
      * Alternative constructor that allows setting common Spark properties directly
      *
      * @param master    Cluster URL to connect to (e.g. mesos://host:port, org.apache.spark://host:port, local[4]).
      * @param appName   A name for your application, to display on the cluster web UI.
      * @param sparkHome Location where Spark is installed on cluster nodes.
      */
    private[spark] def this(master: String, appName: String, sparkHome: String) =
        this(master, appName, sparkHome, Nil, Map())

    /**
      * Alternative constructor that allows setting common Spark properties directly
      *
      * @param master    Cluster URL to connect to (e.g. mesos://host:port, org.apache.spark://host:port, local[4]).
      * @param appName   A name for your application, to display on the cluster web UI.
      * @param sparkHome Location where Spark is installed on cluster nodes.
      * @param jars      Collection of JARs to send to the cluster. These can be paths on the local file
      *                  system or HDFS, HTTP, HTTPS, or FTP URLs.
      */
    private[spark] def this(master: String, appName: String, sparkHome: String, jars: Seq[String]) =
        this(master, appName, sparkHome, jars, Map())

    private[spark] def isEventLogEnabled: Boolean = _conf.getBoolean("org.apache.spark.eventLog.enabled", false)

    private[spark] def eventLogDir: Option[URI] = _eventLogDir

    private[spark] def eventLogCodec: Option[String] = _eventLogCodec

    // This function allows components created by SparkEnv to be mocked in unit tests:
    private[spark] def createSparkEnv(
                                         conf: SparkConf,
                                         isLocal: Boolean,
                                         listenerBus: LiveListenerBus): SparkEnv = {
        SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master))
    }

    def master: String = _conf.get("org.apache.spark.master")

    private[spark] def jobProgressListener: JobProgressListener = _jobProgressListener

    private[spark] def ui: Option[SparkUI] = _ui

    private[spark] def executorMemory: Int = _executorMemory

    private[spark] def eventLogger: Option[EventLoggingListener] = _eventLogger

    private[spark] def executorAllocationManager: Option[ExecutorAllocationManager] =
        _executorAllocationManager

    private def warnSparkMem(value: String): String = {
        logWarning("Using SPARK_MEM to set amount of memory to use per executor process is " +
            "deprecated, please use org.apache.spark.executor.memory instead.")
        value
    }

    private def warnDeprecatedVersions(): Unit = {
        val javaVersion = System.getProperty("java.version").split("[+.\\-]+", 3)
        if (scala.util.Properties.releaseVersion.exists(_.startsWith("2.10"))) {
            logWarning("Support for Scala 2.10 is deprecated as of Spark 2.1.0")
        }
    }

    /**
      * Called by the web UI to obtain executor thread dumps.  This method may be expensive.
      * Logs an error and returns None if we failed to obtain a thread dump, which could occur due
      * to an executor being dead or unresponsive or due to network issues while sending the thread
      * dump message back to the driver.
      */
    private[spark] def getExecutorThreadDump(executorId: String): Option[Array[ThreadStackTrace]] = {
        try {
            if (executorId == SparkContext.DRIVER_IDENTIFIER) {
                Some(Utils.getThreadDump())
            } else {
                val endpointRef = env.blockManager.master.getExecutorEndpointRef(executorId).get
                Some(endpointRef.askSync[Array[ThreadStackTrace]](TriggerThreadDump))
            }
        } catch {
            case e: Exception =>
                logError(s"Exception getting thread dump from executor $executorId", e)
                None
        }
    }

    private[spark] def env: SparkEnv = _env

    private[spark] def getLocalProperties: Properties = localProperties.get()

    private[spark] def setLocalProperties(props: Properties) {
        localProperties.set(props)
    }

    private[spark] def getExecutorIds(): Seq[String] = {
        schedulerBackend match {
            case b: CoarseGrainedSchedulerBackend =>
                b.getExecutorIds()
            case _ =>
                logWarning("Requesting executors is only supported in coarse-grained mode")
                Nil
        }
    }

    /**
      * Request that the cluster manager kill the specified executor without adjusting the
      * application resource requirements.
      *
      * The effect is that a new executor will be launched in place of the one killed by
      * this request. This assumes the cluster manager will automatically and eventually
      * fulfill all missing application resource requests.
      *
      * @note The replace is by no means guaranteed; another application on the same cluster
      *       can steal the window of opportunity and acquire this application's resources in the
      *       mean time.
      * @return whether the request is received.
      */
    private[spark] def killAndReplaceExecutor(executorId: String): Boolean = {
        schedulerBackend match {
            case b: CoarseGrainedSchedulerBackend =>
                b.killExecutors(Seq(executorId), replace = true, force = true).nonEmpty
            case _ =>
                logWarning("Killing executors is only supported in coarse-grained mode")
                false
        }
    }

    private[spark] def schedulerBackend: SchedulerBackend = _schedulerBackend

    /**
      * Gets the locality information associated with the partition in a particular rdd
      *
      * @param rdd       of interest
      * @param partition to be looked up for locality
      * @return list of preferred locations for the partition
      */
    private[spark] def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
        dagScheduler.getPreferredLocs(rdd, partition)
    }

    /**
      * Register an RDD to be persisted in memory and/or disk storage
      */
    private[spark] def persistRDD(rdd: RDD[_]) {
        persistentRdds(rdd.id) = rdd
    }

    /**
      * Unpersist an RDD from memory and/or disk storage
      */
    private[spark] def unpersistRDD(rddId: Int, blocking: Boolean = true) {
        env.blockManager.master.removeRdd(rddId, blocking)
        persistentRdds.remove(rddId)
        listenerBus.post(SparkListenerUnpersistRDD(rddId))
    }

    /**
      * When stopping SparkContext inside Spark components, it's easy to cause dead-lock since Spark
      * may wait for some internal threads to finish. It's better to use this method to stop
      * SparkContext instead.
      */
    private[spark] def stopInNewThread(): Unit = {
        new Thread("stop-org.apache.spark-context") {
            setDaemon(true)

            override def run(): Unit = {
                try {
                    SparkContext.this.stop()
                } catch {
                    case e: Throwable =>
                        logError(e.getMessage, e)
                        throw e
                }
            }
        }.start()
    }

    /**
      * Get Spark's home location from either a value set through the constructor,
      * or the org.apache.spark.home Java property, or the SPARK_HOME environment variable
      * (in that order of preference). If neither of these is set, return None.
      */
    private[spark] def getSparkHome(): Option[String] = {
        conf.getOption("org.apache.spark.home").orElse(Option(System.getenv("SPARK_HOME")))
    }

    /**
      * Submit a map stage for execution. This is currently an internal API only, but might be
      * promoted to DeveloperApi in the future.
      */
    private[spark] def submitMapStage[K, V, C](dependency: ShuffleDependency[K, V, C])
    : SimpleFutureAction[MapOutputStatistics] = {
        assertNotStopped()
        val callSite = getCallSite()
        var result: MapOutputStatistics = null
        val waiter = dagScheduler.submitMapStage(
            dependency,
            (r: MapOutputStatistics) => {
                result = r
            },
            callSite,
            localProperties.get)
        new SimpleFutureAction[MapOutputStatistics](waiter, result)
    }

    private[spark] def dagScheduler: DAGScheduler = _dagScheduler

    private[spark] def dagScheduler_=(ds: DAGScheduler): Unit = {
        _dagScheduler = ds
    }

    /**
      * Capture the current user callsite and return a formatted version for printing. If the user
      * has overridden the call site using `setCallSite()`, this will return the user's version.
      */
    private[spark] def getCallSite(): CallSite = {
        lazy val callSite = Utils.getCallSite()
        CallSite(
            Option(getLocalProperty(CallSite.SHORT_FORM)).getOrElse(callSite.shortForm),
            Option(getLocalProperty(CallSite.LONG_FORM)).getOrElse(callSite.longForm)
        )
    }

    /**
      * Get a local property set in this thread, or null if it is missing. See
      * `org.apache.org.apache.spark.SparkContext.setLocalProperty`.
      */
    def getLocalProperty(key: String): String =
        Option(localProperties.get).map(_.getProperty(key)).orNull

    /**
      * Set the thread-local property for overriding the call sites
      * of actions and RDDs.
      */
    private[spark] def setCallSite(callSite: CallSite) {
        setLocalProperty(CallSite.SHORT_FORM, callSite.shortForm)
        setLocalProperty(CallSite.LONG_FORM, callSite.longForm)
    }

    private[spark] def newShuffleId(): Int = nextShuffleId.getAndIncrement()

    /**
      * 注册一个新的RDD，返回其RDD ID。
      *
      * Register a new RDD, returning its RDD ID
      */
    private[spark] def newRddId(): Int = nextRddId.getAndIncrement()

    /**
      * Registers listeners specified in org.apache.spark.extraListeners, then starts the listener bus.
      * This should be called after all internal listeners have been registered with the listener bus
      * (e.g. after the web UI and event logging listeners have been registered).
      */
    private def setupAndStartListenerBus(): Unit = {
        // Use reflection to instantiate listeners specified via `org.apache.spark.extraListeners`
        try {
            val listenerClassNames: Seq[String] =
                conf.get("org.apache.spark.extraListeners", "").split(',').map(_.trim).filter(_ != "")
            for (className <- listenerClassNames) {
                // Use reflection to find the right constructor
                val constructors = {
                    val listenerClass = Utils.classForName(className)
                    listenerClass
                        .getConstructors
                        .asInstanceOf[Array[Constructor[_ <: SparkListenerInterface]]]
                }
                val constructorTakingSparkConf = constructors.find { c =>
                    c.getParameterTypes.sameElements(Array(classOf[SparkConf]))
                }
                lazy val zeroArgumentConstructor = constructors.find { c =>
                    c.getParameterTypes.isEmpty
                }
                val listener: SparkListenerInterface = {
                    if (constructorTakingSparkConf.isDefined) {
                        constructorTakingSparkConf.get.newInstance(conf)
                    } else if (zeroArgumentConstructor.isDefined) {
                        zeroArgumentConstructor.get.newInstance()
                    } else {
                        throw new SparkException(
                            s"$className did not have a zero-argument constructor or a" +
                                " single-argument constructor that accepts SparkConf. Note: if the class is" +
                                " defined inside of another Scala class, then its constructors may accept an" +
                                " implicit parameter that references the enclosing class; in this case, you must" +
                                " define the listener as a top-level class in order to prevent this extra" +
                                " parameter from breaking Spark's ability to find a valid constructor.")
                    }
                }
                listenerBus.addListener(listener)
                logInfo(s"Registered listener $className")
            }
        } catch {
            case e: Exception =>
                try {
                    stop()
                } finally {
                    throw new SparkException(s"Exception when registering SparkListener", e)
                }
        }

        listenerBus.start()
        _listenerBusStarted = true
    }

    /**
      * 关闭SparkContext。
      *
      * Shut down the SparkContext.
      */
    def stop(): Unit = {
        if (LiveListenerBus.withinListenerThread.value) {
            throw new SparkException(
                s"Cannot stop SparkContext within listener thread of ${LiveListenerBus.name}")
        }
        // Use the stopping variable to ensure no contention for the stop scenario.
        // Still track the stopped variable for use elsewhere in the code.
        if (!stopped.compareAndSet(false, true)) {
            logInfo("SparkContext already stopped.")
            return
        }
        if (_shutdownHookRef != null) {
            ShutdownHookManager.removeShutdownHook(_shutdownHookRef)
        }

        Utils.tryLogNonFatalError {
            postApplicationEnd()
        }
        Utils.tryLogNonFatalError {
            _ui.foreach(_.stop())
        }
        if (env != null) {
            Utils.tryLogNonFatalError {
                env.metricsSystem.report()
            }
        }
        Utils.tryLogNonFatalError {
            _cleaner.foreach(_.stop())
        }
        Utils.tryLogNonFatalError {
            _executorAllocationManager.foreach(_.stop())
        }
        if (_listenerBusStarted) {
            Utils.tryLogNonFatalError {
                listenerBus.stop()
                _listenerBusStarted = false
            }
        }
        Utils.tryLogNonFatalError {
            _eventLogger.foreach(_.stop())
        }
        if (_dagScheduler != null) {
            Utils.tryLogNonFatalError {
                _dagScheduler.stop()
            }
            _dagScheduler = null
        }
        if (env != null && _heartbeatReceiver != null) {
            Utils.tryLogNonFatalError {
                env.rpcEnv.stop(_heartbeatReceiver)
            }
        }
        Utils.tryLogNonFatalError {
            _progressBar.foreach(_.stop())
        }
        _taskScheduler = null
        // TODO: Cache.stop()?
        if (_env != null) {
            Utils.tryLogNonFatalError {
                _env.stop()
            }
            SparkEnv.set(null)
        }
        // Clear this `InheritableThreadLocal`, or it will still be inherited in child threads even this
        // `SparkContext` is stopped.
        localProperties.remove()
        // Unset YARN mode system env variable, to allow switching between cluster types.
        System.clearProperty("SPARK_YARN_MODE")
        SparkContext.clearActiveContext()
        logInfo("Successfully stopped SparkContext")
    }

    /** Post the application end event */
    private def postApplicationEnd() {
        listenerBus.post(SparkListenerApplicationEnd(System.currentTimeMillis))
    }

    /** Post the application start event */
    private def postApplicationStart() {
        // Note: this code assumes that the task scheduler has been initialized and has contacted
        // the cluster manager to get an application ID (in case the cluster manager provides one).
        listenerBus.post(SparkListenerApplicationStart(appName, Some(applicationId),
            startTime, sparkUser, applicationAttemptId, schedulerBackend.getDriverLogUrls))
    }

    def appName: String = _conf.get("org.apache.spark.app.name")

    /**
      * A unique identifier for the Spark application.
      * Its format depends on the scheduler implementation.
      * (i.e.
      * in case of local org.apache.spark app something like 'local-1433865536131'
      * in case of YARN something like 'application_1433865536131_34483'
      * )
      */
    def applicationId: String = _applicationId

    def applicationAttemptId: Option[String] = _applicationAttemptId

    // In order to prevent multiple SparkContexts from being active at the same time, mark this
    // context as having finished construction.
    // NOTE: this must be placed at the end of the SparkContext constructor.
    SparkContext.setActiveContext(this, allowMultipleContexts)
}

/**
  * SparkContext对象包含许多隐式转换和参数，用于各种Spark功能。
  *
  * The SparkContext object contains a number of implicit conversions and parameters for use with
  * various Spark features.
  */
object SparkContext extends Logging {
    private val VALID_LOG_LEVELS =
        Set("ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN")

    /**
      * 保护对跟踪SparkContext构造的全局变量的访问的锁。
      *
      * Lock that guards access to global variables that track SparkContext construction.
      */
    private val SPARK_CONTEXT_CONSTRUCTOR_LOCK = new Object()

    /**
      * 活跃的、完整的SparkContext。如果没有激活的SparkContext，则为“null”。
      *
      * The active, fully-constructed SparkContext.  If no SparkContext is active, then this is `null`.
      *
      * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK.
      */
    private val activeContext: AtomicReference[SparkContext] =
        new AtomicReference[SparkContext](null)
    private[spark] val SPARK_JOB_DESCRIPTION = "org.apache.spark.job.description"
    private[spark] val SPARK_JOB_GROUP_ID = "org.apache.spark.jobGroup.id"
    private[spark] val SPARK_JOB_INTERRUPT_ON_CANCEL = "org.apache.spark.job.interruptOnCancel"
    private[spark] val RDD_SCOPE_KEY = "org.apache.spark.rdd.scope"
    private[spark] val RDD_SCOPE_NO_OVERRIDE_KEY = "org.apache.spark.rdd.scope.noOverride"

    /**
      * 集群模式driver运行在某个executor上
      * Executor id for the driver.  In earlier versions of Spark, this was `<driver>`, but this was
      * changed to `driver` because the angle brackets caused escaping issues in URLs and XML (see
      * SPARK-6716 for more details).
      */
    private[spark] val DRIVER_IDENTIFIER = "driver"

    /**
      * 驱动程序标识符的旧版本，为向后兼容而保留。
      *
      * Legacy version of DRIVER_IDENTIFIER, retained for backwards-compatibility.
      */
    private[spark] val LEGACY_DRIVER_IDENTIFIER = "<driver>"

    /**
      * Points to a partially-constructed SparkContext if some thread is in the SparkContext
      * constructor, or `None` if no SparkContext is being constructed.
      *
      * Access to this field is guarded by SPARK_CONTEXT_CONSTRUCTOR_LOCK
      */
    private var contextBeingConstructed: Option[SparkContext] = None

    /**
      * This function may be used to get or instantiate a SparkContext and register it as a
      * singleton object. Because we can only have one active SparkContext per JVM,
      * this is useful when applications may wish to share a SparkContext.
      *
      * @note This function cannot be used to create multiple SparkContext instances
      *       even if multiple contexts are allowed.
      * @param config `SparkConfig` that will be used for initialisation of the `SparkContext`
      * @return current `SparkContext` (or a new one if it wasn't created before the function call)
      */
    def getOrCreate(config: SparkConf): SparkContext = {
        // Synchronize to ensure that multiple create requests don't trigger an exception
        // from assertNoOtherContextIsRunning within setActiveContext
        SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
            if (activeContext.get() == null) {
                setActiveContext(new SparkContext(config), allowMultipleContexts = false)
            } else {
                if (config.getAll.nonEmpty) {
                    logWarning("Using an existing SparkContext; some configuration may not take effect.")
                }
            }
            activeContext.get()
        }
    }

    /**
      * Called at the end of the SparkContext constructor to ensure that no other SparkContext has
      * raced with this constructor and started.
      */
    private[spark] def setActiveContext(
                                           sc: SparkContext,
                                           allowMultipleContexts: Boolean): Unit = {
        SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
            assertNoOtherContextIsRunning(sc, allowMultipleContexts)
            contextBeingConstructed = None
            activeContext.set(sc)
        }
    }

    /**
      * This function may be used to get or instantiate a SparkContext and register it as a
      * singleton object. Because we can only have one active SparkContext per JVM,
      * this is useful when applications may wish to share a SparkContext.
      *
      * This method allows not passing a SparkConf (useful if just retrieving).
      *
      * @note This function cannot be used to create multiple SparkContext instances
      *       even if multiple contexts are allowed.
      * @return current `SparkContext` (or a new one if wasn't created before the function call)
      */
    def getOrCreate(): SparkContext = {
        SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
            if (activeContext.get() == null) {
                setActiveContext(new SparkContext(), allowMultipleContexts = false)
            }
            activeContext.get()
        }
    }

    /**
      * Find the JAR that contains the class of a particular object, to make it easy for users
      * to pass their JARs to SparkContext. In most cases you can call jarOfObject(this) in
      * your driver program.
      *
      * @param obj reference to an instance which class should be inside of the jar
      * @return jar that contains the class of the instance, `None` if not found
      */
    def jarOfObject(obj: AnyRef): Option[String] = jarOfClass(obj.getClass)

    /**
      * Find the JAR from which a given class was loaded, to make it easy for users to pass
      * their JARs to SparkContext.
      *
      * @param cls class that should be inside of the jar
      * @return jar that contains the Class, `None` if not found
      */
    def jarOfClass(cls: Class[_]): Option[String] = {
        val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
        if (uri != null) {
            val uriStr = uri.toString
            if (uriStr.startsWith("jar:file:")) {
                // URI will be of the form "jar:file:/path/foo.jar!/package/cls.class",
                // so pull out the /path/foo.jar
                Some(uriStr.substring("jar:file:".length, uriStr.indexOf('!')))
            } else {
                None
            }
        } else {
            None
        }
    }

    /** Return the current active [[SparkContext]] if any. */
    private[spark] def getActive: Option[SparkContext] = {
        SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
            Option(activeContext.get())
        }
    }

    /**
      * Called at the beginning of the SparkContext constructor to ensure that no SparkContext is
      * running.  Throws an exception if a running context is detected and logs a warning if another
      * thread is constructing a SparkContext.  This warning is necessary because the current locking
      * scheme prevents us from reliably distinguishing between cases where another context is being
      * constructed and cases where another constructor threw an exception.
      */
    private[spark] def markPartiallyConstructed(
                                                   sc: SparkContext,
                                                   allowMultipleContexts: Boolean): Unit = {
        SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
            assertNoOtherContextIsRunning(sc, allowMultipleContexts)
            contextBeingConstructed = Some(sc)
        }
    }

    private implicit def arrayToArrayWritable[T <% Writable : ClassTag](arr: Traversable[T])
    : ArrayWritable = {
        def anyToWritable[U <% Writable](u: U): Writable = u

        new ArrayWritable(classTag[T].runtimeClass.asInstanceOf[Class[Writable]],
            arr.map(x => anyToWritable(x)).toArray)
    }

    /**
      * Called to ensure that no other SparkContext is running in this JVM.
      *
      * Throws an exception if a running context is detected and logs a warning if another thread is
      * constructing a SparkContext.  This warning is necessary because the current locking scheme
      * prevents us from reliably distinguishing between cases where another context is being
      * constructed and cases where another constructor threw an exception.
      */
    private def assertNoOtherContextIsRunning(
                                                 sc: SparkContext,
                                                 allowMultipleContexts: Boolean): Unit = {
        SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
            Option(activeContext.get()).filter(_ ne sc).foreach { ctx =>
                val errMsg = "Only one SparkContext may be running in this JVM (see SPARK-2243)." +
                    " To ignore this error, set org.apache.spark.driver.allowMultipleContexts = true. " +
                    s"The currently running SparkContext was created at:\n${ctx.creationSite.longForm}"
                val exception = new SparkException(errMsg)
                if (allowMultipleContexts) {
                    logWarning("Multiple running SparkContexts detected in the same JVM!", exception)
                } else {
                    throw exception
                }
            }

            contextBeingConstructed.filter(_ ne sc).foreach { otherContext =>
                // Since otherContext might point to a partially-constructed context, guard against
                // its creationSite field being null:
                val otherContextCreationSite =
                    Option(otherContext.creationSite).map(_.longForm).getOrElse("unknown location")
                val warnMsg = "Another SparkContext is being constructed (or threw an exception in its" +
                    " constructor).  This may indicate an error, since only one SparkContext may be" +
                    " running in this JVM (see SPARK-2243)." +
                    s" The other SparkContext was created at:\n$otherContextCreationSite"
                logWarning(warnMsg)
            }
        }
    }

    /**
      * Clears the active SparkContext metadata.  This is called by `SparkContext#stop()`.  It's
      * also called in unit tests to prevent a flood of warnings from test suites that don't / can't
      * properly clean up their SparkContexts.
      */
    private[spark] def clearActiveContext(): Unit = {
        SPARK_CONTEXT_CONSTRUCTOR_LOCK.synchronized {
            activeContext.set(null)
        }
    }

    /**
      * 创建一个修改版本的SparkConf，参数可以单独传递给SparkContext，使SparkContext的构造函数更容易编写。
      * 这将忽略作为默认值null传递的参数，而不是像SparkConf那样抛出异常。
      *
      * Creates a modified version of a SparkConf with the parameters that can be passed separately
      * to SparkContext, to make it easier to write SparkContext's constructors. This ignores
      * parameters that are passed as the default value of null, instead of throwing an exception
      * like SparkConf would.
      */
    private[spark] def updatedConf(
                                      conf: SparkConf,
                                      master: String,
                                      appName: String,
                                      sparkHome: String = null,
                                      jars: Seq[String] = Nil,
                                      environment: Map[String, String] = Map()): SparkConf = {
        val res = conf.clone()
        res.setMaster(master)
        res.setAppName(appName)
        if (sparkHome != null) {
            res.setSparkHome(sparkHome)
        }
        if (jars != null && !jars.isEmpty) {
            res.setJars(jars)
        }
        res.setExecutorEnv(environment.toSeq)
        res
    }

    /**
      * 在本地模式下执行时使用的驱动程序内核数，否则为0。
      *
      * The number of driver cores to use for execution in local mode, 0 otherwise.
      */
    private[spark] def numDriverCores(master: String): Int = {
        def convertToInt(threads: String): Int = {
            if (threads == "*") Runtime.getRuntime.availableProcessors() else threads.toInt
        }

        master match {
            case "local" => 1
            case SparkMasterRegex.LOCAL_N_REGEX(threads) => convertToInt(threads)
            case SparkMasterRegex.LOCAL_N_FAILURES_REGEX(threads, _) => convertToInt(threads)
            case _ => 0 // driver is not used for execution
        }
    }

    /**
      * 基于给定的主URL创建任务计划程序,返回调度程序后端和任务调度程序的2元组。
      *
      * Create a task scheduler based on a given master URL.
      * Return a 2-tuple of the scheduler backend and the task scheduler.
      */
    private def createTaskScheduler(
                                       sc: SparkContext,
                                       master: String,
                                       deployMode: String): (SchedulerBackend, TaskScheduler) = {
        import SparkMasterRegex._

        // 在本地运行时，不要尝试在失败时重新执行任务。
        // When running locally, don't try to re-execute tasks on failure.
        val MAX_LOCAL_TASK_FAILURES = 1

        master match {
            case "local" =>
                val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
                val backend = new LocalSchedulerBackend(sc.getConf, scheduler, 1)
                scheduler.initialize(backend)
                (backend, scheduler)

            case LOCAL_N_REGEX(threads) =>
                def localCpuCount: Int = Runtime.getRuntime.availableProcessors()

                // local[*]估计机器上的内核数；local[N]正好使用N个线程。
                // local[*] estimates the number of cores on the machine; local[N] uses exactly N threads.
                val threadCount = if (threads == "*") localCpuCount else threads.toInt
                if (threadCount <= 0) {
                    throw new SparkException(s"Asked to run locally with $threadCount threads")
                }
                val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
                val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
                scheduler.initialize(backend)
                (backend, scheduler)

            case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
                def localCpuCount: Int = Runtime.getRuntime.availableProcessors()

                // local[*，M]表示计算机上出现M个故障的内核数,local[N，M]表示正好有N个线程有M个失败。
                // local[*, M] means the number of cores on the computer with M failures
                // local[N, M] means exactly N threads with M failures
                val threadCount = if (threads == "*") localCpuCount else threads.toInt
                val scheduler = new TaskSchedulerImpl(sc, maxFailures.toInt, isLocal = true)
                val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
                scheduler.initialize(backend)
                (backend, scheduler)

            case SPARK_REGEX(sparkUrl) =>
                val scheduler = new TaskSchedulerImpl(sc)
                val masterUrls = sparkUrl.split(",").map("org.apache.spark://" + _)
                val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
                scheduler.initialize(backend)
                (backend, scheduler)

            case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
                // 检查以确保请求的内存<=memoryPerSlave。否则Spark将会终止。
                // Check to make sure memory requested <= memoryPerSlave. Otherwise Spark will just hang.
                val memoryPerSlaveInt = memoryPerSlave.toInt
                if (sc.executorMemory > memoryPerSlaveInt) {
                    throw new SparkException(
                        "Asked to launch cluster with %d MB RAM / worker but requested %d MB/worker".format(
                            memoryPerSlaveInt, sc.executorMemory))
                }

                val scheduler = new TaskSchedulerImpl(sc)
                val localCluster = new LocalSparkCluster(
                    numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt, sc.conf)
                val masterUrls = localCluster.start()
                val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
                scheduler.initialize(backend)
                backend.shutdownCallback = (backend: StandaloneSchedulerBackend) => {
                    localCluster.stop()
                }
                (backend, scheduler)

            case masterUrl =>
                val cm = getClusterManager(masterUrl) match {
                    case Some(clusterMgr) => clusterMgr
                    case None => throw new SparkException("Could not parse Master URL: '" + master + "'")
                }
                try {
                    val scheduler = cm.createTaskScheduler(sc, masterUrl)
                    val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
                    cm.initialize(scheduler, backend)
                    (backend, scheduler)
                } catch {
                    case se: SparkException => throw se
                    case NonFatal(e) =>
                        throw new SparkException("External scheduler cannot be instantiated", e)
                }
        }
    }

    private def getClusterManager(url: String): Option[ExternalClusterManager] = {
        val loader = Utils.getContextOrSparkClassLoader
        val serviceLoaders =
            ServiceLoader.load(classOf[ExternalClusterManager], loader).asScala.filter(_.canCreate(url))
        if (serviceLoaders.size > 1) {
            throw new SparkException(
                s"Multiple external cluster managers registered for the url $url: $serviceLoaders")
        }
        serviceLoaders.headOption
    }
}

/**
  * 从主字符串中提取信息的正则表达式集合。
  *
  * A collection of regexes for extracting information from the master string.
  */
private object SparkMasterRegex {
    // Regular expression used for local[N] and local[*] master formats
    val LOCAL_N_REGEX = """local\[([0-9]+|\*)\]""".r
    // Regular expression for local[N, maxRetries], used in tests with failing tasks
    val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r
    // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
    val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
    // Regular expression for connecting to Spark deploy clusters
    val SPARK_REGEX = """org.apache.spark://(.*)""".r
}

/**
  * 一个类，它封装了如何将某个类型“T”从“writeable”转换成“T”。
  * 它存储与“T”对应的“Writable”类（例如，“Int”的“IntWritable”）和一个用于进行转换的函数。
  *
  * A class encapsulating how to convert some type `T` from `Writable`. It stores both the `Writable`
  * class corresponding to `T` (e.g. `IntWritable` for `Int`) and a function for doing the
  * conversion.
  * The getter for the writable class takes a `ClassTag[T]` in case this is a generic object
  * that doesn't know the type of `T` when it is created. This sounds strange but is necessary to
  * support converting subclasses of `Writable` to themselves (`writableWritableConverter()`).
  */
private[spark] class WritableConverter[T](
                                             val writableClass: ClassTag[T] => Class[_ <: Writable],
                                             val convert: Writable => T)
    extends Serializable

object WritableConverter {

    // Helper objects for converting common types to Writable
    private[spark] def simpleWritableConverter[T, W <: Writable : ClassTag](convert: W => T)
    : WritableConverter[T] = {
        val wClass = classTag[W].runtimeClass.asInstanceOf[Class[W]]
        new WritableConverter[T](_ => wClass, x => convert(x.asInstanceOf[W]))
    }

    // The following implicit functions were in SparkContext before 1.3 and users had to
    // `import SparkContext._` to enable them. Now we move them here to make the compiler find
    // them automatically. However, we still keep the old functions in SparkContext for backward
    // compatibility and forward to the following functions directly.

    implicit def intWritableConverter(): WritableConverter[Int] =
        simpleWritableConverter[Int, IntWritable](_.get)

    implicit def longWritableConverter(): WritableConverter[Long] =
        simpleWritableConverter[Long, LongWritable](_.get)

    implicit def doubleWritableConverter(): WritableConverter[Double] =
        simpleWritableConverter[Double, DoubleWritable](_.get)

    implicit def floatWritableConverter(): WritableConverter[Float] =
        simpleWritableConverter[Float, FloatWritable](_.get)

    implicit def booleanWritableConverter(): WritableConverter[Boolean] =
        simpleWritableConverter[Boolean, BooleanWritable](_.get)

    implicit def bytesWritableConverter(): WritableConverter[Array[Byte]] = {
        simpleWritableConverter[Array[Byte], BytesWritable] { bw =>
            // getBytes method returns array which is longer then data to be returned
            Arrays.copyOfRange(bw.getBytes, 0, bw.getLength)
        }
    }

    implicit def stringWritableConverter(): WritableConverter[String] =
        simpleWritableConverter[String, Text](_.toString)

    implicit def writableWritableConverter[T <: Writable](): WritableConverter[T] =
        new WritableConverter[T](_.runtimeClass.asInstanceOf[Class[T]], _.asInstanceOf[T])
}

/**
  * 封装如何将某个类型“T”转换为“Writable”的类。
  * 它存储与“T”对应的“Writable”类（例如，“Int”的“IntWritable”）和一个用于进行转换的函数。
  *
  * A class encapsulating how to convert some type `T` to `Writable`. It stores both the `Writable`
  * class corresponding to `T` (e.g. `IntWritable` for `Int`) and a function for doing the
  * conversion.
  * The `Writable` class will be used in `SequenceFileRDDFunctions`.
  */
private[spark] class WritableFactory[T](
                                           val writableClass: ClassTag[T] => Class[_ <: Writable],
                                           val convert: T => Writable) extends Serializable

object WritableFactory {

    private[spark] def simpleWritableFactory[T: ClassTag, W <: Writable : ClassTag](convert: T => W)
    : WritableFactory[T] = {
        val writableClass = implicitly[ClassTag[W]].runtimeClass.asInstanceOf[Class[W]]
        new WritableFactory[T](_ => writableClass, convert)
    }

    implicit def intWritableFactory: WritableFactory[Int] =
        simpleWritableFactory(new IntWritable(_))

    implicit def longWritableFactory: WritableFactory[Long] =
        simpleWritableFactory(new LongWritable(_))

    implicit def floatWritableFactory: WritableFactory[Float] =
        simpleWritableFactory(new FloatWritable(_))

    implicit def doubleWritableFactory: WritableFactory[Double] =
        simpleWritableFactory(new DoubleWritable(_))

    implicit def booleanWritableFactory: WritableFactory[Boolean] =
        simpleWritableFactory(new BooleanWritable(_))

    implicit def bytesWritableFactory: WritableFactory[Array[Byte]] =
        simpleWritableFactory(new BytesWritable(_))

    implicit def stringWritableFactory: WritableFactory[String] =
        simpleWritableFactory(new Text(_))

    implicit def writableWritableFactory[T <: Writable : ClassTag]: WritableFactory[T] =
        simpleWritableFactory(w => w)

}
