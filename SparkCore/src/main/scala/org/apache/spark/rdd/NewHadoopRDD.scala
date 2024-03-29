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

package org.apache.spark.rdd

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, FileSplit}
import org.apache.hadoop.mapreduce.task.{JobContextImpl, TaskAttemptContextImpl}
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.IGNORE_CORRUPT_FILES
import org.apache.spark.rdd.NewHadoopRDD.NewHadoopMapPartitionsWithSplitRDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{SerializableConfiguration, ShutdownHookManager}

import scala.reflect.ClassTag

private[spark] class NewHadoopPartition(
                                           rddId: Int,
                                           val index: Int,
                                           rawSplit: InputSplit with Writable)
    extends Partition {

    val serializableHadoopSplit = new SerializableWritable(rawSplit)

    override def hashCode(): Int = 31 * (31 + rddId) + index

    override def equals(other: Any): Boolean = super.equals(other)
}

/**
  * 一种RDD，使用新的MapReduceAPI提供读取存储在Hadoop中的数据。
  * （例如，HDFS中的文件、HBase或S3中的源）
  *
  * :: DeveloperApi ::
  * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
  * sources in HBase, or S3), using the new MapReduce API (`org.apache.hadoop.mapreduce`).
  *
  * @param sc               The SparkContext to associate the RDD with.
  * @param inputFormatClass Storage format of the data to be read.
  * @param keyClass         Class of the key associated with the inputFormatClass.
  * @param valueClass       Class of the value associated with the inputFormatClass.
  * @note Instantiating this class directly is not recommended, please use
  *       `org.apache.org.apache.spark.SparkContext.newAPIHadoopRDD()`
  */
@DeveloperApi
class NewHadoopRDD[K, V](
                            sc: SparkContext,
                            inputFormatClass: Class[_ <: InputFormat[K, V]],
                            keyClass: Class[K],
                            valueClass: Class[V],
                            @transient private val _conf: Configuration)
    extends RDD[(K, V)](sc, Nil) with Logging {

    @transient protected val jobId = new JobID(jobTrackerId, id)
    // private val serializableConf = new SerializableWritable(_conf)
    // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
    private val confBroadcast = sc.broadcast(new SerializableConfiguration(_conf))
    private val jobTrackerId: String = {
        val formatter = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
        formatter.format(new Date())
    }
    private val shouldCloneJobConf = sparkContext.conf.getBoolean("org.apache.spark.hadoop.cloneConf", false)

    private val ignoreCorruptFiles = sparkContext.conf.get(IGNORE_CORRUPT_FILES)

    override def getPartitions: Array[Partition] = {
        val inputFormat = inputFormatClass.newInstance
        inputFormat match {
            case configurable: Configurable =>
                configurable.setConf(_conf)
            case _ =>
        }
        val jobContext = new JobContextImpl(_conf, jobId)
        val rawSplits = inputFormat.getSplits(jobContext).toArray
        val result = new Array[Partition](rawSplits.size)
        for (i <- 0 until rawSplits.size) {
            result(i) = new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
        }
        result
    }

    override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
        val iter = new Iterator[(K, V)] {
            private val split = theSplit.asInstanceOf[NewHadoopPartition]
            logInfo("Input split: " + split.serializableHadoopSplit)
            private val conf = getConf

            private val inputMetrics = context.taskMetrics().inputMetrics
            private val existingBytesRead = inputMetrics.bytesRead

            // Sets InputFileBlockHolder for the file block's information
            split.serializableHadoopSplit.value match {
                case fs: FileSplit =>
                    InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
                case _ =>
                    InputFileBlockHolder.unset()
            }

            // Find a function that will return the FileSystem bytes read by this thread. Do this before
            // creating RecordReader, because RecordReader's constructor might read some bytes
            private val getBytesReadCallback: Option[() => Long] =
            split.serializableHadoopSplit.value match {
                case _: FileSplit | _: CombineFileSplit =>
                    Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
                case _ => None
            }

            // We get our input bytes from thread-local Hadoop FileSystem statistics.
            // If we do a coalesce, however, we are likely to compute multiple partitions in the same
            // task and in the same thread, in which case we need to avoid override values written by
            // previous partitions (SPARK-13071).
            private def updateBytesRead(): Unit = {
                getBytesReadCallback.foreach { getBytesRead =>
                    inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
                }
            }

            private val format = inputFormatClass.newInstance
            format match {
                case configurable: Configurable =>
                    configurable.setConf(conf)
                case _ =>
            }
            private val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
            private val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
            private var finished = false
            private var reader =
                try {
                    val _reader = format.createRecordReader(
                        split.serializableHadoopSplit.value, hadoopAttemptContext)
                    _reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)
                    _reader
                } catch {
                    case e: IOException if ignoreCorruptFiles =>
                        logWarning(
                            s"Skipped the rest content in the corrupted file: ${split.serializableHadoopSplit}",
                            e)
                        finished = true
                        null
                }

            // Register an on-task-completion callback to close the input stream.
            context.addTaskCompletionListener { context =>
                // Update the bytesRead before closing is to make sure lingering bytesRead statistics in
                // this thread get correctly added.
                updateBytesRead()
                close()
            }

            private var havePair = false
            private var recordsSinceMetricsUpdate = 0

            override def hasNext: Boolean = {
                if (!finished && !havePair) {
                    try {
                        finished = !reader.nextKeyValue
                    } catch {
                        case e: IOException if ignoreCorruptFiles =>
                            logWarning(
                                s"Skipped the rest content in the corrupted file: ${split.serializableHadoopSplit}",
                                e)
                            finished = true
                    }
                    if (finished) {
                        // Close and release the reader here; close() will also be called when the task
                        // completes, but for tasks that read from many files, it helps to release the
                        // resources early.
                        close()
                    }
                    havePair = !finished
                }
                !finished
            }

            override def next(): (K, V) = {
                if (!hasNext) {
                    throw new java.util.NoSuchElementException("End of stream")
                }
                havePair = false
                if (!finished) {
                    inputMetrics.incRecordsRead(1)
                }
                if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
                    updateBytesRead()
                }
                (reader.getCurrentKey, reader.getCurrentValue)
            }

            private def close(): Unit = {
                if (reader != null) {
                    InputFileBlockHolder.unset()
                    try {
                        reader.close()
                    } catch {
                        case e: Exception =>
                            if (!ShutdownHookManager.inShutdown()) {
                                logWarning("Exception in RecordReader.close()", e)
                            }
                    } finally {
                        reader = null
                    }
                    if (getBytesReadCallback.isDefined) {
                        updateBytesRead()
                    } else if (split.serializableHadoopSplit.value.isInstanceOf[FileSplit] ||
                        split.serializableHadoopSplit.value.isInstanceOf[CombineFileSplit]) {
                        // If we can't get the bytes read from the FS stats, fall back to the split size,
                        // which may be inaccurate.
                        try {
                            inputMetrics.incBytesRead(split.serializableHadoopSplit.value.getLength)
                        } catch {
                            case e: java.io.IOException =>
                                logWarning("Unable to get input size to set InputMetrics for task", e)
                        }
                    }
                }
            }
        }
        new InterruptibleIterator(context, iter)
    }

    def getConf: Configuration = {
        val conf: Configuration = confBroadcast.value.value
        if (shouldCloneJobConf) {
            // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
            // one job modifies a configuration while another reads it (SPARK-2546, SPARK-10611).  This
            // problem occurs somewhat rarely because most jobs treat the configuration as though it's
            // immutable.  One solution, implemented here, is to clone the Configuration object.
            // Unfortunately, this clone can be very expensive.  To avoid unexpected performance
            // regressions for workloads and Hadoop versions that do not suffer from these thread-safety
            // issues, this cloning is disabled by default.
            NewHadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
                logDebug("Cloning Hadoop Configuration")
                // The Configuration passed in is actually a JobConf and possibly contains credentials.
                // To keep those credentials properly we have to create a new JobConf not a Configuration.
                if (conf.isInstanceOf[JobConf]) {
                    new JobConf(conf)
                } else {
                    new Configuration(conf)
                }
            }
        } else {
            conf
        }
    }

    /**
      * 映射到分区，提供用作分区基础的InputSplit
      *
      * Maps over a partition, providing the InputSplit that was used as the base of the partition.
      */
    @DeveloperApi
    def mapPartitionsWithInputSplit[U: ClassTag](
                                                    f: (InputSplit, Iterator[(K, V)]) => Iterator[U],
                                                    preservesPartitioning: Boolean = false): RDD[U] = {
        new NewHadoopMapPartitionsWithSplitRDD(this, f, preservesPartitioning)
    }

    override def getPreferredLocations(hsplit: Partition): Seq[String] = {
        val split = hsplit.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value
        val locs = HadoopRDD.convertSplitLocationInfo(split.getLocationInfo)
        locs.getOrElse(split.getLocations.filter(_ != "localhost"))
    }

    override def persist(storageLevel: StorageLevel): this.type = {
        if (storageLevel.deserialized) {
            logWarning("Caching NewHadoopRDDs as deserialized objects usually leads to undesired" +
                " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
                " Use a map transformation to make copies of the records.")
        }
        super.persist(storageLevel)
    }

}

private[spark] object NewHadoopRDD {
    /**
      * 配置的构造函数不是线程安全的（请参阅SPARK-1097和HADOOP-10456）。因此，我们在调用newConfiguration（）之前同步这个锁。
      *
      * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
      * Therefore, we synchronize on this lock before calling new Configuration().
      */
    val CONFIGURATION_INSTANTIATION_LOCK = new Object()

    /**
      * 类似于[[org.apache.spark.rdd.MapPartitionsRDD]]，但将InputSplit传递给给定函数，而不是分区的索引。
      *
      * Analogous to [[org.apache.spark.rdd.MapPartitionsRDD]], but passes in an InputSplit to
      * the given function rather than the index of the partition.
      */
    private[spark] class NewHadoopMapPartitionsWithSplitRDD[U: ClassTag, T: ClassTag](
                                                                                         prev: RDD[T],
                                                                                         f: (InputSplit, Iterator[T]) => Iterator[U],
                                                                                         preservesPartitioning: Boolean = false)
        extends RDD[U](prev) {

        override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

        override def getPartitions: Array[Partition] = firstParent[T].partitions

        override def compute(split: Partition, context: TaskContext): Iterator[U] = {
            val partition = split.asInstanceOf[NewHadoopPartition]
            val inputSplit = partition.serializableHadoopSplit.value
            f(inputSplit, firstParent[T].iterator(split, context))
        }
    }

}
