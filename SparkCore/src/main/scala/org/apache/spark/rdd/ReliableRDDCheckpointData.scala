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

import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

/**
  * 将RDD数据写入可靠存储器的检查点实现。这允许驱动程序在先前计算的状态出现故障时重新启动。
  *
  * An implementation of checkpointing that writes the RDD data to reliable storage.
  * This allows drivers to be restarted on failure with previously computed state.
  */
private[spark] class ReliableRDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
    extends RDDCheckpointData[T](rdd) with Logging {

    // The directory to which the associated RDD has been checkpointed to
    // This is assumed to be a non-local path that points to some reliable storage
    private val cpDir: String =
    ReliableRDDCheckpointData.checkpointPath(rdd.context, rdd.id)
        .map(_.toString)
        .getOrElse {
            throw new SparkException("Checkpoint dir must be specified.")
        }

    /**
      * 返回此RDD被检查点指向的目录。如果RDD尚未检查点，则返回None。
      *
      * Return the directory to which this RDD was checkpointed.
      * If the RDD is not checkpointed yet, return None.
      */
    def getCheckpointDir: Option[String] = RDDCheckpointData.synchronized {
        if (isCheckpointed) {
            Some(cpDir.toString)
        } else {
            None
        }
    }

    /**
      * 将此RDD具体化，并将其内容写入可靠的DFS。这将在该RDD上调用的第一个操作完成后立即调用。
      *
      * Materialize this RDD and write its content to a reliable DFS.
      * This is called immediately after the first action invoked on this RDD has completed.
      */
    protected override def doCheckpoint(): CheckpointRDD[T] = {
        val newRDD = ReliableCheckpointRDD.writeRDDToCheckpointDirectory(rdd, cpDir)

        // Optionally clean our checkpoint files if the reference is out of scope
        if (rdd.conf.getBoolean("org.apache.spark.cleaner.referenceTracking.cleanCheckpoints", false)) {
            rdd.context.cleaner.foreach { cleaner =>
                cleaner.registerRDDCheckpointDataForCleanup(newRDD, rdd.id)
            }
        }

        logInfo(s"Done checkpointing RDD ${rdd.id} to $cpDir, new parent is RDD ${newRDD.id}")
        newRDD
    }

}

private[spark] object ReliableRDDCheckpointData extends Logging {

    /**
      * 清理与此RDD的检查点数据关联的文件
      * Clean up the files associated with the checkpoint data for this RDD.
      */
    def cleanCheckpoint(sc: SparkContext, rddId: Int): Unit = {
        checkpointPath(sc, rddId).foreach { path =>
            path.getFileSystem(sc.hadoopConfiguration).delete(path, true)
        }
    }

    /**
      * 返回写入此RDD检查点数据的目录的路径。
      * Return the path of the directory to which this RDD's checkpoint data is written.
      */
    def checkpointPath(sc: SparkContext, rddId: Int): Option[Path] = {
        sc.checkpointDir.map { dir => new Path(dir, s"rdd-$rddId") }
    }
}
