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

import org.apache.spark.Partition

import scala.reflect.ClassTag

/**
  * 枚举以通过检查点管理RDD的状态转换
  *
  * Enumeration to manage state transitions of an RDD through checkpointing
  *
  * [ Initialized --{@literal >} checkpointing in progress --{@literal >} checkpointed ]
  */
private[spark] object CheckpointState extends Enumeration {
    type CheckpointState = Value
    val Initialized, CheckpointingInProgress, Checkpointed = Value
}

/**
  * 此类包含与RDD检查点相关的所有信息。这个类的每个实例都与一个RDD相关联。
  * 它管理关联RDD的检查点过程，并通过提供检查点RDD的更新分区、迭代器和首选位置来管理检查点后状态。
  *
  * This class contains all the information related to RDD checkpointing. Each instance of this
  * class is associated with an RDD. It manages process of checkpointing of the associated RDD,
  * as well as, manages the post-checkpoint state by providing the updated partitions,
  * iterator and preferred locations of the checkpointed RDD.
  */
private[spark] abstract class RDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
        extends Serializable {

    import CheckpointState._

    // The checkpoint state of the associated RDD.
    protected var cpState = Initialized

    // The RDD that contains our checkpointed data
    private var cpRDD: Option[CheckpointRDD[T]] = None

    // TODO: are we sure we need to use a global lock in the following methods?

    /**
      * 返回此RDD的检查点数据是否已持久化。
      *
      * Return whether the checkpoint data for this RDD is already persisted.
      */
    def isCheckpointed: Boolean = RDDCheckpointData.synchronized {
        cpState == Checkpointed
    }

    /**
      * 将此RDD具体化并持久化其内容。这是在对该RDD调用的第一个操作完成后立即调用的.
      * Materialize this RDD and persist its content.
      * This is called immediately after the first action invoked on this RDD has completed.
      */
    final def checkpoint(): Unit = {
        // Guard against multiple threads checkpointing the same RDD by
        // atomically flipping the state of this RDDCheckpointData
        RDDCheckpointData.synchronized {
            if (cpState == Initialized) {
                cpState = CheckpointingInProgress
            } else {
                return
            }
        }

        val newRDD = doCheckpoint()

        // Update our state and truncate the RDD lineage
        RDDCheckpointData.synchronized {
            cpRDD = Some(newRDD)
            cpState = Checkpointed
            rdd.markCheckpointed()
        }
    }

    /**
      * 返回包含检查点数据的RDD。
      * 只有当检查点状态为“Checkpointed”时，才会定义此项。

      * Return the RDD that contains our checkpointed data.
      * This is only defined if the checkpoint state is `Checkpointed`.
      */
    def checkpointRDD: Option[CheckpointRDD[T]] = RDDCheckpointData.synchronized {
        cpRDD
    }

    /**
      * 返回生成的检查点RDD的分区。
      * 仅用于测试。

      * Return the partitions of the resulting checkpoint RDD.
      * For tests only.
      */
    def getPartitions: Array[Partition] = RDDCheckpointData.synchronized {
        cpRDD.map(_.partitions).getOrElse {
            Array.empty
        }
    }

    /**
      * 将此RDD具体化并持久化其内容。
      * Materialize this RDD and persist its content.
      *
      * 子类应该覆盖此方法来定义自定义检查点行为。
      * Subclasses should override this method to define custom checkpointing behavior.
      *
      * @return the checkpoint RDD created in the process.
      */
    protected def doCheckpoint(): CheckpointRDD[T]

}

/**
  * Global lock for synchronizing checkpoint operations.
  */
private[spark] object RDDCheckpointData
