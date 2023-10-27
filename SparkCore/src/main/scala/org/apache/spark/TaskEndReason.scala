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

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, Utils}

// ==============================================================================================
// NOTE: new task end reasons MUST be accompanied with serialization logic in util.JsonProtocol!
// ==============================================================================================

/**
  * 任务结束的各种可能原因。low-level的TaskScheduler应该为“短暂”的失败重试几次任务，并且只报告需要重新提交一些旧阶段的失败，
  * 例如shuffle map fetch失败。
  *
  * :: DeveloperApi ::
  * Various possible reasons why a task ended. The low-level TaskScheduler is supposed to retry
  * tasks several times for "ephemeral" failures, and only report back failures that require some
  * old stages to be resubmitted, such as shuffle map fetch failures.
  */
@DeveloperApi
sealed trait TaskEndReason

/**
  * 任务成功了。
  *
  * :: DeveloperApi ::
  * Task succeeded.
  */
@DeveloperApi
case object Success extends TaskEndReason

/**
  * 任务失败的各种可能原因。
  *
  * :: DeveloperApi ::
  * Various possible reasons why a task failed.
  */
@DeveloperApi
sealed trait TaskFailedReason extends TaskEndReason {
    /**
      * web UI中显示的错误消息。
      *
      * Error message displayed in the web UI.
      */
    def toErrorString: String

    /**
      * 此任务失败是否应计入在阶段中止前允许任务失败的最大次数。
      * 如果任务失败与任务无关，则设置为false；例如，如果任务失败是因为它所运行的执行器被杀死。
      *
      * Whether this task failure should be counted towards the maximum number of times the task is
      * allowed to fail before the stage is aborted.  Set to false in cases where the task's failure
      * was unrelated to the task; for example, if the task failed because the executor it was running
      * on was killed.
      */
    def countTowardsTaskFailures: Boolean = true
}

/**
  * “org.apache.org.apache.spark.scheduler.ShuffleMapTask”早些时候成功完成，但在阶段完成之前我们失去了执行器。这意味着Spark需要重新安排任务，以便在不同的执行器上重新执行。
  *
  * :: DeveloperApi ::
  * A `org.apache.org.apache.spark.scheduler.ShuffleMapTask` that completed successfully earlier, but we
  * lost the executor before the stage completed. This means Spark needs to reschedule the task
  * to be re-executed on a different executor.
  */
@DeveloperApi
case object Resubmitted extends TaskFailedReason {
    override def toErrorString: String = "Resubmitted (resubmitted due to lost executor)"
}

/**
  * 任务无法从远程节点获取混洗数据。可能意味着我们已经丢失了任务试图从中获取的远程执行器，因此需要重新运行前一阶段。
  *
  * :: DeveloperApi ::
  * Task failed to fetch shuffle data from a remote node. Probably means we have lost the remote
  * executors the task is trying to fetch from, and thus need to rerun the previous stage.
  */
@DeveloperApi
case class FetchFailed(
                          bmAddress: BlockManagerId, // Note that bmAddress can be null
                          shuffleId: Int,
                          mapId: Int,
                          reduceId: Int,
                          message: String)
    extends TaskFailedReason {
    override def toErrorString: String = {
        val bmAddressString = if (bmAddress == null) "null" else bmAddress.toString
        s"FetchFailed($bmAddressString, shuffleId=$shuffleId, mapId=$mapId, reduceId=$reduceId, " +
            s"message=\n$message\n)"
    }

    /**
      * Fetch failures lead to a different failure handling path: (1) we don't abort the stage after
      * 4 task failures, instead we immediately go back to the stage which generated the map output,
      * and regenerate the missing data.  (2) we don't count fetch failures for blacklisting, since
      * presumably its not the fault of the executor where the task ran, but the executor which
      * stored the data. This is especially important because we might rack up a bunch of
      * fetch-failures in rapid succession, on all nodes of the cluster, due to one bad node.
      */
    override def countTowardsTaskFailures: Boolean = false
}

/**
  * 由于运行时异常，任务失败。这是最常见的故障情况，也会捕获用户程序异常。
  *
  * :: DeveloperApi ::
  * Task failed due to a runtime exception. This is the most common failure case and also captures
  * user program exceptions.
  *
  * `stackTrace` contains the stack trace of the exception itself. It still exists for backward
  * compatibility. It's better to use `this(e: Throwable, metrics: Option[TaskMetrics])` to
  * create `ExceptionFailure` as it will handle the backward compatibility properly.
  *
  * `fullStackTrace` is a better representation of the stack trace because it contains the whole
  * stack trace including the exception and its causes
  *
  * `exception` is the actual exception that caused the task to fail. It may be `None` in
  * the case that the exception is not in fact serializable. If a task fails more than
  * once (due to retries), `exception` is that one that caused the last failure.
  */
@DeveloperApi
case class ExceptionFailure(
                               className: String,
                               description: String,
                               stackTrace: Array[StackTraceElement],
                               fullStackTrace: String,
                               private val exceptionWrapper: Option[ThrowableSerializationWrapper],
                               accumUpdates: Seq[AccumulableInfo] = Seq.empty,
                               private[spark] var accums: Seq[AccumulatorV2[_, _]] = Nil)
    extends TaskFailedReason {

    def exception: Option[Throwable] = exceptionWrapper.flatMap(w => Option(w.exception))

    override def toErrorString: String =
        if (fullStackTrace == null) {
            // fullStackTrace is added in 1.2.0
            // If fullStackTrace is null, use the old error string for backward compatibility
            exceptionString(className, description, stackTrace)
        } else {
            fullStackTrace
        }

    /**
      * 返回异常的漂亮字符串表示，包括堆栈跟踪。注意：它不包括异常的原因，仅用于向后兼容性。
      *
      * Return a nice string representation of the exception, including the stack trace.
      * Note: It does not include the exception's causes, and is only used for backward compatibility.
      */
    private def exceptionString(
                                   className: String,
                                   description: String,
                                   stackTrace: Array[StackTraceElement]): String = {
        val desc = if (description == null) "" else description
        val st = if (stackTrace == null) "" else stackTrace.map("        " + _).mkString("\n")
        s"$className: $desc\n$st"
    }

    /**
      * `preserveCause` is used to keep the exception itself so it is available to the
      * driver. This may be set to `false` in the event that the exception is not in fact
      * serializable.
      */
    private[spark] def this(
                               e: Throwable,
                               accumUpdates: Seq[AccumulableInfo],
                               preserveCause: Boolean) {
        this(e.getClass.getName, e.getMessage, e.getStackTrace, Utils.exceptionString(e),
            if (preserveCause) Some(new ThrowableSerializationWrapper(e)) else None, accumUpdates)
    }

    private[spark] def this(e: Throwable, accumUpdates: Seq[AccumulableInfo]) {
        this(e, accumUpdates, preserveCause = true)
    }

    private[spark] def withAccums(accums: Seq[AccumulatorV2[_, _]]): ExceptionFailure = {
        this.accums = accums
        this
    }
}

/**
  * A class for recovering from exceptions when deserializing a Throwable that was
  * thrown in user task code. If the Throwable cannot be deserialized it will be null,
  * but the stacktrace and message will be preserved correctly in SparkException.
  */
private[spark] class ThrowableSerializationWrapper(var exception: Throwable) extends
    Serializable with Logging {
    private def writeObject(out: ObjectOutputStream): Unit = {
        out.writeObject(exception)
    }

    private def readObject(in: ObjectInputStream): Unit = {
        try {
            exception = in.readObject().asInstanceOf[Throwable]
        } catch {
            case e: Exception => log.warn("Task exception could not be deserialized", e)
        }
    }
}

/**
  * 任务成功完成，但在获取之前，执行者的块管理器丢失了结果。
  * :: DeveloperApi ::
  * The task finished successfully, but the result was lost from the executor's block manager before
  * it was fetched.
  */
@DeveloperApi
case object TaskResultLost extends TaskFailedReason {
    override def toErrorString: String = "TaskResultLost (result lost from block manager)"
}

/**
  * 任务被故意终止，需要重新安排。
  * :: DeveloperApi ::
  * Task was killed intentionally and needs to be rescheduled.
  */
@DeveloperApi
case class TaskKilled(reason: String) extends TaskFailedReason {
    override def toErrorString: String = s"TaskKilled ($reason)"

    override def countTowardsTaskFailures: Boolean = false
}

/**
  * 任务请求驱动程序提交，但被拒绝。
  *
  * :: DeveloperApi ::
  * Task requested the driver to commit, but was denied.
  */
@DeveloperApi
case class TaskCommitDenied(
                               jobID: Int,
                               partitionID: Int,
                               attemptNumber: Int) extends TaskFailedReason {
    override def toErrorString: String = s"TaskCommitDenied (Driver denied task commit)" +
        s" for job: $jobID, partition: $partitionID, attemptNumber: $attemptNumber"

    /**
      * If a task failed because its attempt to commit was denied, do not count this failure
      * towards failing the stage. This is intended to prevent spurious stage failures in cases
      * where many speculative tasks are launched and denied to commit.
      */
    override def countTowardsTaskFailures: Boolean = false
}

/**
  * 任务失败，因为运行它的执行器丢失。这可能是因为任务使JVM崩溃。
  *
  * :: DeveloperApi ::
  * The task failed because the executor that it was running on was lost. This may happen because
  * the task crashed the JVM.
  */
@DeveloperApi
case class ExecutorLostFailure(
                                  execId: String,
                                  exitCausedByApp: Boolean = true,
                                  reason: Option[String]) extends TaskFailedReason {
    override def toErrorString: String = {
        val exitBehavior = if (exitCausedByApp) {
            "caused by one of the running tasks"
        } else {
            "unrelated to the running tasks"
        }
        s"ExecutorLostFailure (executor ${execId} exited ${exitBehavior})" +
            reason.map { r => s" Reason: $r" }.getOrElse("")
    }

    override def countTowardsTaskFailures: Boolean = exitCausedByApp
}

/**
  * 我们不知道任务为什么结束——例如，因为在反序列化任务结果时发生了ClassNotFound异常。
  *
  * :: DeveloperApi ::
  * We don't know why the task ended -- for example, because of a ClassNotFound exception when
  * deserializing the task result.
  */
@DeveloperApi
case object UnknownReason extends TaskFailedReason {
    override def toErrorString: String = "UnknownReason"
}
