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

class SparkException(message: String, cause: Throwable)
        extends Exception(message, cause) {

    def this(message: String) = this(message, null)
}

/**
  * 当驱动程序进程中的某些用户代码执行失败时引发异常，
  * 例如累加器更新失败或takeOrdered中的失败（用户提供的排序实现可能会出现错误）。
  * Exception thrown when execution of some user code in the driver process fails, e.g.
  * accumulator update fails or failure in takeOrdered (user supplies an Ordering implementation
  * that can be misbehaving.
  */
private[spark] class SparkDriverExecutionException(cause: Throwable)
        extends SparkException("Execution error", cause)

/**
  * 当主用户代码作为子进程（例如pyspark）运行，并且我们希望父SparkSubmit进程使用相同的退出代码退出时引发异常。
  *
  * Exception thrown when the main user code is run as a child process (e.g. pyspark) and we want
  * the parent SparkSubmit process to exit with the same exit code.
  */
private[spark] case class SparkUserAppException(exitCode: Int)
        extends SparkException(s"User application exited with $exitCode")
