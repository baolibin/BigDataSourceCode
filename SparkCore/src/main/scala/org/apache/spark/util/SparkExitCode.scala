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

package org.apache.spark.util

private[spark] object SparkExitCode {
    /**
      * 已达到默认的未捕获异常处理程序。
      *
      * The default uncaught exception handler was reached.
      */
    val UNCAUGHT_EXCEPTION = 50

    /**
      * 调用了默认的未捕获异常处理程序，在记录异常时遇到异常。
      *
      * The default uncaught exception handler was called and an exception was encountered while
      * logging the exception.
      */
    val UNCAUGHT_EXCEPTION_TWICE = 51

    /**
      * 已达到默认的未捕获异常处理程序，未捕获异常是OutOfMemoryError
      *
      * The default uncaught exception handler was reached, and the uncaught exception was an
      * OutOfMemoryError.
      */
    val OOM = 52

}
