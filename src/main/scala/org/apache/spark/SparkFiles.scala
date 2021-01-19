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

import java.io.File

/**
  * 解析通过`SparkContext.addFile文件()`添加的文件的路径。
  *
  * Resolves paths to files added through `SparkContext.addFile()`.
  */
object SparkFiles {

    /**
      * 获取文件的绝对路径
      * Get the absolute path of a file added through `SparkContext.addFile()`.
      */
    def get(filename: String): String =
        new File(getRootDirectory(), filename).getAbsolutePath()

    /**
      * 获取根路径
      * Get the root directory that contains files added through `SparkContext.addFile()`.
      */
    def getRootDirectory(): String =
        SparkEnv.get.driverTmpDir.getOrElse(".")

}
