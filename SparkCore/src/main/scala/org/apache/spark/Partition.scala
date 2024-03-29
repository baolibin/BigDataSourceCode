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

/**
  * RDD中分区的标识符。
  *
  * An identifier for a partition in an RDD.
  */
trait Partition extends Serializable {
    /**
      * 在其父RDD中获取分区的索引
      *
      * Get the partition's index within its parent RDD
      */
    def index: Int

    // A better default implementation of HashCode
    // 使用index作为默认的哈希值
    override def hashCode(): Int = index

    // 使用Java Object默认的equals方法
    override def equals(other: Any): Boolean = super.equals(other)
}
