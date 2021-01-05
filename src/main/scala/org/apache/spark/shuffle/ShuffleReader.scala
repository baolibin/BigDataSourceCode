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

package org.apache.spark.shuffle

/**
  * reduce task读数据从shuffle system,拉取数据会跨节点传输数据,发生大量IO操作,常见容易发生数据倾斜问题.
  * Obtained inside a reduce task to read combined records from the mappers.
  */
private[spark] trait ShuffleReader[K, C] {
    /** Read the combined key-values for this reduce task */
    /**
      * Reduce Task拉取数据,通过Http协议.
      * 拉取的数据量级大会溢写磁盘,会发生归并排序.
      */
    def read(): Iterator[Product2[K, C]]

    /**
      * Close this reader.
      * TODO: Add this back when we make the ShuffleReader a developer API that others can implement
      * (at which point this will likely be necessary).
      */
    // def stop(): Unit
}
