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

/**
  * 用于提取错误根本原因的提取器对象。
  *
  * Extractor Object for pulling out the root cause of an error.
  * If the error contains no cause, it will return the error itself.
  *
  * Usage:
  * try {
  * ...
  * } catch {
  * case CausedBy(ex: CommitDeniedException) => ...
  * }
  */
private[spark] object CausedBy {

    def unapply(e: Throwable): Option[Throwable] = {
        Option(e.getCause).flatMap(cause => unapply(cause)).orElse(Some(e))
    }
}
