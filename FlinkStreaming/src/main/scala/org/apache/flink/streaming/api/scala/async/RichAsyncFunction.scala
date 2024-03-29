/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.scala.async

import org.apache.flink.api.common.functions.AbstractRichFunction

/**
  * AsyncFunction的丰富变体。作为org.apache.flink.api.common.functions.RichFunction
  * 它提供对org.apache.flink.api.common.functions.RuntimeContext的访问，并提供安装和拆卸方法。
  *
  * Rich variant of [[AsyncFunction]]. As a [[org.apache.flink.api.common.functions.RichFunction]],
  * it gives access to the [[org.apache.flink.api.common.functions.RuntimeContext]] and provides
  * setup and teardown methods.
  *
  * State related apis in [[org.apache.flink.api.common.functions.RuntimeContext]] are not supported
  * yet because the key may get changed while accessing states in the working thread.
  *
  * [[org.apache.flink.api.common.functions.IterationRuntimeContext#getIterationAggregator(String)]]
  * is not supported since the aggregator may be modified by multiple threads.
  *
  * @tparam IN  The type of the input value.
  * @tparam OUT The type of the output value.
  */
abstract class RichAsyncFunction[IN, OUT]
    extends AbstractRichFunction
        with AsyncFunction[IN, OUT] {}
