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

package org.apache.spark.streaming;

import org.apache.spark.annotation.DeveloperApi;

/**
 * 表示StreamingContext的状态。
 * <p>
 * :: DeveloperApi ::
 * <p>
 * Represents the state of a StreamingContext.
 */
@DeveloperApi
public enum StreamingContextState {
    /**
     * 上下文已创建，但尚未启动。可以在上下文中创建输入数据流、转换和输出操作。
     * <p>
     * The context has been created, but not been started yet.
     * Input DStreams, transformations and output operations can be created on the context.
     */
    INITIALIZED,

    /**
     * 上下文已启动，但尚未停止。无法在上下文中创建输入数据流、转换和输出操作。
     * <p>
     * The context has been started, and been not stopped.
     * Input DStreams, transformations and output operations cannot be created on the context.
     */
    ACTIVE,

    /**
     * 上下文已停止，无法再使用。
     * <p>
     * The context has been stopped and cannot be used any more.
     */
    STOPPED
}
