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

package org.apache.spark;

import java.io.Serializable;

/**
 * 暴露有关Spark执行器的信息。
 * 此接口的设计目的不是在Spark之外实现。我们可能会添加其他方法，这些方法可能会破坏与外部实现的二进制兼容性。
 * <p>
 * Exposes information about Spark Executors.
 * <p>
 * This interface is not designed to be implemented outside of Spark.  We may add additional methods
 * which may break binary compatibility with outside implementations.
 */
public interface SparkExecutorInfo extends Serializable {
    String host();

    int port();

    long cacheSize();

    int numRunningTasks();
}
