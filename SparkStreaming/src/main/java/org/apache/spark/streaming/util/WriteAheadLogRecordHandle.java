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

package org.apache.spark.streaming.util;

/**
 * 这个抽象类表示一个句柄，该句柄引用在{@link WriteAheadLog WriteAheadLog}中写入的记录。
 * 它必须包含WriteAheadLog类实现读取和返回记录所需的所有信息。
 * <p>
 * :: DeveloperApi ::
 * <p>
 * This abstract class represents a handle that refers to a record written in a
 * {@link WriteAheadLog WriteAheadLog}.
 * It must contain all the information necessary for the record to be read and returned by
 * an implementation of the WriteAheadLog class.
 * @see WriteAheadLog
 */
@org.apache.spark.annotation.DeveloperApi
public abstract class WriteAheadLogRecordHandle implements java.io.Serializable {
}
