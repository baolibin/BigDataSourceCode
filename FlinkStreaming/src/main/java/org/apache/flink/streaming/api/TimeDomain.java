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

package org.apache.flink.streaming.api;

/**
 * {@code TimeDomain} specifies whether a firing timer is based on event time or processing time.
 */
public enum TimeDomain {

	/**
	 * 时间基于事件的时间戳。
	 * Time is based on the timestamp of events.
	 */
	EVENT_TIME,

	/**
	 * 时间基于进行加工的机器的当前加工时间。
	 * Time is based on the current processing-time of a machine where processing happens.
	 */
	PROCESSING_TIME
}
