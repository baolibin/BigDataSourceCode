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

package org.apache.flink.api.java.functions;

import org.apache.flink.annotation.Internal;

/**
 * 用作虚拟的{@linkkeyselector}，允许对非键控用例使用键控操作符。
 * 本质上，它为所有传入的记录提供相同的键，即{@code（byte）0}值。
 * <p>
 * Used as a dummy {@link KeySelector} to allow using keyed operators
 * for non-keyed use cases. Essentially, it gives all incoming records
 * the same key, which is a {@code (byte) 0} value.
 * @param <T> The type of the input element.
 */
@Internal
public class NullByteKeySelector<T> implements KeySelector<T, Byte> {

	private static final long serialVersionUID = 614256539098549020L;

	@Override
	public Byte getKey(T value) throws Exception {
		return 0;
	}
}
