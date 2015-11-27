/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.querywindow;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class WindowResultLongLongSchema implements DeserializationSchema<WindowResult<Long, Tuple2<Long, Long>>>, SerializationSchema<WindowResult<Long, Tuple2<Long, Long>>> {

	private static final long serialVersionUID = 1L;

	@Override
	public WindowResult<Long, Tuple2<Long, Long>> deserialize(byte[] message) {
		return null;
	}

	@Override
	public boolean isEndOfStream(WindowResult<Long, Tuple2<Long, Long>> longTuple2WindowResult) {
		return false;
	}

	@Override
	public byte[] serialize(WindowResult<Long, Tuple2<Long, Long>> element) {
		return element.toString().getBytes();
	}

	@Override
	public TypeInformation<WindowResult<Long, Tuple2<Long, Long>>> getProducedType() {
		return new WindowResultType<>(new TupleTypeInfo<Tuple2<Long, Long>>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO), BasicTypeInfo.LONG_TYPE_INFO);
	}
}
