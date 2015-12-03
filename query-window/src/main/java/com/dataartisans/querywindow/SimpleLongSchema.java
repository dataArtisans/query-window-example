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
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class SimpleLongSchema implements DeserializationSchema<Long>, SerializationSchema<Long> {

	private static final long serialVersionUID = 1L;

	@Override
	public Long deserialize(byte[] message) {
		try {
			return Long.parseLong(new String(message));
		} catch (Exception e) {
			return -1L;
		}
	}

	@Override
	public boolean isEndOfStream(Long nextElement) {
		return false;
	}

	@Override
	public byte[] serialize(Long element) {
		return element.toString().getBytes();
	}

	@Override
	public TypeInformation<Long> getProducedType() {
		return BasicTypeInfo.LONG_TYPE_INFO;
	}
}
