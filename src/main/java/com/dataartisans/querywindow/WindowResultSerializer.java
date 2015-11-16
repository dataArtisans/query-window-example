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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * {@link TypeSerializer} for {@code WindowResult}.
 *
 * @param <T> The window result type.
 * @param <QT> The type of the query element.
 */
public final class WindowResultSerializer<T, QT> extends TypeSerializer<WindowResult<QT, T>> {
	private static final long serialVersionUID = 1L;

	private static final byte IS_REGULAR_RESULT = 0;
	private static final byte IS_QUERY_RESULT = 1;
	private static final byte IS_EMPTY_QUERY_RESULT = 2;

	private final TypeSerializer<T> resultSerializer;
	private final TypeSerializer<QT> querySerializer;

	@SuppressWarnings("unchecked,rawtypes")
	public WindowResultSerializer(TypeSerializer<T> resultSerializer,
			TypeSerializer<QT> querySerializer) {
		this.resultSerializer = resultSerializer;
		this.querySerializer =  querySerializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<WindowResult<QT, T>> duplicate() {
		return new WindowResultSerializer<>(resultSerializer.duplicate(), querySerializer.duplicate());
	}

	@Override
	public WindowResult<QT, T> createInstance() {
		return WindowResult.queryResult(null, null);
	}

	@Override
	public WindowResult<QT, T> copy(WindowResult<QT, T> from) {
		if (from.isRegularResult()) {
			return WindowResult.regularResult(resultSerializer.copy(from.result));
		} else if (!from.isEmptyQueryResult()){
			return WindowResult.queryResult(querySerializer.copy(from.query), resultSerializer.copy(from.result));
		} else {
			// empty query result
			return WindowResult.queryResult(querySerializer.copy(from.query), null);
		}
	}

	@Override
	public WindowResult<QT, T> copy(WindowResult<QT, T> from,
			WindowResult<QT, T> reuse) {
		if (from.isRegularResult()) {
			reuse.query = null;
			reuse.result = resultSerializer.copy(from.result);
		} else if (!from.isEmptyQueryResult()){
			reuse.query = querySerializer.copy(from.query);
			reuse.result = resultSerializer.copy(from.result);
		} else {
			// empty query result
			reuse.query = querySerializer.copy(from.query);
			reuse.result = null;
		}
		return reuse;
	}

	@Override
	public int getLength() {
		return 0;
	}

	@Override
	public void serialize(WindowResult<QT, T> record, DataOutputView target) throws IOException {

		if (record.isRegularResult()) {
			target.writeByte(IS_REGULAR_RESULT);
			resultSerializer.serialize(record.result, target);
		} else if (!record.isEmptyQueryResult()){
			target.writeByte(IS_QUERY_RESULT);
			querySerializer.serialize(record.query, target);
			resultSerializer.serialize(record.result, target);
		} else {
			target.write(IS_EMPTY_QUERY_RESULT);
			querySerializer.serialize(record.query, target);
		}
	}

	@Override
	public WindowResult<QT, T> deserialize(DataInputView source) throws IOException {
		byte type = source.readByte();
		if (type == IS_REGULAR_RESULT) {
			return WindowResult.regularResult(resultSerializer.deserialize(source));
		} else if (type == IS_QUERY_RESULT){
			return WindowResult.queryResult(querySerializer.deserialize(source), resultSerializer.deserialize(source));
		} else {
			return WindowResult.queryResult(querySerializer.deserialize(source), null);
		}
	}

	@Override
	public WindowResult<QT, T> deserialize(WindowResult<QT, T> reuse, DataInputView source) throws IOException {
		byte type = source.readByte();
		if (type == IS_REGULAR_RESULT) {
			reuse.result = resultSerializer.deserialize(source);
			reuse.query = null;
			return reuse;
		} else if (type == IS_QUERY_RESULT){
			reuse.query = querySerializer.deserialize(source);
			reuse.result = resultSerializer.deserialize(source);
			return reuse;
		} else {
			reuse.result = null;
			reuse.query = querySerializer.deserialize(source);
			return reuse;
		}
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		byte type = source.readByte();
		target.writeByte(type);
		if (type == IS_REGULAR_RESULT) {
			resultSerializer.copy(source, target);
		} else if (type == IS_QUERY_RESULT){
			querySerializer.copy(source, target);
			resultSerializer.copy(source, target);
		} else {
			// empty query result
			querySerializer.copy(source, target);
		}

	}

	@Override
	@SuppressWarnings("unchecked,rawtypes")
	public boolean equals(Object obj) {
		if (obj instanceof WindowResultSerializer) {
			WindowResultSerializer windowResultSerializer = (WindowResultSerializer) obj;
			return querySerializer.canEqual(windowResultSerializer.querySerializer) &&
					resultSerializer.canEqual(windowResultSerializer.resultSerializer) &&
					querySerializer.equals(windowResultSerializer.querySerializer) &&
					resultSerializer.equals(windowResultSerializer.resultSerializer);

		}
		return false;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof WindowResultSerializer;
	}

	@Override
	public int hashCode() {
		return Objects.hash(resultSerializer, querySerializer);
	}
}

