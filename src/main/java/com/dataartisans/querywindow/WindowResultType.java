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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * {@link TypeInformation} for {@code WindowResult}.
 *
 * @param <T> The window result type.
 * @param <QT> The type of the query element.
 */
public class WindowResultType<T, QT> extends TypeInformation<WindowResult<QT, T>> {
	private static final long serialVersionUID = 1L;

	TypeInformation<T> resultType;
	TypeInformation<QT> queryType;

	public WindowResultType(TypeInformation<T> resultType,
			TypeInformation<QT> queryType) {
		this.resultType = resultType;
		this.queryType = queryType;
	}

	public TypeInformation<T>  getResultType() {
		return resultType;
	}

	@SuppressWarnings("unchecked,rawtypes")
	public TypeInformation<Tuple2<QT, T>> getQueryType() {
		return new TupleTypeInfo<>(queryType, resultType);
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 0;
	}

	@Override
	public int getTotalFields() {
		return 0;
	}

	@Override
	@SuppressWarnings("unchecked,rawtypes")
	public Class<WindowResult<QT, T>> getTypeClass() {
		return (Class) WindowResult.class;
	}

	@Override
	public List<TypeInformation<?>> getGenericParameters() {
		return Collections.emptyList();
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public boolean isSortKeyType() {
		return false;
	}

	@Override
	@SuppressWarnings("unchecked,rawtypes")
	public TypeSerializer<WindowResult<QT, T>> createSerializer(
			ExecutionConfig config) {
		TypeSerializer<T> resultSerializer = resultType.createSerializer(config);
		TypeSerializer<QT> querySerializer = queryType.createSerializer(config);

		return new WindowResultSerializer<>(resultSerializer, querySerializer);
	}

	@Override
	public String toString() {
		return "WindowResultType(" + resultType + ", " + queryType + ")";
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof WindowResultType;
	}

	@Override
	@SuppressWarnings("unchecked, rawtypes")
	public boolean equals(Object obj) {
		if (obj instanceof WindowResultType) {
			WindowResultType windowResultType = (WindowResultType) obj;
			return queryType.canEqual(windowResultType.queryType) &&
					resultType.canEqual(windowResultType.resultType) &&
					queryType.equals(windowResultType.queryType) &&
					resultType.equals(windowResultType.resultType);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(resultType, queryType);
	}
}

