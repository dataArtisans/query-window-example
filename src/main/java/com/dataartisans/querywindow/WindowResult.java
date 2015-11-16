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

/**
 * Result type for {@link QueryableWindowOperator}. If a result is the result of a query
 * operation this contains the result along with the original query element.
 *
 * <p>This should not be directly used, only as the result type of window query operations.
 *
 * @param <QT> The type of the query elements.
 * @param <T> Result type of the window operation.
 */
public final class WindowResult<QT, T> {
	protected T result;
	protected QT query;

	private WindowResult(T result, QT query) {
		this.result = result;
		this.query = query;
	}

	/**
	 * Creates a wrapped regular window result.
	 *
	 * @param regularResult The windowing result
	 *
	 * @param <QT> The type of the query elements.
	 * @param <T> Result type of the window operation.
	 */
	public static <T, QT> WindowResult<QT, T> regularResult(T regularResult) {
		return new WindowResult<>(regularResult, null);
	}

	/**
	 * Creates a wrapped window query result/
	 *
	 * @param query The query element.
	 * @param result The windowing result
	 *
	 * @param <QT> The type of the query elements.
	 * @param <T> Result type of the window operation.
	 */
	public static <T, QT> WindowResult<QT, T> queryResult(QT query, T result) {
		return new WindowResult<>(result, query);
	}

	/**
	 * Returns {@code true} if this is the result of a regular window evaluation.
	 */
	public boolean isRegularResult() {
		return query == null;
	}

	/**
	 * Returns {@code true} if this is the result of a window query.
	 * window evaluation or the result of a window query.
	 */
	public boolean isQueryResult() {
		return query != null;
	}

	/**
	 * Returns {@code true} if this is the result of a window query where the operator
	 * does not have any result for the query key.
	 */
	public boolean isEmptyQueryResult() {
		return result == null && query != null;
	}

	/**
	 * Returns the windowing result. The result can either be the result of a regular
	 * window evaluation or the result of a window query.
	 */		public T result() {
		return result;
	}

	/**
	 * Returns the query element if this is the result of a window query. Otherwise returns
	 * {@code Null}.
	 */
	public QT query() {
		return query;
	}

	@Override
	public String toString() {
		if (result != null && query == null) {
			return "RegularResult(" + result + ")";
		} else {
			return "QueryResult(" + query + ": " + result + ")";
		}
	}
}
