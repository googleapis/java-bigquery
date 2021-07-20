/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigquery;

public class BigQueryResultSetImpl<T> implements BigQueryResultSet<T> {

  private final Schema schema;
  private final long totalRows;
  private final T nextRow;

  public BigQueryResultSetImpl(Schema schema, long totalRows, T nextRow) {
    this.schema = schema;
    this.totalRows = totalRows;
    this.nextRow = nextRow;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public long getTotalRows() {
    return totalRows;
  }

  @Override
  public T getNext() {
    return null;
  }
}
