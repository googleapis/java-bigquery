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

import com.google.api.services.bigquery.model.TableRow;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.BlockingQueue;

// TODO: This implementation deals with the JSON response. We can have respective implementations
public class BigQueryResultSetImpl<T> implements BigQueryResultSet<T> {

  private final Schema schema;
  private final long totalRows;
  private final BlockingQueue<T> buffer;
  private T cursor;
  private ResultSetWrapper underlyingResultSet = null;

  // TODO : Implement a wrapper/struct like spanner
  public BigQueryResultSetImpl(Schema schema, long totalRows, BlockingQueue<T> buffer) {
    this.schema = schema;
    this.totalRows = totalRows;
    this.buffer = buffer;
    this.underlyingResultSet = new ResultSetWrapper();
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
  public ResultSet getResultSet() {
    return underlyingResultSet;
  }

  private class ResultSetWrapper extends AbstractJdbcResultSet {
    @Override
    /*Advances the result set to the next row, returning false if no such row exists. Potentially blocking operation*/
    public boolean next() throws SQLException {
      if (buffer.peek() == null) { // producer has no more rows left.
        return false;
      }
      try {
        cursor = buffer.take(); // advance the cursor,Potentially blocking operation
      } catch (InterruptedException e) {
        throw new SQLException("No rows left");
      }

      return true;
    }

    @Override
    public Object getObject(String fieldName) throws SQLException {
      if (cursor instanceof TableRow) {
        TableRow currentRow = (TableRow) cursor;
        if (currentRow == null) {
          throw new SQLException("No rows left");
        }
        return currentRow.get(fieldName);
      }
      // TODO: Add similar clauses for Apache Arrow
      return null;
    }

    @Override
    public String getString(String fieldName) throws SQLException {
      Object value = getObject(fieldName);
      if (value == null) {
        throw new SQLException("fieldName can't be null");
      } else if (!(value instanceof String)) {
        throw new SQLException("value not in instance of String");
      } else {
        return (String) value;
      }
    }

    @Override
    public long getLong(String fieldName) throws SQLException {
      Object value = getObject(fieldName);
      if (value == null) {
        throw new SQLException("fieldName can't be null");
      } else if (!(value instanceof Long)) {
        throw new SQLException("value not in instance of Long");
      } else {
        return Long.parseLong(String.valueOf(value));
      }
    }

    @Override
    public double getDouble(String fieldName) throws SQLException {
      Object value = getObject(fieldName);
      if (value == null) {
        throw new SQLException("fieldName can't be null");
      } else if (!(value instanceof Double)) {
        throw new SQLException("value not in instance of Double");
      } else {
        return Double.parseDouble(String.valueOf(value));
      }
    }

    @Override
    public BigDecimal getBigDecimal(String fieldName) throws SQLException {
      Object value = getObject(fieldName);
      if (value == null) {
        throw new SQLException("fieldName can't be null");
      } else if (!(value instanceof Long
          || value instanceof Double
          || value instanceof BigDecimal
          || value instanceof String)) {
        throw new SQLException("value cannot be converted to BigDecimal");
      } else {
        return new BigDecimal(String.valueOf(value));
      }
    }

    @Override
    public boolean getBoolean(String fieldName) throws SQLException {
      Object value = getObject(fieldName);
      if (value == null) {
        throw new SQLException("fieldName can't be null");
      } else if (!(value instanceof Boolean || (value instanceof String))) {
        throw new SQLException("value not in instance of Boolean");
      } else {
        return Boolean.parseBoolean(String.valueOf(value));
      }
    }

    @Override
    public byte getByte(String fieldName) throws SQLException {
      Object value = getObject(fieldName);
      if (value == null) {
        throw new SQLException("fieldName can't be null");
      } else if (!(value instanceof Byte || (value instanceof String))) {
        throw new SQLException("value not in instance of Boolean");
      } else {
        return Byte.parseByte(String.valueOf(value));
      }
    }

    @Override
    public Timestamp getTimestamp(String fieldName) throws SQLException {
      Object value = getObject(fieldName);
      if (value == null) {
        throw new SQLException("fieldName can't be null");
      } else if (!(value instanceof Long
          || value instanceof Timestamp
          || value instanceof String)) {
        throw new SQLException("value cannot be converted to Timestamp");
      } else {
        return Timestamp.valueOf(String.valueOf(value));
      }
    }

    @Override
    public int getInt(String fieldName) throws SQLException {
      Object value = getObject(fieldName);
      if (value == null) {
        throw new SQLException("fieldName can't be null");
      } else if (!(value instanceof Integer || value instanceof String)) {
        throw new SQLException("value cannot be converted to int");
      } else {
        return Integer.parseInt(String.valueOf(value));
      }
    }
  }
}
