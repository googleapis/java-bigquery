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
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.concurrent.BlockingQueue;

// TODO: This implementation deals with the JSON response. We can have respective implementations
public class BigQueryResultSetImpl<T> implements BigQueryResultSet<T> {

  private final Schema schema;
  private final long totalRows;
  private final BlockingQueue<T> buffer;
  private T cursor;
  private final ResultSetWrapper underlyingResultSet;

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
      try {
        cursor = buffer.take(); // advance the cursor,Potentially blocking operation
        if (isEndOfStream(cursor)) { // check for end of stream
          cursor = null;
          return false;
        }
      } catch (InterruptedException e) {
        throw new SQLException("Error occurred while reading buffer");
      }

      return true;
    }

    private boolean isEndOfStream(T cursor) {
      return cursor
          instanceof
          ConnectionImpl.EndOfFieldValueList; // TODO: Similar check will be required for Arrow
    }

    @Override
    public Object getObject(String fieldName) throws SQLException {
      if (cursor != null && cursor instanceof TableRow) {
        TableRow currentRow = (TableRow) cursor;
        if (fieldName == null) {
          throw new SQLException("fieldName can't be null");
        }
        return currentRow.get(fieldName);
      }
      // TODO: Add similar clauses for Apache Arrow
      return null;
    }

    @Override
    public String getString(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return null;
      } else if (cursor instanceof FieldValueList) {
        return ((FieldValueList) cursor).get(fieldName).getStringValue();
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public int getInt(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return 0; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        return ((FieldValueList) cursor).get(fieldName).getNumericValue().intValue();
      }
      return 0; // TODO: Implementation for Arrow
    }

    @Override
    public long getLong(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return 0l; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        return ((FieldValueList) cursor).get(fieldName).getNumericValue().longValue();
      }
      return 0; // TODO: Implementation for Arrow
    }

    @Override
    public double getDouble(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return 0d; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        return ((FieldValueList) cursor).get(fieldName).getNumericValue().doubleValue();
      }
      return 0; // TODO: Implementation for Arrow
    }

    @Override
    public BigDecimal getBigDecimal(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return null; // the column value (full precision); if the value is SQL NULL, the value
        // returned is null in the Java programming language. as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        return BigDecimal.valueOf(
            ((FieldValueList) cursor).get(fieldName).getNumericValue().doubleValue());
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public boolean getBoolean(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return false; //  if the value is SQL NULL, the value returned is false
      } else if (cursor instanceof FieldValueList) {
        return ((FieldValueList) cursor).get(fieldName).getBooleanValue();
      }
      return false; // TODO: Implementation for Arrow
    }

    @Override
    public byte[] getBytes(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        return ((FieldValueList) cursor).get(fieldName).getBytesValue();
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public Timestamp getTimestamp(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        return new Timestamp(((FieldValueList) cursor).get(fieldName).getTimestampValue());
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public Time getTime(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        return new Time(((FieldValueList) cursor).get(fieldName).getTimestampValue());
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public Date getDate(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        return new Date(((FieldValueList) cursor).get(fieldName).getTimestampValue());
      }
      return null; // TODO: Implementation for Arrow
    }
  }
}
