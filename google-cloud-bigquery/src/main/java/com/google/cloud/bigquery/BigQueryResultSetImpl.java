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

import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.concurrent.BlockingQueue;

// TODO: This implementation deals with the JSON response. We can have respective implementations
public class BigQueryResultSetImpl<T> implements BigQueryResultSet<T> {

  private Schema schema;
  private ArrowSchema arrowSchema;
  private final long totalRows;
  private final BlockingQueue<T> buffer;
  private T cursor;
  private final ResultSetWrapper underlyingResultSet;
  private final BigQueryResultSetStats bigQueryResultSetStats;

  public BigQueryResultSetImpl(
      Object schema,
      long totalRows,
      BlockingQueue<T> buffer,
      BigQueryResultSetStats bigQueryResultSetStats) {
    if (schema instanceof Schema) {
      this.schema = (Schema) schema;
    } else if (schema instanceof ArrowSchema) {
      this.arrowSchema = (ArrowSchema) schema;
    }

    this.totalRows = totalRows;
    this.buffer = buffer;
    this.underlyingResultSet = new ResultSetWrapper();
    this.bigQueryResultSetStats = bigQueryResultSetStats;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  // TODO(prasmish): Implement a method to return ArrowSchema

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
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return null;
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? null : fieldValue.getValue();
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null;
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? null : fieldValue.getValue();
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public String getString(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return null;
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? null : fieldValue.getStringValue();
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null;
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? null : fieldValue.getStringValue();
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
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? 0 : fieldValue.getNumericValue().intValue();
      }
      return 0; // TODO: Implementation for Arrow
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
      if (cursor == null) {
        return 0; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? 0 : fieldValue.getNumericValue().intValue();
      }
      return 0; // TODO: Implementation for Arrow
    }

    @Override
    public long getLong(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return 0L; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? 0L : fieldValue.getNumericValue().longValue();
      }
      return 0L; // TODO: Implementation for Arrow
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
      if (cursor == null) {
        return 0L; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? 0L : fieldValue.getNumericValue().longValue();
      }
      return 0L; // TODO: Implementation for Arrow
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
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? 0d : fieldValue.getNumericValue().doubleValue();
      }
      return 0d; // TODO: Implementation for Arrow
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
      if (cursor == null) {
        return 0d; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? 0d : fieldValue.getNumericValue().doubleValue();
      }
      return 0d; // TODO: Implementation for Arrow
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
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null
            ? null
            : BigDecimal.valueOf(fieldValue.getNumericValue().doubleValue());
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null; // the column value (full precision); if the value is SQL NULL, the value
        // returned is null in the Java programming language. as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null
            ? null
            : BigDecimal.valueOf(fieldValue.getNumericValue().doubleValue());
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
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() != null && fieldValue.getBooleanValue();
      }
      return false; // TODO: Implementation for Arrow
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
      if (cursor == null) {
        return false; //  if the value is SQL NULL, the value returned is false
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() != null && fieldValue.getBooleanValue();
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
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? null : fieldValue.getBytesValue();
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? null : fieldValue.getBytesValue();
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
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null
            ? null
            : new Timestamp(
                fieldValue.getTimestampValue()
                    / 1000); // getTimestampValue returns time in microseconds, and TimeStamp
        // expects it in millis
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null
            ? null
            : new Timestamp(
                fieldValue.getTimestampValue()
                    / 1000); // getTimestampValue returns time in microseconds, and TimeStamp
        // expects it in millis
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
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return getTime(fieldValue);
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return getTime(fieldValue);
      }
      return null; // TODO: Implementation for Arrow
    }

    private Time getTime(FieldValue fieldValue) throws SQLException {
      if (fieldValue.getValue() != null) {
        // Time ranges from 00:00:00 to 23:59:59.99999. in BigQuery. Parsing it to java.sql.Time
        String strTime = fieldValue.getStringValue();
        String[] timeSplt = strTime.split(":");
        if (timeSplt.length != 3) {
          throw new SQLException("Can not parse the value " + strTime + " to java.sql.Time");
        }
        int hr = Integer.parseInt(timeSplt[0]);
        int min = Integer.parseInt(timeSplt[1]);
        int sec = 0, nanoSec = 0;
        if (timeSplt[2].contains(".")) {
          String[] secSplt = timeSplt[2].split("\\.");
          sec = Integer.parseInt(secSplt[0]);
          nanoSec = Integer.parseInt(secSplt[1]);
        } else {
          sec = Integer.parseInt(timeSplt[2]);
        }
        return Time.valueOf(LocalTime.of(hr, min, sec, nanoSec));
      } else {
        return null;
      }
    }

    @Override
    public Date getDate(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? null : Date.valueOf(fieldValue.getStringValue());
      }
      return null; // TODO: Implementation for Arrow
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? null : Date.valueOf(fieldValue.getStringValue());
      }
      return null; // TODO: Implementation for Arrow
    }
  }

  @Override
  public BigQueryResultSetStats getBigQueryResultSetStats() {
    return bigQueryResultSetStats;
  }
}
