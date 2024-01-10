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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.Text;

public class BigQueryResultImpl<T> implements BigQueryResult<T> {

  private static final String NULL_CURSOR_MSG =
      "Error occurred while reading the cursor. This could happen if getters are called after we are done reading all the records";

  // This class represents a row of records, the columns are represented as a map
  // (columnName:columnValue pair)
  static class Row {
    private Map<String, Object> value;
    private boolean isLast;

    public Row(Map<String, Object> value) {
      this.value = value;
    }

    public Row(Map<String, Object> value, boolean isLast) {
      this.value = value;
      this.isLast = isLast;
    }

    public Map<String, Object> getValue() {
      return value;
    }

    public boolean isLast() {
      return isLast;
    }

    public boolean hasField(String fieldName) {
      return this.value.containsKey(fieldName);
    }

    public Object get(String fieldName) {
      return this.value.get(fieldName);
    }
  }

  private final Schema schema;
  private final long totalRows;
  private final BlockingQueue<T> buffer;
  private T cursor;
  private final BigQueryResultSet underlyingResultSet;
  private final BigQueryResultStats bigQueryResultStats;
  private final FieldList schemaFieldList;

  public BigQueryResultImpl(
      Schema schema,
      long totalRows,
      BlockingQueue<T> buffer,
      BigQueryResultStats bigQueryResultStats) {
    this.schema = schema;
    this.totalRows = totalRows;
    this.buffer = buffer;
    this.underlyingResultSet = new BigQueryResultSet();
    this.bigQueryResultStats = bigQueryResultStats;
    this.schemaFieldList = schema.getFields();
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

  private class BigQueryResultSet extends AbstractJdbcResultSet {
    private boolean hasReachedEnd =
        false; // flag which will be set to true when we have encountered a EndOfStream or when
    // curTup.isLast(). Ref: https://github.com/googleapis/java-bigquery/issues/2033

    @Override
    /*Advances the result set to the next row, returning false if no such row exists. Potentially blocking operation*/
    public boolean next() throws SQLException {
      if (hasReachedEnd) { // if end of stream is reached then we can simply return false
        return false;
      }
      try {
        cursor = buffer.take(); // advance the cursor,Potentially blocking operation
        if (isEndOfStream(cursor)) { // check for end of stream
          cursor = null;
          hasReachedEnd = true;
          return false;
        } else if (cursor instanceof Row) {
          Row curTup = (Row) cursor;
          if (curTup.isLast()) { // last Tuple
            cursor = null;
            hasReachedEnd = true;
            return false;
          }
          return true;
        } else if (cursor instanceof FieldValueList) { // cursor is advanced, we can return true now
          return true;
        } else { // this case should never occur as the cursor will either be a Row of EoS
          throw new BigQuerySQLException("Could not process the current row");
        }
      } catch (InterruptedException e) {
        throw new SQLException(
            "Error occurred while advancing the cursor. This could happen when connection is closed while we call the next method");
      }
    }

    private boolean isEndOfStream(T cursor) {
      return cursor instanceof ConnectionImpl.EndOfFieldValueList;
    }

    @Override
    public Object getObject(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return (fieldValue == null || fieldValue.getValue() == null) ? null : fieldValue.getValue();
      } else { // Data received from Read API (Arrow)
        Row curRow = (Row) cursor;
        if (!curRow.hasField(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        return curRow.get(fieldName);
      }
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null;
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return (fieldValue == null || fieldValue.getValue() == null) ? null : fieldValue.getValue();
      } else { // Data received from Read API (Arrow)
        return getObject(schemaFieldList.get(columnIndex).getName());
      }
    }

    @Override
    public String getString(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        if ((fieldValue == null || fieldValue.getValue() == null)) {
          return null;
        } else if (fieldValue
            .getAttribute()
            .equals(FieldValue.Attribute.REPEATED)) { // Case for Arrays
          return fieldValue.getValue().toString();
        } else {
          return fieldValue.getStringValue();
        }
      } else { // Data received from Read API (Arrow)
        Row curRow = (Row) cursor;
        if (!curRow.hasField(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object currentVal = curRow.get(fieldName);
        if (currentVal == null) {
          return null;
        } else if (currentVal instanceof JsonStringArrayList) { // arrays
          JsonStringArrayList jsnAry = (JsonStringArrayList) currentVal;
          return jsnAry.toString();
        } else if (currentVal instanceof LocalDateTime) {
          LocalDateTime dateTime = (LocalDateTime) currentVal;
          return dateTime.toString();
        } else {
          Text textVal = (Text) currentVal;
          return textVal.toString();
        }
      }
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null;
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? null
            : fieldValue.getStringValue();
      } else { // Data received from Read API (Arrow)
        return getString(schemaFieldList.get(columnIndex).getName());
      }
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
        return (fieldValue == null || fieldValue.getValue() == null)
            ? 0
            : fieldValue.getNumericValue().intValue();
      } else { // Data received from Read API (Arrow)

        Row curRow = (Row) cursor;
        if (!curRow.hasField(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object curVal = curRow.get(fieldName);
        if (curVal == null) {
          return 0;
        }
        if (curVal instanceof Text) { // parse from text to int
          return Integer.parseInt(((Text) curVal).toString());
        } else if (curVal
            instanceof
            Long) { // incase getInt is called for a Long value. Loss of precision might occur
          return ((Long) curVal).intValue();
        }
        return ((BigDecimal) curVal).intValue();
      }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
      if (cursor == null) {
        return 0; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? 0
            : fieldValue.getNumericValue().intValue();
      } else { // Data received from Read API (Arrow)
        return getInt(schemaFieldList.get(columnIndex).getName());
      }
    }

    @Override
    public long getLong(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? 0L
            : fieldValue.getNumericValue().longValue();
      } else { // Data received from Read API (Arrow)
        Row curRow = (Row) cursor;
        if (!curRow.hasField(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object curVal = curRow.get(fieldName);
        if (curVal == null) {
          return 0L;
        } else { // value will be Long or BigDecimal, but are Number
          return ((Number) curVal).longValue();
        }
      }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
      if (cursor == null) {
        return 0L; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? 0L
            : fieldValue.getNumericValue().longValue();
      } else { // Data received from Read API (Arrow)
        return getInt(schemaFieldList.get(columnIndex).getName());
      }
    }

    @Override
    public double getDouble(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? 0d
            : fieldValue.getNumericValue().doubleValue();
      } else { // Data received from Read API (Arrow)
        Row curRow = (Row) cursor;
        if (!curRow.hasField(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object curVal = curRow.get(fieldName);
        return curVal == null ? 0.0d : new BigDecimal(curVal.toString()).doubleValue();
      }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
      if (cursor == null) {
        return 0d; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? 0d
            : fieldValue.getNumericValue().doubleValue();
      } else { // Data received from Read API (Arrow)
        return getDouble(schemaFieldList.get(columnIndex).getName());
      }
    }

    @Override
    public BigDecimal getBigDecimal(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? null
            : BigDecimal.valueOf(fieldValue.getNumericValue().doubleValue());
      } else { // Data received from Read API (Arrow)
        return BigDecimal.valueOf(getDouble(fieldName));
      }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
      if (cursor == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? null
            : BigDecimal.valueOf(fieldValue.getNumericValue().doubleValue());
      } else { // Data received from Read API (Arrow)
        return getBigDecimal(schemaFieldList.get(columnIndex).getName());
      }
    }

    @Override
    public boolean getBoolean(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() != null && fieldValue.getBooleanValue();
      } else { // Data received from Read API (Arrow)
        Row curRow = (Row) cursor;
        if (!curRow.hasField(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object curVal = curRow.get(fieldName);
        return curVal != null && (Boolean) curVal;
      }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
      if (cursor == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() != null && fieldValue.getBooleanValue();
      } else { // Data received from Read API (Arrow)
        return getBoolean(schemaFieldList.get(columnIndex).getName());
      }
    }

    @Override
    public byte[] getBytes(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new SQLException("fieldName can't be null");
      }
      if (cursor == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? null
            : fieldValue.getBytesValue();
      } else { // Data received from Read API (Arrow)
        Row curRow = (Row) cursor;
        if (!curRow.hasField(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object curVal = curRow.get(fieldName);
        return curVal == null ? null : (byte[]) curVal;
      }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? null
            : fieldValue.getBytesValue();
      } else { // Data received from Read API (Arrow)
        return getBytes(schemaFieldList.get(columnIndex).getName());
      }
    }

    @Override
    public Timestamp getTimestamp(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      }
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? null
            : new Timestamp(
                fieldValue.getTimestampValue()
                    / 1000); // getTimestampValue returns time in microseconds, and TimeStamp
        // expects it in millis
      } else {
        Row curRow = (Row) cursor;
        if (!curRow.hasField(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object timeStampVal = curRow.get(fieldName);
        return timeStampVal == null
            ? null
            : new Timestamp((Long) timeStampVal / 1000); // Timestamp is represented as a Long
      }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
      if (cursor == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? null
            : new Timestamp(
                fieldValue.getTimestampValue()
                    / 1000); // getTimestampValue returns time in microseconds, and TimeStamp
        // expects it in millis
      } else { // Data received from Read API (Arrow)
        return getTimestamp(schemaFieldList.get(columnIndex).getName());
      }
    }

    @Override
    public Time getTime(String fieldName) throws SQLException {
      if (fieldName == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      }
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return getTimeFromFieldVal(fieldValue);
      } else { // Data received from Read API (Arrow)
        Row curRow = (Row) cursor;
        if (!curRow.hasField(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object timeStampObj = curRow.get(fieldName);
        return timeStampObj == null
            ? null
            : new Time(
                ((Long) timeStampObj)
                    / 1000); // Time.toString() will return 12:11:35 in GMT as 17:41:35 in
        // (GMT+5:30). This can be offset using getTimeZoneOffset
      }
    }

    private int getTimeZoneOffset() {
      TimeZone timeZone = TimeZone.getTimeZone(ZoneId.systemDefault());
      return timeZone.getOffset(new java.util.Date().getTime()); // offset in seconds
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
      if (cursor == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return getTimeFromFieldVal(fieldValue);
      } else { // Data received from Read API (Arrow)
        return getTime(schemaFieldList.get(columnIndex).getName());
      }
    }

    private Time getTimeFromFieldVal(FieldValue fieldValue) throws SQLException {
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
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? null
            : Date.valueOf(fieldValue.getStringValue());
      } else { // Data received from Read API (Arrow)
        Row curRow = (Row) cursor;
        if (!curRow.hasField(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object dateObj = curRow.get(fieldName);
        if (dateObj == null) {
          return null;
        } else {
          Integer dateInt = (Integer) dateObj;
          long dateInMillis =
              TimeUnit.DAYS.toMillis(
                  Long.valueOf(
                      dateInt)); // For example int 18993 represents 2022-01-01, converting time to
          // milli seconds
          return new Date(dateInMillis);
        }
      }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
      if (cursor == null) {
        throw new BigQuerySQLException(NULL_CURSOR_MSG);
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return (fieldValue == null || fieldValue.getValue() == null)
            ? null
            : Date.valueOf(fieldValue.getStringValue());
      } else { // Data received from Read API (Arrow)
        return getDate(schemaFieldList.get(columnIndex).getName());
      }
    }
  }

  @Override
  public BigQueryResultStats getBigQueryResultStats() {
    return bigQueryResultStats;
  }
}
