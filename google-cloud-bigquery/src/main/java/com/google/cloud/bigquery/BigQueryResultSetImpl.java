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

import com.google.cloud.Tuple;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.Text;

// TODO: This implementation deals with the JSON response. We can have respective implementations
public class BigQueryResultSetImpl<T> implements BigQueryResultSet<T> {

  private final Schema schema;
  private final long totalRows;
  private final BlockingQueue<T> buffer;
  private T cursor;
  private final ResultSetWrapper underlyingResultSet;
  private final BigQueryResultSetStats bigQueryResultSetStats;
  private final FieldList schemaFieldList;

  public BigQueryResultSetImpl(
      Schema schema,
      long totalRows,
      BlockingQueue<T> buffer,
      BigQueryResultSetStats bigQueryResultSetStats) {
    this.schema = schema;
    this.totalRows = totalRows;
    this.buffer = buffer;
    this.underlyingResultSet = new ResultSetWrapper();
    this.bigQueryResultSetStats = bigQueryResultSetStats;
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

  private class ResultSetWrapper extends AbstractJdbcResultSet {
    @Override
    /*Advances the result set to the next row, returning false if no such row exists. Potentially blocking operation*/
    public boolean next() throws SQLException {
      try {
        cursor = buffer.take(); // advance the cursor,Potentially blocking operation
        if (isEndOfStream(cursor)) { // check for end of stream
          cursor = null;
          return false;
        } else if (cursor instanceof Tuple) {
          Tuple<Map<String, Object>, Boolean> curTup = (Tuple<Map<String, Object>, Boolean>) cursor;
          if (!curTup.y()) { // last Tuple
            cursor = null;
            return false;
          }
        }
      } catch (InterruptedException e) {
        throw new SQLException("Error occurred while reading buffer");
      }

      return true;
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
        return null;
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? null : fieldValue.getValue();
      } else { // Data received from Read API (Arrow)
        Tuple<Map<String, Object>, Boolean> curTuple = (Tuple<Map<String, Object>, Boolean>) cursor;
        if (!curTuple.x().containsKey(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        return curTuple.x().get(fieldName);
      }
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null;
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? null : fieldValue.getValue();
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
        return null;
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? null : fieldValue.getStringValue();
      } else { // Data received from Read API (Arrow)
        Tuple<Map<String, Object>, Boolean> curTuple = (Tuple<Map<String, Object>, Boolean>) cursor;
        if (!curTuple.x().containsKey(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object currentVal = curTuple.x().get(fieldName);
        if (currentVal == null) {
          return null;
        }
        if (currentVal instanceof JsonStringArrayList) { // arrays
          JsonStringArrayList jsnAry = (JsonStringArrayList) currentVal;
          return jsnAry.toString();
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
        return fieldValue.getValue() == null ? null : fieldValue.getStringValue();
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
        return fieldValue.getValue() == null ? 0 : fieldValue.getNumericValue().intValue();
      } else { // Data received from Read API (Arrow)

        Tuple<Map<String, Object>, Boolean> curTuple = (Tuple<Map<String, Object>, Boolean>) cursor;
        if (!curTuple.x().containsKey(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object curVal = curTuple.x().get(fieldName);
        return curVal == null ? 0 : ((BigDecimal) curVal).intValue();
      }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
      if (cursor == null) {
        return 0; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? 0 : fieldValue.getNumericValue().intValue();
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
        return 0L; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? 0L : fieldValue.getNumericValue().longValue();
      } else { // Data received from Read API (Arrow)
        Tuple<Map<String, Object>, Boolean> curTuple = (Tuple<Map<String, Object>, Boolean>) cursor;
        if (!curTuple.x().containsKey(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object curVal = curTuple.x().get(fieldName);
        return curVal == null ? 0 : ((BigDecimal) curVal).longValue();
      }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
      if (cursor == null) {
        return 0L; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? 0L : fieldValue.getNumericValue().longValue();
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
        return 0d; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? 0d : fieldValue.getNumericValue().doubleValue();
      } else { // Data received from Read API (Arrow)
        Tuple<Map<String, Object>, Boolean> curTuple = (Tuple<Map<String, Object>, Boolean>) cursor;
        if (!curTuple.x().containsKey(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object curVal = curTuple.x().get(fieldName);
        return curVal == null ? 0.0d : ((BigDecimal) curVal).doubleValue();
      }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
      if (cursor == null) {
        return 0d; // the column value; if the value is SQL NULL, the value returned is 0 as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? 0d : fieldValue.getNumericValue().doubleValue();
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
        return null; // the column value (full precision); if the value is SQL NULL, the value
        // returned is null in the Java programming language. as per
        // java.sql.ResultSet definition
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null
            ? null
            : BigDecimal.valueOf(fieldValue.getNumericValue().doubleValue());
      } else { // Data received from Read API (Arrow)
        return BigDecimal.valueOf(getDouble(fieldName));
      }
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
        return false; //  if the value is SQL NULL, the value returned is false
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() != null && fieldValue.getBooleanValue();
      } else { // Data received from Read API (Arrow)
        Tuple<Map<String, Object>, Boolean> curTuple = (Tuple<Map<String, Object>, Boolean>) cursor;
        if (!curTuple.x().containsKey(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object curVal = curTuple.x().get(fieldName);
        return curVal != null && (Boolean) curVal;
      }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
      if (cursor == null) {
        return false; //  if the value is SQL NULL, the value returned is false
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
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? null : fieldValue.getBytesValue();
      } else { // Data received from Read API (Arrow)
        Tuple<Map<String, Object>, Boolean> curTuple = (Tuple<Map<String, Object>, Boolean>) cursor;
        if (!curTuple.x().containsKey(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object curVal = curTuple.x().get(fieldName);
        return curVal == null ? null : (byte[]) curVal;
      }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? null : fieldValue.getBytesValue();
      } else { // Data received from Read API (Arrow)
        return getBytes(schemaFieldList.get(columnIndex).getName());
      }
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
      } else {
        Tuple<Map<String, Object>, Boolean> curTuple = (Tuple<Map<String, Object>, Boolean>) cursor;
        if (!curTuple.x().containsKey(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object timeStampVal = curTuple.x().get(fieldName);
        return timeStampVal == null
            ? null
            : new Timestamp((Long) timeStampVal / 1000); // Timestamp is represented as a Long
      }
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
      } else { // Data received from Read API (Arrow)
        return getTimestamp(schemaFieldList.get(columnIndex).getName());
      }
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
        return getTimeFromFieldVal(fieldValue);
      } else { // Data received from Read API (Arrow)
        Tuple<Map<String, Object>, Boolean> curTuple = (Tuple<Map<String, Object>, Boolean>) cursor;
        if (!curTuple.x().containsKey(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object timeStampObj = curTuple.x().get(fieldName);
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
        return null; //  if the value is SQL NULL, the value returned is null
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
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(fieldName);
        return fieldValue.getValue() == null ? null : Date.valueOf(fieldValue.getStringValue());
      } else { // Data received from Read API (Arrow)
        Tuple<Map<String, Object>, Boolean> curTuple = (Tuple<Map<String, Object>, Boolean>) cursor;
        if (!curTuple.x().containsKey(fieldName)) {
          throw new SQLException(String.format("Field %s not found", fieldName));
        }
        Object dateObj = curTuple.x().get(fieldName);
        if (dateObj == null) {
          return null;
        } else {
          Integer dateInt = (Integer) dateObj;
          long dateInMillis =
              Long.valueOf(dateInt)
                  * (24 * 60 * 60
                      * 1000); // For example int 18993 represents 2022-01-01, converting time to
          // milli seconds
          return new Date(dateInMillis);
        }
      }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
      if (cursor == null) {
        return null; //  if the value is SQL NULL, the value returned is null
      } else if (cursor instanceof FieldValueList) {
        FieldValue fieldValue = ((FieldValueList) cursor).get(columnIndex);
        return fieldValue.getValue() == null ? null : Date.valueOf(fieldValue.getStringValue());
      } else { // Data received from Read API (Arrow)
        return getDate(schemaFieldList.get(columnIndex).getName());
      }
    }
  }

  @Override
  public BigQueryResultSetStats getBigQueryResultSetStats() {
    return bigQueryResultSetStats;
  }
}
