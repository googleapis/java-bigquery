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

import com.google.api.services.bigquery.model.QueryParameter;
import java.util.List;
import java.util.Map;

/**
 * A Connection is a session between a Java application and BigQuery. SQL statements are executed
 * and results are returned within the context of a connection.
 */
public interface QueryConnection {

  /**
   * Sets how long to wait for the query to complete, in milliseconds, before the request times out
   * and returns. Note that this is only a timeout for the request, not the query. If the query
   * takes longer to run than the timeout value, the call returns without any results and with the
   * 'jobComplete' flag set to false. You can call GetQueryResults() to wait for the query to
   * complete and read the results. The default value is 10000 milliseconds (10 seconds).
   *
   * @param timeoutMs or {@code null} for none
   */
  void setSynchronousResponseTimeoutMs(Long timeoutMs) throws BigQuerySQLException;

  /** Returns the synchronous response timeoutMs associated with this query */
  Long getSynchronousResponseTimeoutMs() throws BigQuerySQLException;

  /**
   * Sets a connection-level property to customize query behavior. Under JDBC, these correspond
   * directly to connection properties passed to the DriverManager.
   *
   * @param connectionProperties connectionProperties or {@code null} for none
   */
  void setConnectionProperties(List<ConnectionProperty> connectionProperties)
      throws BigQuerySQLException;

  /** Returns the connection properties for connection string with this query */
  List<ConnectionProperty> getConnectionProperties() throws BigQuerySQLException;

  /**
   * Sets the default dataset. This dataset is used for all unqualified table names used in the
   * query.
   */
  void setDefaultDataset() throws BigQuerySQLException;

  /** Returns the default dataset */
  DatasetId getDefaultDataset() throws BigQuerySQLException;

  /**
   * Sets the labels associated with this query. You can use these to organize and group your
   * queries. Label keys and values can be no longer than 63 characters, can only contain lowercase
   * letters, numeric characters, underscores and dashes. International characters are allowed.
   * Label values are optional. Label keys must start with a letter and each label in the list must
   * have a different key.
   *
   * @param labels labels or {@code null} for none
   */
  void setLabels(Map<String, String> labels) throws BigQuerySQLException;

  /** Returns the labels associated with this query */
  Map<String, String> getLabels() throws BigQuerySQLException;

  /** Clear the labels associated with this query */
  void cleaLabels() throws BigQuerySQLException;

  /**
   * Limits the bytes billed for this job. Queries that will have bytes billed beyond this limit
   * will fail (without incurring a charge). If unspecified, this will be set to your project
   * default.
   *
   * @param maximumBytesBilled maximum bytes billed for this job
   */
  void setMaximumBytesBilled(Long maximumBytesBilled) throws BigQuerySQLException;

  /** Returns the limits the bytes billed for this job */
  Long getMaximumBytesBilled() throws BigQuerySQLException;

  /**
   * Sets the maximum number of rows of data to return per page of results. Setting this flag to a
   * small value such as 1000 and then paging through results might improve reliability when the
   * query result set is large. In addition to this limit, responses are also limited to 10 MB. By
   * default, there is no maximum row count, and only the byte limit applies.
   *
   * @param maxResults maxResults or {@code null} for none
   */
  void setMaxResults(Long maxResults) throws BigQuerySQLException;

  /** Returns the maximum number of rows of data */
  Long getMaxResults() throws BigQuerySQLException;

  /** Returns query parameters for standard SQL queries */
  List<QueryParameter> getQueryParameters() throws BigQuerySQLException;

  /**
   * Sets whether to look for the result in the query cache. The query cache is a best-effort cache
   * that will be flushed whenever tables in the query are modified. Moreover, the query cache is
   * only available when {@link QueryJobConfiguration.Builder#setDestinationTable(TableId)} is not
   * set.
   *
   * @see <a href="https://cloud.google.com/bigquery/querying-data#querycaching">Query Caching</a>
   */
  void setUseQueryCache(Boolean useQueryCache) throws BigQuerySQLException;

  /** Returns whether to look for the result in the query cache */
  Boolean getUseQueryCache() throws BigQuerySQLException;

  /**
   * Execute a query dry run that does not return any BigQueryResultSet
   *
   * @param sql typically a static SQL SELECT statement
   * @exception BigQuerySQLException if a database access error occurs
   */
  void dryRun(String sql) throws BigQuerySQLException;

  /**
   * Execute a SQL statement that returns a single BigQueryResultSet
   *
   * @param sql typically a static SQL SELECT statement
   * @return a BigQueryResultSet that contains the data produced by the query
   * @exception BigQuerySQLException if a database access error occurs
   */
  BigQueryResultSet executeSelect(String sql) throws BigQuerySQLException;

  /**
   * Execute a SQL statement with query parameters that returns a single BigQueryResultSet
   *
   * @param sql typically a static SQL SELECT statement
   * @param parameters named or positional parameters
   * @return a BigQueryResultSet that contains the data produced by the query
   * @exception BigQuerySQLException if a database access error occurs
   */
  BigQueryResultSet executeSelect(String sql, List<Parameter> parameters)
      throws BigQuerySQLException;

  /**
   * Sets the values necessary to determine whether table result will be read using the BigQuery
   * Storage client Read API. The BigQuery Storage client Read API will be used to read the query
   * result when the totalToFirstPageSizeRatio (default 3) and minimumTableSize (default 100MB)
   * conditions set are met. A ReadSession will be created using Apache Avro data format for
   * serialization.
   *
   * <p>It also sets the maximum number of table rows allowed in buffer before streaming them to the
   * BigQueryResultSet.
   *
   * @param readClientConnectionConfiguration or {@code null} for none
   * @exception BigQueryException if an error occurs in setting these values
   */
  void setReadClientConnectionConfiguration(
      ReadClientConnectionConfiguration readClientConnectionConfiguration) throws BigQueryException;
}

class Parameter {
  String name;
  QueryParameterValue value;
}
