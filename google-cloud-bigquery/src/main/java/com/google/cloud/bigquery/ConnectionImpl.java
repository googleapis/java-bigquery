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
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.cloud.bigquery.spi.v2.BigQueryRpc;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

/** Implementation for {@link Connection}, the generic BigQuery connection API (not JDBC). */
final class ConnectionImpl<T> implements Connection {

  private ConnectionSettings connectionSettings;
  private BigQueryOptions options;
  private BigQueryRpc bigQueryRpc;
  private BigQueryRetryConfig retryConfig;

  ConnectionImpl(
      ConnectionSettings connectionSettings,
      BigQueryOptions options,
      BigQueryRpc bigQueryRpc,
      BigQueryRetryConfig retryConfig) {
    this.connectionSettings = connectionSettings;
    this.options = options;
    this.bigQueryRpc = bigQueryRpc;
    this.retryConfig = retryConfig;
  }

  @Override
  public Boolean cancel() throws BigQuerySQLException {
    return null;
  }

  @Override
  public BigQueryDryRunResult dryRun(String sql) throws BigQuerySQLException {
    return null;
  }

  @Override
  public BigQueryResultSet<T> executeSelect(String sql) throws BigQuerySQLException {
    // use jobs.query if all properties of the Connection are supported
    if (isFastQuerySupported()) {
      String projectId = options.getProjectId();
      QueryRequest queryRequest = createQueryRequest(connectionSettings, sql, null, null);
      return queryRpc(projectId, queryRequest);
    }
    // use jobs.insert otherwise
    return null;
  }

  @Override
  public <T> BigQueryResultSet<T> executeSelect(
      String sql, List<QueryParameter> queryParameters, Map<String, String> labels)
      throws BigQuerySQLException {
    if (isFastQuerySupported()) {
      final String projectId = options.getProjectId();
      final QueryRequest preparedQueryRequest =
          createQueryRequest(connectionSettings, sql, queryParameters, labels);
      // return queryRpc(projectId, preparedQueryRequest);
    }
    return null;
  }

  private BigQueryResultSet<T> queryRpc(final String projectId, final QueryRequest queryRequest) {
    com.google.api.services.bigquery.model.QueryResponse results;
    try {
      results =
          BigQueryRetryHelper.runWithRetries(
              new Callable<QueryResponse>() {
                @Override
                public com.google.api.services.bigquery.model.QueryResponse call() {
                  return bigQueryRpc.queryRpc(projectId, queryRequest);
                }
              },
              options.getRetrySettings(),
              BigQueryBaseService.BIGQUERY_EXCEPTION_HANDLER,
              options.getClock(),
              retryConfig);
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      throw BigQueryException.translateAndThrow(e);
    }
    if (results.getErrors() != null) {
      List<BigQueryError> bigQueryErrors =
          Lists.transform(results.getErrors(), BigQueryError.FROM_PB_FUNCTION);
      // Throwing BigQueryException since there may be no JobId and we want to stay consistent
      // with the case where there there is a HTTP error
      throw new BigQueryException(bigQueryErrors);
    }

    long numRows;
    Schema schema;
    if (results.getJobComplete() && results.getSchema() != null) {
      schema = Schema.fromPb(results.getSchema());
      numRows = results.getTotalRows().longValue();
    } else {
      // Query is long running (> 10s) and hasn't completed yet, or query completed but didn't
      // return the schema, fallback. Some operations don't return the schema and can be optimized
      // here, but this is left as future work.
      JobId jobId = JobId.fromPb(results.getJobReference());
      return null;
    }

    // TODO(prasmish): WRITE A PRIVATE METHOD TO CACHE FIRST PAGE
    // cacheFirstPage(...)

    if (results.getPageToken() != null) {
      JobId jobId = JobId.fromPb(results.getJobReference());
      return new BigQueryResultSetImpl<>(
          schema, numRows, null
          /** iterate(cachedFirstPage)* */
          );
    }

    // only 1 page of result
    // TODO(prasmish): GET NEXT ROW AND PASS INTO BigQueryResultSetImpl

    // Query is long running (> 10s) and hasn't completed yet or has more than 1 pages of results
    // Use tabledata.list or high through-put Read API to fetch the rest of the pages
    JobId jobId = JobId.fromPb(results.getJobReference());
    if (useReadAPI(results)) {
      // Discard the first page and use BigQuery Storage Read API to fetch the entire result set

    } else {
      // Use tabledata.list to fetch the rest of the result set

    }
    return null;
  }

  private boolean isFastQuerySupported() {
    return connectionSettings.getClustering() == null
        && connectionSettings.getCreateDisposition() == null
        && connectionSettings.getDestinationEncryptionConfiguration() == null
        && connectionSettings.getDestinationTable() == null
        && connectionSettings.getJobTimeoutMs() == null
        && connectionSettings.getMaximumBillingTier() == null
        && connectionSettings.getPriority() == null
        && connectionSettings.getRangePartitioning() == null
        && connectionSettings.getSchemaUpdateOptions() == null
        && connectionSettings.getTableDefinitions() == null
        && connectionSettings.getTimePartitioning() == null
        && connectionSettings.getUserDefinedFunctions() == null
        && connectionSettings.getWriteDisposition() == null;
  }

  private boolean useReadAPI(QueryResponse response) {
    long totalRows = response.getTotalRows().longValue();
    long pageRows = response.getRows().size();
    long resultRatio = totalRows / pageRows;
    return resultRatio
            > connectionSettings
                .getReadClientConnectionConfiguration()
                .getTotalToPageRowCountRatio()
        && totalRows > connectionSettings.getReadClientConnectionConfiguration().getMinResultSize();
  }

  private QueryRequest createQueryRequest(
      ConnectionSettings connectionSettings,
      String sql,
      List<QueryParameter> queryParameters,
      Map<String, String> labels) {
    QueryRequest content = new QueryRequest();
    String requestId = UUID.randomUUID().toString();

    if (connectionSettings.getConnectionProperties() != null) {
      content.setConnectionProperties(
          Lists.transform(
              connectionSettings.getConnectionProperties(), ConnectionProperty.TO_PB_FUNCTION));
    }
    if (connectionSettings.getDefaultDataset() != null) {
      content.setDefaultDataset(connectionSettings.getDefaultDataset().toPb());
    }
    if (connectionSettings.getMaximumBytesBilled() != null) {
      content.setMaximumBytesBilled(connectionSettings.getMaximumBytesBilled());
    }
    if (connectionSettings.getMaxResults() != null) {
      content.setMaxResults(connectionSettings.getMaxResults());
    }
    if (queryParameters != null) {
      content.setQueryParameters(queryParameters);
    }
    if (labels != null) {
      content.setLabels(labels);
    }
    content.setQuery(sql);
    content.setRequestId(requestId);
    return content;
  }
}
