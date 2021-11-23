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

import static com.google.cloud.RetryHelper.runWithRetries;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

import com.google.api.services.bigquery.model.*;
import com.google.cloud.RetryHelper;
import com.google.cloud.Tuple;
import com.google.cloud.bigquery.spi.v2.BigQueryRpc;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/** Implementation for {@link Connection}, the generic BigQuery connection API (not JDBC). */
final class ConnectionImpl implements Connection {

  private ConnectionSettings connectionSettings;
  private BigQueryOptions bigQueryOptions;
  private BigQueryRpc bigQueryRpc;
  private BigQueryRetryConfig retryConfig;
  // false even after interrupting it here! This could be due to the retrial logic, so
  // using it as a workaround.
  private final int MAX_PROCESS_QUERY_THREADS_CNT = 5;
  private ExecutorService queryTaskExecutor =
      Executors.newFixedThreadPool(MAX_PROCESS_QUERY_THREADS_CNT);
  private final Logger logger = Logger.getLogger(this.getClass().getName());

  ConnectionImpl(
      ConnectionSettings connectionSettings,
      BigQueryOptions bigQueryOptions,
      BigQueryRpc bigQueryRpc,
      BigQueryRetryConfig retryConfig) {
    this.connectionSettings = connectionSettings;
    this.bigQueryOptions = bigQueryOptions;
    this.bigQueryRpc = bigQueryRpc;
    this.retryConfig = retryConfig;
  }

  /*
  Cancel method shutdowns the pageFetcher and producerWorker threads gracefully using interrupt

  The pageFetcher threat wont request for any subsequent threads after interrupting and shutdown as soon as any ongoing RPC call returns

  The producerWorker wont populate the buffer with any further records and clear the buffer, put a EoF marker and shutdown
   */
  @Override
  public synchronized Boolean cancel() throws BigQuerySQLException {
    queryTaskExecutor.shutdownNow();
    return queryTaskExecutor.isShutdown();
  }

  @Override
  public BigQueryDryRunResult dryRun(String sql) throws BigQuerySQLException {
    // TODO: run a dummy query using tabaledata.list end point ? What are the query params
    // jobs.query or jobs.getqueuryresults queryrequestinfo.java
    return null;
  }

  @Override
  public BigQueryResultSet executeSelect(String sql) throws BigQuerySQLException {
    // use jobs.query if all the properties of connectionSettings are supported
    if (isFastQuerySupported()) {
      String projectId = bigQueryOptions.getProjectId();
      QueryRequest queryRequest = createQueryRequest(connectionSettings, sql, null, null);
      return queryRpc(projectId, queryRequest);
    }
    // use jobs.insert otherwise
    com.google.api.services.bigquery.model.Job queryJob =
        createQueryJob(sql, connectionSettings, null, null);
    return null; // TODO getQueryResultsRpc(JobId.fromPb(queryJob.getJobReference()));
  }

  @Override
  public BigQueryResultSet executeSelect(
      String sql, List<QueryParameter> parameters, Map<String, String> labels)
      throws BigQuerySQLException {
    // use jobs.query if possible
    if (isFastQuerySupported()) {
      final String projectId = bigQueryOptions.getProjectId();
      final QueryRequest queryRequest =
          createQueryRequest(connectionSettings, sql, parameters, labels);
      return queryRpc(projectId, queryRequest);
    }
    // use jobs.insert otherwise
    com.google.api.services.bigquery.model.Job queryJob =
        createQueryJob(sql, connectionSettings, parameters, labels);
    return null; // TODO getQueryResultsRpc(JobId.fromPb(queryJob.getJobReference()));
  }

  static class EndOfFieldValueList
      extends AbstractList<
          FieldValue> { // A reference of this class is used as a token to inform the thread
    // consuming `buffer` BigQueryResultSetImpl that we have run out of records
    @Override
    public FieldValue get(int index) {
      return null;
    }

    @Override
    public int size() {
      return 0;
    }
  }

  private BigQueryResultSet queryRpc(final String projectId, final QueryRequest queryRequest) {
    com.google.api.services.bigquery.model.QueryResponse results;
    try {
      results =
          BigQueryRetryHelper.runWithRetries(
              () -> bigQueryRpc.queryRpc(projectId, queryRequest),
              bigQueryOptions.getRetrySettings(),
              BigQueryBaseService.BIGQUERY_EXCEPTION_HANDLER,
              bigQueryOptions.getClock(),
              retryConfig);
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      throw BigQueryException.translateAndThrow(e);
    }

    if (results.getErrors() != null) {
      List<BigQueryError> bigQueryErrors =
          results.getErrors().stream()
              .map(BigQueryError.FROM_PB_FUNCTION)
              .collect(Collectors.toList());
      // Throwing BigQueryException since there may be no JobId and we want to stay consistent
      // with the case where there there is a HTTP error
      throw new BigQueryException(bigQueryErrors);
    }

    // Query finished running and we can paginate all the results
    if (results.getJobComplete() && results.getSchema() != null) {
      return processQueryResponseResults(results); // processQueryResponseResults(results);
    } else {
      // Query is long running (> 10s) and hasn't completed yet, or query completed but didn't
      // return the schema, fallback to jobs.insert path. Some operations don't return the schema
      // and
      // can be optimized here, but this is left as future work.
      long totalRows = results.getTotalRows().longValue();
      long pageRows = results.getRows().size();
      JobId jobId = JobId.fromPb(results.getJobReference());
      return getQueryResultsWithJobId(totalRows, pageRows, null, jobId);
    }
  }

  private static Iterable<FieldValueList> getIterableFieldValueList(
      Iterable<TableRow> tableDataPb, final Schema schema) {
    return ImmutableList.copyOf(
        Iterables.transform(
            tableDataPb != null ? tableDataPb : ImmutableList.<TableRow>of(),
            new Function<TableRow, FieldValueList>() {
              FieldList fields = schema != null ? schema.getFields() : null;

              @Override
              public FieldValueList apply(TableRow rowPb) {
                return FieldValueList.fromPb(rowPb.getF(), fields);
              }
            }));
  }

  // Determines the optimal number of caches pages to improve read performance
  private int getPageCacheSize(Long numBufferedRows, long numRows, Schema schema) {
    final int MIN_CACHE_SIZE = 2; // Min number of pages in the page size
    final int MAX_CACHE_SIZE = 20; // //Min number of pages in the page size
    int columnsRead = schema.getFields().size();
    int numCachedPages = 10; // default page size
    long numCachedRows = numBufferedRows == null ? 0 : numBufferedRows.longValue();

    // TODO: Further enhance this logic
    if (numCachedRows > 10000) {
      numCachedPages =
          2; // the size of numBufferedRows is quite large and as per our tests we should be able to
      // do enough even with low
    } else if (columnsRead > 15
        && numCachedRows
            > 5000) { // too many fields are being read, setting the page size on the lower end
      numCachedPages = 3;
    } else if (numCachedRows < 2000
        && columnsRead < 15) { // low pagesize with fewer number of columns, we can cache more pages
      numCachedPages = 20;
    } else { // default - under 10K numCachedRows with any number of columns
      numCachedPages = 5;
    }
    return numCachedPages < MIN_CACHE_SIZE
        ? MIN_CACHE_SIZE
        : (Math.min(
            numCachedPages,
            MAX_CACHE_SIZE)); // numCachedPages should be between the defined min and max
  }

  private BigQueryResultSet processQueryResponseResults(
      com.google.api.services.bigquery.model.QueryResponse results) {
    Schema schema;
    long numRows;
    schema = Schema.fromPb(results.getSchema());
    numRows = results.getTotalRows().longValue();

    // Producer thread for populating the buffer row by row
    // Keeping the buffersize more than the page size and ensuring it's always a reasonable number
    int bufferSize =
        (int)
            (connectionSettings.getNumBufferedRows() == null
                    || connectionSettings.getNumBufferedRows() < 10000
                ? 20000
                : (connectionSettings.getNumBufferedRows() * 2));
    BlockingQueue<AbstractList<FieldValue>> buffer =
        new LinkedBlockingDeque<>(
            bufferSize); // this keeps the deserialized records at the row level, which will be
    // consumed by the BQResultSet
    BlockingQueue<Tuple<Iterable<FieldValueList>, Boolean>> pageCache =
        new LinkedBlockingDeque<>(
            getPageCacheSize(
                connectionSettings.getNumBufferedRows(),
                numRows,
                schema)); // this keeps the parsed FieldValueLists
    BlockingQueue<Tuple<TableDataList, Boolean>> rpcResponseQueue =
        new LinkedBlockingDeque<>(
            getPageCacheSize(
                connectionSettings.getNumBufferedRows(),
                numRows,
                schema)); // this keeps the raw RPC response

    JobId jobId = JobId.fromPb(results.getJobReference());
    final TableId destinationTable = queryJobsGetRpc(jobId);

    // This thread makes the RPC calls and paginates
    Runnable nextPageTask =
        () -> {
          String pageToken = results.getPageToken();
          try {
            while (pageToken != null) { // paginate for non null token
              if (Thread.currentThread().isInterrupted()
                  || queryTaskExecutor.isShutdown()) { // do not process further pages and shutdown
                break;
              }
              TableDataList tabledataList = tableDataListRpc(destinationTable, pageToken);
              pageToken = tabledataList.getPageToken();
              rpcResponseQueue.put(
                  Tuple.of(
                      tabledataList,
                      true)); // this will be parsed asynchronously without blocking the current
              // thread
            }
            rpcResponseQueue.put(
                Tuple.of(
                    null,
                    false)); // this will stop the parseDataTask as well in case of interrupt or
            // when the pagination completes
          } catch (Exception e) {
            throw new BigQueryException(0, e.getMessage(), e);
          }
        };
    queryTaskExecutor.execute(nextPageTask);

    parseRpcDataAsync(
        results,
        schema,
        pageCache,
        rpcResponseQueue); // parses data on a separate thread, thus maximising processing
    // throughput

    populateBufferAsync(
        rpcResponseQueue, pageCache, buffer); // spawns a thread to populate the buffer

    // This will work for pagination as well, as buffer is getting updated asynchronously
    return new BigQueryResultSetImpl<AbstractList<FieldValue>>(schema, numRows, buffer);
  }

  /*
  This method takes TableDataList from rpcResponseQueue and populates pageCache with FieldValueList
   */
  private void parseRpcDataAsync(
      com.google.api.services.bigquery.model.QueryResponse results,
      Schema schema,
      BlockingQueue<Tuple<Iterable<FieldValueList>, Boolean>> pageCache,
      BlockingQueue<Tuple<TableDataList, Boolean>> rpcResponseQueue) {

    // parse and put the first page in the pageCache before the other pages are parsed from the RPC
    // calls
    Iterable<FieldValueList> firstFieldValueLists =
        getIterableFieldValueList(results.getRows(), schema);
    try {
      pageCache.put(
          Tuple.of(firstFieldValueLists, true)); // this is the first page which we have received.
    } catch (InterruptedException e) {
      throw new BigQueryException(0, e.getMessage(), e);
    }

    // rpcResponseQueue will get null tuple if Cancel method is called, so no need to explicitly use
    // thread interrupt here
    Runnable parseDataTask =
        () -> {
          try {
            boolean hasMorePages = true;
            while (hasMorePages) {
              Tuple<TableDataList, Boolean> rpcResponse = rpcResponseQueue.take();
              TableDataList tabledataList = rpcResponse.x();
              hasMorePages = rpcResponse.y();
              if (tabledataList != null) {
                Iterable<FieldValueList> fieldValueLists =
                    getIterableFieldValueList(tabledataList.getRows(), schema); // Parse
                pageCache.put(Tuple.of(fieldValueLists, true));
              }
            }
          } catch (InterruptedException e) {
            logger.log(
                Level.WARNING,
                "\n" + Thread.currentThread().getName() + " Interrupted",
                e); // Thread might get interrupted while calling the Cancel method, which is
            // expected, so logging this instead of throwing the exception back
          }
          try {
            pageCache.put(Tuple.of(null, false)); // no further pages
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        };
    queryTaskExecutor.execute(parseDataTask);
  }

  private void populateBufferAsync(
      BlockingQueue<Tuple<TableDataList, Boolean>> rpcResponseQueue,
      BlockingQueue<Tuple<Iterable<FieldValueList>, Boolean>> pageCache,
      BlockingQueue<AbstractList<FieldValue>> buffer) {
    Runnable populateBufferRunnable =
        () -> { // producer thread populating the buffer
          Iterable<FieldValueList> fieldValueLists = null;
          boolean hasRows = true; // as we have to process the first page
          while (hasRows) {
            try {
              Tuple<Iterable<FieldValueList>, Boolean> nextPageTuple = pageCache.take();
              hasRows = nextPageTuple.y();
              fieldValueLists = nextPageTuple.x();
            } catch (InterruptedException e) {
              logger.log(
                  Level.WARNING,
                  "\n" + Thread.currentThread().getName() + " Interrupted",
                  e); // Thread might get interrupted while calling the Cancel method, which is
              // expected, so logging this instead of throwing the exception back
            }

            if (Thread.currentThread().isInterrupted()
                || fieldValueLists
                    == null) { // do not process further pages and shutdown (outerloop)
              break;
            }

            for (FieldValueList fieldValueList : fieldValueLists) {
              try {
                if (Thread.currentThread()
                    .isInterrupted()) { // do not process further pages and shutdown (inner loop)
                  break;
                }
                buffer.put(fieldValueList);
              } catch (InterruptedException e) {
                throw new BigQueryException(0, e.getMessage(), e);
              }
            }
          }

          if (Thread.currentThread()
              .isInterrupted()) { // clear the buffer for any outstanding records
            buffer.clear();
            rpcResponseQueue
                .clear(); // IMP - so that if it's full then it unblocks and the interrupt logic
            // could trigger
          }

          try {
            buffer.put(
                new EndOfFieldValueList()); // All the pages has been processed, put this marker
          } catch (InterruptedException e) {
            throw new BigQueryException(0, e.getMessage(), e);
          }
          queryTaskExecutor.shutdownNow(); // Shutdown the thread pool
        };

    queryTaskExecutor.execute(populateBufferRunnable);
  }

  /* Returns query results using either tabledata.list or the high throughput Read API */
  private BigQueryResultSet getQueryResultsWithJobId(
      long totalRows, long pageRows, Schema schema, JobId jobId) {
    TableId destinationTable = queryJobsGetRpc(jobId);

    return null; // use processQueryResponseResults(); for remaining pages
    /*  return useReadAPI(totalRows, pageRows)
    ? highThroughPutRead(destinationTable)
    : null; // plugin tableDataListRpc(destinationTable, schema, null);, Use processQueryResponseResults ?
                */
  }

  /* Returns the destinationTable from jobId by calling jobs.get API */
  private TableId queryJobsGetRpc(JobId jobId) {
    final JobId completeJobId =
        jobId
            .setProjectId(bigQueryOptions.getProjectId())
            .setLocation(
                jobId.getLocation() == null && bigQueryOptions.getLocation() != null
                    ? bigQueryOptions.getLocation()
                    : jobId.getLocation());
    com.google.api.services.bigquery.model.Job jobPb;
    try {
      jobPb =
          runWithRetries(
              () ->
                  bigQueryRpc.getQueryJob(
                      completeJobId.getProject(),
                      completeJobId.getJob(),
                      completeJobId.getLocation()),
              bigQueryOptions.getRetrySettings(),
              BigQueryBaseService.BIGQUERY_EXCEPTION_HANDLER,
              bigQueryOptions.getClock());
      if (bigQueryOptions.getThrowNotFound() && jobPb == null) {
        throw new BigQueryException(HTTP_NOT_FOUND, "Query job not found");
      }
    } catch (RetryHelper.RetryHelperException e) {
      throw BigQueryException.translateAndThrow(e);
    }
    Job job = Job.fromPb(bigQueryOptions.getService(), jobPb);
    return ((QueryJobConfiguration) job.getConfiguration()).getDestinationTable();
  }

  private TableDataList tableDataListRpc(TableId destinationTable, String pageToken) {
    try {
      final TableId completeTableId =
          destinationTable.setProjectId(
              Strings.isNullOrEmpty(destinationTable.getProject())
                  ? bigQueryOptions.getProjectId()
                  : destinationTable.getProject());
      TableDataList results =
          runWithRetries(
              () ->
                  bigQueryOptions
                      .getBigQueryRpcV2()
                      .listTableDataWithRowLimit(
                          completeTableId.getProject(),
                          completeTableId.getDataset(),
                          completeTableId.getTable(),
                          connectionSettings.getNumBufferedRows(),
                          pageToken),
              bigQueryOptions.getRetrySettings(),
              BigQueryBaseService.BIGQUERY_EXCEPTION_HANDLER,
              bigQueryOptions.getClock());

      return results;
    } catch (RetryHelper.RetryHelperException e) {
      throw BigQueryException.translateAndThrow(e);
    }
  }

  private BigQueryResultSet highThroughPutRead(TableId destinationTable) {
    return null;
  }

  /* Returns results of the query associated with the provided job using jobs.getQueryResults API */
  private BigQueryResultSet getQueryResultsRpc(
      JobId jobId) { // TODO(prasmish) temp: This is a slower endpoint
    JobId completeJobId =
        jobId
            .setProjectId(bigQueryOptions.getProjectId())
            .setLocation(
                jobId.getLocation() == null && bigQueryOptions.getLocation() != null
                    ? bigQueryOptions.getLocation()
                    : jobId.getLocation());
    try {
      GetQueryResultsResponse results =
          BigQueryRetryHelper.runWithRetries(
              () ->
                  bigQueryRpc.getQueryResultsWithRowLimit(
                      completeJobId.getProject(),
                      completeJobId.getJob(),
                      completeJobId.getLocation(),
                      connectionSettings.getNumBufferedRows()),
              bigQueryOptions.getRetrySettings(),
              BigQueryBaseService.BIGQUERY_EXCEPTION_HANDLER,
              bigQueryOptions.getClock(),
              retryConfig);

      if (results.getErrors() != null) {
        List<BigQueryError> bigQueryErrors =
            results.getErrors().stream()
                .map(BigQueryError.FROM_PB_FUNCTION)
                .collect(Collectors.toList());
        // Throwing BigQueryException since there may be no JobId and we want to stay consistent
        // with the case where there there is a HTTP error
        throw new BigQueryException(bigQueryErrors);
      }
      return processGetQueryResults(jobId, results);
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      throw BigQueryException.translateAndThrow(e);
    }
  }

  private BigQueryResultSet processGetQueryResults(JobId jobId, GetQueryResultsResponse results) {
    long numRows = results.getTotalRows() == null ? 0 : results.getTotalRows().longValue();
    Schema schema = results.getSchema() == null ? null : Schema.fromPb(results.getSchema());

    // only use this API for the first page of result
    if (results.getPageToken() == null) {
      // return new BigQueryResultSetImpl(schema, numRows, null /* TODO: iterate(cachedFirstPage)
      // */);
      return null; // TODO: Plugin the buffer logic
    }
    // use tabledata.list or Read API to fetch subsequent pages of results
    long totalRows = results.getTotalRows() == null ? 0 : results.getTotalRows().longValue();
    long pageRows = results.getRows().size();
    return getQueryResultsWithJobId(totalRows, pageRows, schema, jobId);
  }

  private boolean isFastQuerySupported() {
    // TODO: add regex logic to check for scripting
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

  private boolean useReadAPI(Long totalRows, Long pageRows) {
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
          connectionSettings.getConnectionProperties().stream()
              .map(ConnectionProperty.TO_PB_FUNCTION)
              .collect(Collectors.toList()));
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
    // The new Connection interface only supports StandardSQL dialect
    content.setUseLegacySql(false);
    return content;
  }

  private com.google.api.services.bigquery.model.Job createQueryJob(
      String sql,
      ConnectionSettings connectionSettings,
      List<QueryParameter> queryParameters,
      Map<String, String> labels) {
    com.google.api.services.bigquery.model.JobConfiguration configurationPb =
        new com.google.api.services.bigquery.model.JobConfiguration();
    JobConfigurationQuery queryConfigurationPb = new JobConfigurationQuery();
    queryConfigurationPb.setQuery(sql);
    if (queryParameters != null) {
      queryConfigurationPb.setQueryParameters(queryParameters);
      if (queryParameters.get(0).getName() == null) {
        queryConfigurationPb.setParameterMode("POSITIONAL");
      } else {
        queryConfigurationPb.setParameterMode("NAMED");
      }
    }
    if (connectionSettings.getDestinationTable() != null) {
      queryConfigurationPb.setDestinationTable(connectionSettings.getDestinationTable().toPb());
    }
    if (connectionSettings.getTableDefinitions() != null) {
      queryConfigurationPb.setTableDefinitions(
          Maps.transformValues(
              connectionSettings.getTableDefinitions(),
              ExternalTableDefinition.TO_EXTERNAL_DATA_FUNCTION));
    }
    if (connectionSettings.getUserDefinedFunctions() != null) {
      queryConfigurationPb.setUserDefinedFunctionResources(
          connectionSettings.getUserDefinedFunctions().stream()
              .map(UserDefinedFunction.TO_PB_FUNCTION)
              .collect(Collectors.toList()));
    }
    if (connectionSettings.getCreateDisposition() != null) {
      queryConfigurationPb.setCreateDisposition(
          connectionSettings.getCreateDisposition().toString());
    }
    if (connectionSettings.getWriteDisposition() != null) {
      queryConfigurationPb.setWriteDisposition(connectionSettings.getWriteDisposition().toString());
    }
    if (connectionSettings.getDefaultDataset() != null) {
      queryConfigurationPb.setDefaultDataset(connectionSettings.getDefaultDataset().toPb());
    }
    if (connectionSettings.getPriority() != null) {
      queryConfigurationPb.setPriority(connectionSettings.getPriority().toString());
    }
    if (connectionSettings.getAllowLargeResults() != null) {
      queryConfigurationPb.setAllowLargeResults(connectionSettings.getAllowLargeResults());
    }
    if (connectionSettings.getUseQueryCache() != null) {
      queryConfigurationPb.setUseQueryCache(connectionSettings.getUseQueryCache());
    }
    if (connectionSettings.getFlattenResults() != null) {
      queryConfigurationPb.setFlattenResults(connectionSettings.getFlattenResults());
    }
    if (connectionSettings.getMaximumBillingTier() != null) {
      queryConfigurationPb.setMaximumBillingTier(connectionSettings.getMaximumBillingTier());
    }
    if (connectionSettings.getMaximumBytesBilled() != null) {
      queryConfigurationPb.setMaximumBytesBilled(connectionSettings.getMaximumBytesBilled());
    }
    if (connectionSettings.getSchemaUpdateOptions() != null) {
      ImmutableList.Builder<String> schemaUpdateOptionsBuilder = new ImmutableList.Builder<>();
      for (JobInfo.SchemaUpdateOption schemaUpdateOption :
          connectionSettings.getSchemaUpdateOptions()) {
        schemaUpdateOptionsBuilder.add(schemaUpdateOption.name());
      }
      queryConfigurationPb.setSchemaUpdateOptions(schemaUpdateOptionsBuilder.build());
    }
    if (connectionSettings.getDestinationEncryptionConfiguration() != null) {
      queryConfigurationPb.setDestinationEncryptionConfiguration(
          connectionSettings.getDestinationEncryptionConfiguration().toPb());
    }
    if (connectionSettings.getTimePartitioning() != null) {
      queryConfigurationPb.setTimePartitioning(connectionSettings.getTimePartitioning().toPb());
    }
    if (connectionSettings.getClustering() != null) {
      queryConfigurationPb.setClustering(connectionSettings.getClustering().toPb());
    }
    if (connectionSettings.getRangePartitioning() != null) {
      queryConfigurationPb.setRangePartitioning(connectionSettings.getRangePartitioning().toPb());
    }
    if (connectionSettings.getConnectionProperties() != null) {
      queryConfigurationPb.setConnectionProperties(
          connectionSettings.getConnectionProperties().stream()
              .map(ConnectionProperty.TO_PB_FUNCTION)
              .collect(Collectors.toList()));
    }
    if (connectionSettings.getJobTimeoutMs() != null) {
      configurationPb.setJobTimeoutMs(connectionSettings.getJobTimeoutMs());
    }
    if (labels != null) {
      configurationPb.setLabels(labels);
    }
    // The new Connection interface only supports StandardSQL dialect
    queryConfigurationPb.setUseLegacySql(false);
    configurationPb.setQuery(queryConfigurationPb);

    com.google.api.services.bigquery.model.Job jobPb =
        JobInfo.of(QueryJobConfiguration.fromPb(configurationPb)).toPb();
    com.google.api.services.bigquery.model.Job queryJob;
    try {
      queryJob =
          BigQueryRetryHelper.runWithRetries(
              () -> bigQueryRpc.createJobForQuery(jobPb),
              bigQueryOptions.getRetrySettings(),
              BigQueryBaseService.BIGQUERY_EXCEPTION_HANDLER,
              bigQueryOptions.getClock(),
              retryConfig);
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      throw BigQueryException.translateAndThrow(e);
    }
    return queryJob;
  }
}
