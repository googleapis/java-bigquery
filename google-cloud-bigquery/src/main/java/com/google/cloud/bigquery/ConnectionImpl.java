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

import com.google.api.core.InternalApi;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.QueryParameter;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.RetryHelper;
import com.google.cloud.Tuple;
import com.google.cloud.bigquery.spi.v2.BigQueryRpc;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

/** Implementation for {@link Connection}, the generic BigQuery connection API (not JDBC). */
class ConnectionImpl implements Connection {

  private final ConnectionSettings connectionSettings;
  private final BigQueryOptions bigQueryOptions;
  private final BigQueryRpc bigQueryRpc;
  private final BigQueryRetryConfig retryConfig;
  private final int bufferSize; // buffer size in Producer Thread
  private final int MAX_PROCESS_QUERY_THREADS_CNT = 5;
  private final ExecutorService queryTaskExecutor =
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
    this.bufferSize =
        (int)
            (connectionSettings.getNumBufferedRows() == null
                    || connectionSettings.getNumBufferedRows() < 10000
                ? 20000
                : Math.min(connectionSettings.getNumBufferedRows() * 2, 100000));
  }

  /**
   * Cancel method shutdowns the pageFetcher and producerWorker threads gracefully using interrupt.
   * The pageFetcher threat will not request for any subsequent threads after interrupting and
   * shutdown as soon as any ongoing RPC call returns. The producerWorker will not populate the
   * buffer with any further records and clear the buffer, put a EoF marker and shutdown.
   *
   * @return Boolean value true if the threads were interrupted
   * @throws BigQuerySQLException
   */
  @Override
  public synchronized Boolean cancel() throws BigQuerySQLException {
    queryTaskExecutor.shutdownNow();
    return queryTaskExecutor.isShutdown();
  }

  /**
   * This method runs a dry run query
   *
   * @param sql SQL SELECT statement
   * @return BigQueryDryRunResult containing List<QueryParameter> and Schema
   * @throws BigQuerySQLException
   */
  @Override
  public BigQueryDryRunResult dryRun(String sql) throws BigQuerySQLException {
    com.google.api.services.bigquery.model.Job dryRunJob = createDryRunJob(sql);
    Schema schema = Schema.fromPb(dryRunJob.getStatistics().getQuery().getSchema());
    List<QueryParameter> queryParameters =
        dryRunJob.getStatistics().getQuery().getUndeclaredQueryParameters();
    return new BigQueryDryRunResultImpl(schema, queryParameters);
  }

  /**
   * This method executes a SQL SELECT query
   *
   * @param sql SQL SELECT statement
   * @return BigQueryResultSet containing the output of the query
   * @throws BigQuerySQLException
   */
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
    JobId jobId = JobId.fromPb(queryJob.getJobReference());
    GetQueryResultsResponse firstPage = getQueryResultsFirstPage(jobId);
    return getSubsequentQueryResultsWithJob(
        firstPage.getTotalRows().longValue(), (long) firstPage.getRows().size(), jobId, firstPage);
  }

  /**
   * This method executes a SQL SELECT query
   *
   * @param sql SQL SELECT query
   * @param parameters named or positional parameters. The set of query parameters must either be
   *     all positional or all named parameters.
   * @param labels the labels associated with this query. You can use these to organize and group
   *     your query jobs. Label keys and values can be no longer than 63 characters, can only
   *     contain lowercase letters, numeric characters, underscores and dashes. International
   *     characters are allowed. Label values are optional. Label keys must start with a letter and
   *     each label in the list must have a different key.
   * @return BigQueryResultSet containing the output of the query
   * @throws BigQuerySQLException
   */
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
    JobId jobId = JobId.fromPb(queryJob.getJobReference());
    GetQueryResultsResponse firstPage = getQueryResultsFirstPage(jobId);
    return getSubsequentQueryResultsWithJob(
        firstPage.getTotalRows().longValue(), (long) firstPage.getRows().size(), jobId, firstPage);
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
      // Throwing BigQueryException since there may be no JobId, and we want to stay consistent
      // with the case where there is an HTTP error
      throw new BigQueryException(bigQueryErrors);
    }

    // Query finished running and we can paginate all the results
    if (results.getJobComplete() && results.getSchema() != null) {
      return processQueryResponseResults(results);
    } else {
      // Query is long-running (> 10s) and hasn't completed yet, or query completed but didn't
      // return the schema, fallback to jobs.insert path. Some operations don't return the schema
      // and can be optimized here, but this is left as future work.
      long totalRows = results.getTotalRows().longValue();
      long pageRows = results.getRows().size();
      JobId jobId = JobId.fromPb(results.getJobReference());
      GetQueryResultsResponse firstPage = getQueryResultsFirstPage(jobId);
      return getSubsequentQueryResultsWithJob(totalRows, pageRows, jobId, firstPage);
    }
  }

  private BigQueryResultSetStats getBigQueryResultSetStats(JobId jobId) {
    // Create GetQueryResultsResponse query statistics
    Job queryJob = getQueryJobRpc(jobId);
    JobStatistics.QueryStatistics statistics = queryJob.getStatistics();
    DmlStats dmlStats = statistics.getDmlStats() == null ? null : statistics.getDmlStats();
    JobStatistics.SessionInfo sessionInfo =
        statistics.getSessionInfo() == null ? null : statistics.getSessionInfo();
    return new BigQueryResultSetStatsImpl(dmlStats, sessionInfo);
  }
  /* This method processed the first page of GetQueryResultsResponse and then it uses tabledata.list */
  @InternalApi("Exposed for testing")
  public BigQueryResultSet tableDataList(GetQueryResultsResponse firstPage, JobId jobId) {
    Schema schema;
    long numRows;
    schema = Schema.fromPb(firstPage.getSchema());
    numRows = firstPage.getTotalRows().longValue();

    BigQueryResultSetStats bigQueryResultSetStats = getBigQueryResultSetStats(jobId);

    // Keeps the deserialized records at the row level, which is consumed by BigQueryResultSet
    BlockingQueue<AbstractList<FieldValue>> buffer = new LinkedBlockingDeque<>(bufferSize);

    // Keeps the parsed FieldValueLists
    BlockingQueue<Tuple<Iterable<FieldValueList>, Boolean>> pageCache =
        new LinkedBlockingDeque<>(
            getPageCacheSize(connectionSettings.getNumBufferedRows(), numRows, schema));

    // Keeps the raw RPC responses
    BlockingQueue<Tuple<TableDataList, Boolean>> rpcResponseQueue =
        new LinkedBlockingDeque<>(
            getPageCacheSize(connectionSettings.getNumBufferedRows(), numRows, schema));

    runNextPageTaskAsync(firstPage.getPageToken(), getDestinationTable(jobId), rpcResponseQueue);

    parseRpcDataAsync(
        firstPage.getRows(),
        schema,
        pageCache,
        rpcResponseQueue); // parses data on a separate thread, thus maximising processing
    // throughput

    populateBufferAsync(
        rpcResponseQueue, pageCache, buffer); // spawns a thread to populate the buffer

    // This will work for pagination as well, as buffer is getting updated asynchronously
    return new BigQueryResultSetImpl<AbstractList<FieldValue>>(
        schema, numRows, buffer, bigQueryResultSetStats);
  }

  @InternalApi("Exposed for testing")
  public BigQueryResultSet processQueryResponseResults(
      com.google.api.services.bigquery.model.QueryResponse results) {
    Schema schema;
    long numRows;
    schema = Schema.fromPb(results.getSchema());
    numRows = results.getTotalRows().longValue();
    // Create QueryResponse query statistics
    DmlStats dmlStats =
        results.getDmlStats() == null ? null : DmlStats.fromPb(results.getDmlStats());
    JobStatistics.SessionInfo sessionInfo =
        results.getSessionInfo() == null
            ? null
            : JobStatistics.SessionInfo.fromPb(results.getSessionInfo());
    BigQueryResultSetStats bigQueryResultSetStats =
        new BigQueryResultSetStatsImpl(dmlStats, sessionInfo);

    BlockingQueue<AbstractList<FieldValue>> buffer = new LinkedBlockingDeque<>(bufferSize);
    BlockingQueue<Tuple<Iterable<FieldValueList>, Boolean>> pageCache =
        new LinkedBlockingDeque<>(
            getPageCacheSize(connectionSettings.getNumBufferedRows(), numRows, schema));
    BlockingQueue<Tuple<TableDataList, Boolean>> rpcResponseQueue =
        new LinkedBlockingDeque<>(
            getPageCacheSize(connectionSettings.getNumBufferedRows(), numRows, schema));

    JobId jobId = JobId.fromPb(results.getJobReference());

    runNextPageTaskAsync(results.getPageToken(), getDestinationTable(jobId), rpcResponseQueue);

    parseRpcDataAsync(results.getRows(), schema, pageCache, rpcResponseQueue);

    populateBufferAsync(rpcResponseQueue, pageCache, buffer);

    return new BigQueryResultSetImpl<AbstractList<FieldValue>>(
        schema, numRows, buffer, bigQueryResultSetStats);
  }

  @InternalApi("Exposed for testing")
  public void runNextPageTaskAsync(
      String firstPageToken,
      TableId destinationTable,
      BlockingQueue<Tuple<TableDataList, Boolean>> rpcResponseQueue) {
    // This thread makes the RPC calls and paginates
    Runnable nextPageTask =
        () -> {
          String pageToken = firstPageToken; // results.getPageToken();
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
  }

  /*
  This method takes TableDataList from rpcResponseQueue and populates pageCache with FieldValueList
   */
  @InternalApi("Exposed for testing")
  public void parseRpcDataAsync(
      // com.google.api.services.bigquery.model.QueryResponse results,
      List<TableRow> tableRows,
      Schema schema,
      BlockingQueue<Tuple<Iterable<FieldValueList>, Boolean>> pageCache,
      BlockingQueue<Tuple<TableDataList, Boolean>> rpcResponseQueue) {

    // parse and put the first page in the pageCache before the other pages are parsed from the RPC
    // calls
    Iterable<FieldValueList> firstFieldValueLists = getIterableFieldValueList(tableRows, schema);
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
            logger.log(
                Level.WARNING,
                "\n" + Thread.currentThread().getName() + " Interrupted",
                e); // Thread might get interrupted while calling the Cancel method, which is
            // expected, so logging this instead of throwing the exception back
          }
        };
    queryTaskExecutor.execute(parseDataTask);
  }

  @InternalApi("Exposed for testing")
  public void populateBufferAsync(
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

  /* Helper method that parse and populate a page with TableRows */
  private static Iterable<FieldValueList> getIterableFieldValueList(
      Iterable<TableRow> tableDataPb, final Schema schema) {
    return ImmutableList.copyOf(
        Iterables.transform(
            tableDataPb != null ? tableDataPb : ImmutableList.of(),
            new Function<TableRow, FieldValueList>() {
              final FieldList fields = schema != null ? schema.getFields() : null;

              @Override
              public FieldValueList apply(TableRow rowPb) {
                return FieldValueList.fromPb(rowPb.getF(), fields);
              }
            }));
  }

  /* Helper method that determines the optimal number of caches pages to improve read performance */
  private int getPageCacheSize(Long numBufferedRows, long numRows, Schema schema) {
    final int MIN_CACHE_SIZE = 3; // Min number of pages in the page size
    final int MAX_CACHE_SIZE = 20; // //Min number of pages in the page size
    int columnsRead = schema.getFields().size();
    int numCachedPages;
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

  /* Returns query results using either tabledata.list or the high throughput Read API */
  @InternalApi("Exposed for testing")
  public BigQueryResultSet getSubsequentQueryResultsWithJob(
      Long totalRows, Long pageRows, JobId jobId, GetQueryResultsResponse firstPage) {
    TableId destinationTable = getDestinationTable(jobId);
    return useReadAPI(totalRows, pageRows)
        ? highThroughPutRead(
            destinationTable,
            firstPage.getTotalRows().longValue(),
            firstPage.getSchema(),
            getBigQueryResultSetStats(
                jobId)) // discord first page and stream the entire BigQueryResultSet using
        // the Read API
        : tableDataList(firstPage, jobId);
  }

  /* Returns Job from jobId by calling the jobs.get API */
  private Job getQueryJobRpc(JobId jobId) {
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
    return Job.fromPb(bigQueryOptions.getService(), jobPb);
  }

  /* Returns the destinationTable from jobId by calling jobs.get API */
  @InternalApi("Exposed for testing")
  public TableId getDestinationTable(JobId jobId) {
    Job job = getQueryJobRpc(jobId);
    return ((QueryJobConfiguration) job.getConfiguration()).getDestinationTable();
  }

  @InternalApi("Exposed for testing")
  public TableDataList tableDataListRpc(TableId destinationTable, String pageToken) {
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

  private BigQueryReadClient
      bqReadClient; // TODO - Check if this instance be reused, currently it's being initialized on
  // every call
  // query result should be saved in the destination table
  private BigQueryResultSet highThroughPutRead(
      TableId destinationTable,
      long totalRows,
      TableSchema tableSchema,
      BigQueryResultSetStats stats) {

    try {
      bqReadClient = BigQueryReadClient.create();
      String parent = String.format("projects/%s", destinationTable.getProject());
      String srcTable =
          String.format(
              "projects/%s/datasets/%s/tables/%s",
              destinationTable.getProject(),
              destinationTable.getDataset(),
              destinationTable.getProject());

      /*  ReadSession.TableReadOptions options =
      ReadSession.TableReadOptions.newBuilder().
                                  .build();*/

      // Read all the columns if the source table and stream the data back in Arrow format
      ReadSession.Builder sessionBuilder =
          ReadSession.newBuilder().setTable(srcTable).setDataFormat(DataFormat.ARROW)
          // .setReadOptions(options)//TODO: Check if entire table be read if we are not specifying
          // the options
          ;

      CreateReadSessionRequest.Builder builder =
          CreateReadSessionRequest.newBuilder()
              .setParent(parent)
              .setReadSession(sessionBuilder)
              .setMaxStreamCount(1) // Currently just one stream is allowed
          // DO a regex check using order by and use multiple streams
          ;

      ReadSession readSession = bqReadClient.createReadSession(builder.build());

      // TODO(prasmish) Modify the type in the Tuple, dynamically determine the capacity
      BlockingQueue<Tuple<Map<String, Object>, Boolean>> buffer = new LinkedBlockingDeque<>(500);

      Map<String, Integer> arrowNameToIndex = new HashMap<>();
      Schema schema = Schema.fromPb(tableSchema);
      // deserialize and populate the buffer async, so that the client isn't blocked
      processArrowStreamAsync(
          readSession,
          buffer,
          new ArrowRowReader(readSession.getArrowSchema(), arrowNameToIndex),
          schema);

      return new BigQueryResultSetImpl<Tuple<Map<String, Object>, Boolean>>(
          schema, totalRows, buffer, stats);

    } catch (IOException e) {
      // Throw Error
      e.printStackTrace();
      throw new RuntimeException(e.getMessage()); // TODO exception handling
    }
  }

  private void processArrowStreamAsync(
      ReadSession readSession,
      BlockingQueue<Tuple<Map<String, Object>, Boolean>> buffer,
      ArrowRowReader reader,
      Schema schema) {

    Runnable arrowStreamProcessor =
        () -> {
          try // (ArrowRowReader reader = new ArrowRowReader(readSession.getArrowSchema()))
          {
            // Use the first stream to perform reading.
            String streamName = readSession.getStreams(0).getName();

            ReadRowsRequest readRowsRequest =
                ReadRowsRequest.newBuilder().setReadStream(streamName).build();

            // Process each block of rows as they arrive and decode using our simple row reader.
            com.google.api.gax.rpc.ServerStream<ReadRowsResponse> stream =
                bqReadClient.readRowsCallable().call(readRowsRequest);
            for (ReadRowsResponse response : stream) {
              reader.processRows(response.getArrowRecordBatch(), buffer, schema);
            }
            buffer.put(Tuple.of(null, false)); // marking end of stream
          } catch (Exception e) {
            throw BigQueryException.translateAndThrow(e);
          }
        };

    queryTaskExecutor.execute(arrowStreamProcessor);
  }

  private class ArrowRowReader
      implements AutoCloseable { // TODO: Update to recent version of Arrow to avoid memoryleak

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    // Decoder object will be reused to avoid re-allocation and too much garbage collection.
    private final VectorSchemaRoot root;
    private final VectorLoader loader;

    public ArrowRowReader(ArrowSchema arrowSchema, Map<String, Integer> arrowNameToIndex)
        throws IOException {
      org.apache.arrow.vector.types.pojo.Schema schema =
          MessageSerializer.deserializeSchema(
              new org.apache.arrow.vector.ipc.ReadChannel(
                  new ByteArrayReadableSeekableByteChannel(
                      arrowSchema.getSerializedSchema().toByteArray())));
      List<FieldVector> vectors = new ArrayList<>();
      List<Field> fields = schema.getFields();
      for (int i = 0; i < fields.size(); i++) {
        vectors.add(fields.get(i).createVector(allocator));
        arrowNameToIndex.put(
            fields.get(i).getName(),
            i); // mapping for getting against the field name in the result set
      }
      root = new VectorSchemaRoot(vectors);
      loader = new VectorLoader(root);
    }

    /** @param batch object returned from the ReadRowsResponse. */
    public void processRows(
        ArrowRecordBatch batch,
        BlockingQueue<Tuple<Map<String, Object>, Boolean>> buffer,
        Schema schema)
        throws IOException { // deserialize the values and consume the hash of the values
      try {
        org.apache.arrow.vector.ipc.message.ArrowRecordBatch deserializedBatch =
            MessageSerializer.deserializeRecordBatch(
                new ReadChannel(
                    new ByteArrayReadableSeekableByteChannel(
                        batch.getSerializedRecordBatch().toByteArray())),
                allocator);

        loader.load(deserializedBatch);
        // Release buffers from batch (they are still held in the vectors in root).
        deserializedBatch.close();

        // Parse the vectors using BQ Schema. Deserialize the data at the row level and add it to
        // the
        // buffer
        FieldList fields = schema.getFields();
        for (int rowNum = 0;
            rowNum < root.getRowCount();
            rowNum++) { // for the given number of rows in the batch

          Map<String, Object> curRow = new HashMap<>();
          for (int col = 0; col < fields.size(); col++) { // iterate all the vectors for a given row
            com.google.cloud.bigquery.Field field = fields.get(col);
            FieldVector curFieldVec =
                root.getVector(
                    field.getName()); // can be accessed using the index or Vector/column name
            // Now cast the FieldVector depending on the type
            if (field.getType() == LegacySQLTypeName.STRING) {
              VarCharVector varCharVector = (VarCharVector) curFieldVec;
              curRow.put(
                  field.getName(),
                  new String(varCharVector.get(rowNum))); // store the row:value mapping
            } else if (field.getType() == LegacySQLTypeName.TIMESTAMP) {
              TimeStampMicroVector timeStampMicroVector = (TimeStampMicroVector) curFieldVec;
              curRow.put(
                  field.getName(), timeStampMicroVector.get(rowNum)); // store the row:value mapping
            } else if (field.getType() == LegacySQLTypeName.INTEGER) {
              BigIntVector bigIntVector = (BigIntVector) curFieldVec;
              curRow.put(
                  field.getName(), (int) bigIntVector.get(rowNum)); // store the row:value mapping
            } else {
              throw new RuntimeException("TODO: Implement remaining support type conversions");
            }
          }
          buffer.put(Tuple.of(curRow, true));
        }
        root.clear(); // TODO: make sure to clear the root while implementing the thread
        // interruption logic (Connection.close method)

      } catch (InterruptedException e) {
        throw BigQueryException.translateAndThrow(e);
      } finally {
        try {
          root.clear();
        } catch (RuntimeException e) {
          logger.log(Level.WARNING, "\n Error while clearing VectorSchemaRoot ", e);
        }
      }
    }

    @Override
    public void close() {
      root.close();
      allocator.close();
    }
  }
  /*Returns just the first page of GetQueryResultsResponse using the jobId*/
  @InternalApi("Exposed for testing")
  public GetQueryResultsResponse getQueryResultsFirstPage(JobId jobId) {
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
      return results;
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      throw BigQueryException.translateAndThrow(e);
    }
  }

  @InternalApi("Exposed for testing")
  public boolean isFastQuerySupported() {
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
    if (connectionSettings.getReadClientConnectionConfiguration()
        != null) { // Adding a null check to avoid NPE
      return resultRatio
              > connectionSettings
                  .getReadClientConnectionConfiguration()
                  .getTotalToPageRowCountRatio()
          && totalRows
              > connectionSettings.getReadClientConnectionConfiguration().getMinResultSize();
    } else {
      return false;
    }
  }

  // Used for job.query API endpoint
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
    if (connectionSettings.getCreateSession() != null) {
      content.setCreateSession(connectionSettings.getCreateSession());
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

  // Used by jobs.getQueryResults API endpoint
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
    if (connectionSettings.getCreateSession() != null) {
      queryConfigurationPb.setCreateSession(connectionSettings.getCreateSession());
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

  // Used by dryRun
  private com.google.api.services.bigquery.model.Job createDryRunJob(String sql) {
    com.google.api.services.bigquery.model.JobConfiguration configurationPb =
        new com.google.api.services.bigquery.model.JobConfiguration();
    configurationPb.setDryRun(true);
    JobConfigurationQuery queryConfigurationPb = new JobConfigurationQuery();
    String parameterMode = sql.contains("?") ? "POSITIONAL" : "NAMED";
    queryConfigurationPb.setParameterMode(parameterMode);
    queryConfigurationPb.setQuery(sql);
    // UndeclaredQueryParameter is only supported in StandardSQL
    queryConfigurationPb.setUseLegacySql(false);
    if (connectionSettings.getDefaultDataset() != null) {
      queryConfigurationPb.setDefaultDataset(connectionSettings.getDefaultDataset().toPb());
    }
    configurationPb.setQuery(queryConfigurationPb);

    com.google.api.services.bigquery.model.Job jobPb =
        JobInfo.of(QueryJobConfiguration.fromPb(configurationPb)).toPb();

    com.google.api.services.bigquery.model.Job dryRunJob;
    try {
      dryRunJob =
          BigQueryRetryHelper.runWithRetries(
              () -> bigQueryRpc.createJobForQuery(jobPb),
              bigQueryOptions.getRetrySettings(),
              BigQueryBaseService.BIGQUERY_EXCEPTION_HANDLER,
              bigQueryOptions.getClock(),
              retryConfig);
    } catch (BigQueryRetryHelper.BigQueryRetryHelperException e) {
      throw BigQueryException.translateAndThrow(e);
    }
    return dryRunJob;
  }
}
