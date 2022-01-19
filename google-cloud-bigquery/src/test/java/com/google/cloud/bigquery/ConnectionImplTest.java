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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.spi.BigQueryRpcFactory;
import com.google.cloud.bigquery.spi.v2.BigQueryRpc;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConnectionImplTest {
  private BigQueryOptions options;
  private BigQueryRpcFactory rpcFactoryMock;
  private BigQueryRpc bigqueryRpcMock;
  private Connection connectionMock;
  private BigQuery bigquery;
  private Connection connection;
  private static final String PROJECT = "project";
  private static final String DEFAULT_TEST_DATASET = "bigquery_test_dataset";
  private static final String PAGE_TOKEN = "ABCD123";
  private static final String FAST_SQL =
      "SELECT  county, state_name FROM bigquery_test_dataset.large_data_testing_table limit 2";
  private static final long DEFAULT_PAGE_SIZE = 10000L;
  private ConnectionSettings connectionSettings;
  private static final Schema FAST_QUERY_SCHEMA =
      Schema.of(
          Field.newBuilder("country", StandardSQLTypeName.STRING)
              .setMode(Field.Mode.NULLABLE)
              .build(),
          Field.newBuilder("state_name", StandardSQLTypeName.BIGNUMERIC)
              .setMode(Field.Mode.NULLABLE)
              .build());
  private static final TableSchema FAST_QUERY_TABLESCHEMA = FAST_QUERY_SCHEMA.toPb();
  private static final BigQueryResultSet BQ_RS_MOCK_RES =
      new BigQueryResultSetImpl(FAST_QUERY_SCHEMA, 2, null, null);

  private static final BigQueryResultSet BQ_RS_MOCK_RES_MULTI_PAGE =
      new BigQueryResultSetImpl(FAST_QUERY_SCHEMA, 4, null, null);

  private BigQueryOptions createBigQueryOptionsForProject(
      String project, BigQueryRpcFactory rpcFactory) {
    return BigQueryOptions.newBuilder()
        .setProjectId(project)
        .setServiceRpcFactory(rpcFactory)
        .setRetrySettings(ServiceOptions.getNoRetrySettings())
        .build();
  }

  @Before
  public void setUp() {
    rpcFactoryMock = mock(BigQueryRpcFactory.class);
    bigqueryRpcMock = mock(BigQueryRpc.class);
    connectionMock = mock(Connection.class);
    when(rpcFactoryMock.create(any(BigQueryOptions.class))).thenReturn(bigqueryRpcMock);
    options = createBigQueryOptionsForProject(PROJECT, rpcFactoryMock);
    bigquery = options.getService();

    connectionSettings =
        ConnectionSettings.newBuilder()
            .setDefaultDataset(DatasetId.of(DEFAULT_TEST_DATASET))
            .setNumBufferedRows(DEFAULT_PAGE_SIZE)
            .build();
    bigquery =
        options
            .toBuilder()
            .setRetrySettings(ServiceOptions.getDefaultRetrySettings())
            .build()
            .getService();
    connection = bigquery.createConnection(connectionSettings);
    assertNotNull(connection);
  }

  @Test
  public void testFastQuerySinglePage() throws BigQuerySQLException {
    com.google.api.services.bigquery.model.QueryResponse mockQueryRes =
        new QueryResponse().setSchema(FAST_QUERY_TABLESCHEMA).setJobComplete(true);
    when(bigqueryRpcMock.queryRpc(any(String.class), any(QueryRequest.class)))
        .thenReturn(mockQueryRes);
    Connection connectionSpy = Mockito.spy(connection);
    doReturn(BQ_RS_MOCK_RES)
        .when(connectionSpy)
        .processQueryResponseResults(
            any(com.google.api.services.bigquery.model.QueryResponse.class));

    BigQueryResultSet res = connectionSpy.executeSelect(FAST_SQL);
    assertEquals(res.getTotalRows(), 2);
    assertEquals(FAST_QUERY_SCHEMA, res.getSchema());
    verify(connectionSpy, times(1))
        .processQueryResponseResults(
            any(com.google.api.services.bigquery.model.QueryResponse.class));
  }

  @Test
  public void testFastQueryMultiplePages() throws BigQuerySQLException {
    com.google.api.services.bigquery.model.QueryResponse mockQueryRes =
        new QueryResponse()
            .setSchema(FAST_QUERY_TABLESCHEMA)
            .setJobComplete(true)
            .setPageToken(PAGE_TOKEN);
    when(bigqueryRpcMock.queryRpc(any(String.class), any(QueryRequest.class)))
        .thenReturn(mockQueryRes);
    Connection connectionSpy = Mockito.spy(connection);

    doReturn(BQ_RS_MOCK_RES_MULTI_PAGE)
        .when(connectionSpy)
        .processQueryResponseResults(
            any(com.google.api.services.bigquery.model.QueryResponse.class));

    BigQueryResultSet res = connectionSpy.executeSelect(FAST_SQL);
    assertEquals(res.getTotalRows(), 4);
    assertEquals(FAST_QUERY_SCHEMA, res.getSchema());
    verify(connectionSpy, times(1))
        .processQueryResponseResults(
            any(com.google.api.services.bigquery.model.QueryResponse.class));
  }

  @Test
  public void testCancel() throws BigQuerySQLException {
    boolean cancelled = connection.cancel();
    assertTrue(cancelled);
  }

  @Test
  public void testQueryDryRun() throws BigQuerySQLException {
    // TODO

  }

  // Question: Shall I expose these as well for testing and test using some mock/dummy data?
  // TODO: Add testNextPageTask()

  // TODO: Add testParseDataTask()

  // TODO: Add testPopulateBuffer()

  // TODO: Add testGetQueryResultsFirstPage()

  // TODO: Add testFastQueryMultiplePages() --> should exercise processQueryResponseResults() method

  // TODO: Add testFastQueryLongRunning() --> should exercise getSubsequentQueryResultsWithJob()
  // method

  // TODO: Add testLegacyQuerySinglePage() --> should exercise createQueryJob() and
  // getQueryResultsFirstPage()

  // TODO: Add testLegacyQueryMultiplePages() --> should exercise processQueryResponseResults()
  // method

  // TODO: Add testQueryDryRun()

}
