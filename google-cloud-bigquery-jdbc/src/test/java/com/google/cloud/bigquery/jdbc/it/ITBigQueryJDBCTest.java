/*
 * Copyright 2023 Google LLC
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

package com.google.cloud.bigquery.jdbc.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.exception.BigQueryJdbcException;
import com.google.cloud.bigquery.exception.BigQueryJdbcRuntimeException;
import com.google.cloud.bigquery.exception.BigQueryJdbcSqlFeatureNotSupportedException;
import com.google.cloud.bigquery.exception.BigQueryJdbcSqlSyntaxErrorException;
import com.google.cloud.bigquery.jdbc.BigQueryConnection;
import com.google.cloud.bigquery.jdbc.BigQueryDriver;
import com.google.cloud.bigquery.jdbc.DataSource;
import com.google.cloud.bigquery.jdbc.PooledConnectionDataSource;
import com.google.cloud.bigquery.jdbc.PooledConnectionListener;
import com.google.cloud.bigquery.jdbc.utils.TestUtilities.TestConnectionListener;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import javax.sql.PooledConnection;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ITBigQueryJDBCTest extends ITDriverAgnosticTest {
  static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
  static Connection bigQueryConnection;
  static BigQuery bigQuery;
  static Statement bigQueryStatement;
  static Connection bigQueryConnectionNoReadApi;
  static Statement bigQueryStatementNoReadApi;
  static final String connection_uri =
      "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
          + PROJECT_ID
          + ";OAUTHTYPE=3";
  static final String session_enabled_connection_uri =
      "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
          + PROJECT_ID
          + ";OAUTHTYPE=3;EnableSession=1";
  private static final String BASE_QUERY =
      "SELECT * FROM bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2017 order by"
          + " trip_distance asc LIMIT %s";
  private static final Random random = new Random();
  private static final int randomNumber = random.nextInt(9999);
  private static final String DATASET = "JDBC_PRESUBMIT_INTEGRATION_DATASET";
  private static final String DATASET2 = "JDBC_PRESUBMIT_INTEGRATION_DATASET_2";
  private static final String CONSTRAINTS_DATASET = "JDBC_CONSTRAINTS_TEST_DATASET";
  private static final String CONSTRAINTS_TABLE_NAME = "JDBC_CONSTRAINTS_TEST_TABLE";
  private static final String CONSTRAINTS_TABLE_NAME2 = "JDBC_CONSTRAINTS_TEST_TABLE2";
  private static final String CONSTRAINTS_TABLE_NAME3 = "JDBC_CONSTRAINTS_TEST_TABLE3";
  private static final String CALLABLE_STMT_PROC_NAME = "IT_CALLABLE_STMT_PROC_TEST";
  private static final String CALLABLE_STMT_TABLE_NAME = "IT_CALLABLE_STMT_PROC_TABLE";
  private static final String CALLABLE_STMT_PARAM_KEY = "CALL_STMT_PARAM_KEY";
  private static final String CALLABLE_STMT_DML_INSERT_PROC_NAME =
      "IT_CALLABLE_STMT_PROC_DML_INSERT_TEST";
  private static final String CALLABLE_STMT_DML_UPDATE_PROC_NAME =
      "IT_CALLABLE_STMT_PROC_DML_UPDATE_TEST";
  private static final String CALLABLE_STMT_DML_DELETE_PROC_NAME =
      "IT_CALLABLE_STMT_PROC_DML_DELETE_TEST";
  private static final String CALLABLE_STMT_DML_TABLE_NAME = "IT_CALLABLE_STMT_PROC_DML_TABLE";
  private static final Long DEFAULT_CONN_POOL_SIZE = 10L;
  private static final Long CUSTOM_CONN_POOL_SIZE = 5L;
  private static final Object EXCEPTION_REPLACEMENT = "EXCEPTION-WAS-RAISED";

  private static String requireEnvVar(String varName) {
    String value = System.getenv(varName);
    assertNotNull(
        "Environment variable " + varName + " is required to perform these tests.",
        System.getenv(varName));
    return value;
  }

  private JsonObject getAuthJson() throws IOException {
    final String secret = requireEnvVar("SA_SECRET");
    JsonObject authJson;
    // Supporting both formats of SA_SECRET:
    // - Local runs can point to a json file
    // - Cloud Build has JSON value
    try {
      InputStream stream = Files.newInputStream(Paths.get(secret));
      InputStreamReader reader = new InputStreamReader(stream);
      authJson = JsonParser.parseReader(reader).getAsJsonObject();
    } catch (IOException e) {
      authJson = JsonParser.parseString(secret).getAsJsonObject();
    }
    assertTrue(authJson.has("client_email"));
    assertTrue(authJson.has("private_key"));
    assertTrue(authJson.has("project_id"));
    return authJson;
  }

  private void validateConnection(String connection_uri) throws SQLException {
    Connection connection = DriverManager.getConnection(connection_uri);
    assertNotNull(connection);
    assertFalse(connection.isClosed());
    assertEquals(
        "GOOGLE_SERVICE_ACCOUNT",
        ((BigQueryConnection) connection).getAuthProperties().get("OAuthType"));
    String query =
        "SELECT DISTINCT repository_name FROM `bigquery-public-data.samples.github_timeline` LIMIT"
            + " 850";
    Statement statement = connection.createStatement();
    ResultSet jsonResultSet = statement.executeQuery(query);
    assertTrue(jsonResultSet.getClass().getName().contains("BigQueryJsonResultSet"));
    connection.close();
  }

  @BeforeClass
  public static void beforeClass() throws SQLException {
    bigQueryConnection = DriverManager.getConnection(connection_uri, new Properties());
    bigQueryStatement = bigQueryConnection.createStatement();

    Properties noReadApi = new Properties();
    noReadApi.setProperty("EnableHighThroughputAPI", "0");
    bigQueryConnectionNoReadApi = DriverManager.getConnection(connection_uri, noReadApi);
    bigQueryStatementNoReadApi = bigQueryConnectionNoReadApi.createStatement();
    bigQuery = BigQueryOptions.newBuilder().build().getService();
  }

  @AfterClass
  public static void afterClass() throws SQLException {
    bigQueryStatement.close();
    bigQueryConnection.close();
    bigQueryStatementNoReadApi.close();
    bigQueryConnectionNoReadApi.close();
  }

  @Test
  public void testValidServiceAccountAuthentication() throws SQLException, IOException {
    final JsonObject authJson = getAuthJson();
    File tempFile = File.createTempFile("auth", ".json");
    tempFile.deleteOnExit();
    Files.write(tempFile.toPath(), authJson.toString().getBytes());

    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "ProjectId="
            + authJson.get("project_id").getAsString()
            + ";OAuthType=0;"
            + "OAuthPvtKeyPath="
            + tempFile.toPath()
            + ";";

    validateConnection(connection_uri);
  }

  @Test
  public void testServiceAccountAuthenticationMissingOAuthPvtKeyPath() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "ProjectId="
            + PROJECT_ID
            + ";OAuthType=0;";

    try {
      DriverManager.getConnection(connection_uri);
      Assert.fail();
    } catch (BigQueryJdbcRuntimeException ex) {
      assertTrue(ex.getMessage().contains("No valid credentials provided."));
    }
  }

  @Test
  public void testValidServiceAccountAuthenticationOAuthPvtKeyAsPath()
      throws SQLException, IOException {
    final JsonObject authJson = getAuthJson();
    File tempFile = File.createTempFile("auth", ".json");
    tempFile.deleteOnExit();
    Files.write(tempFile.toPath(), authJson.toString().getBytes());

    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "ProjectId="
            + authJson.get("project_id").getAsString()
            + ";OAuthType=0;"
            + "OAuthServiceAcctEmail=;"
            + ";OAuthPvtKey="
            + tempFile.toPath()
            + ";";
    validateConnection(connection_uri);
  }

  @Test
  public void testValidServiceAccountAuthenticationViaEmailAndPkcs8Key()
      throws SQLException, IOException {
    final JsonObject authJson = getAuthJson();

    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "ProjectId="
            + authJson.get("project_id").getAsString()
            + ";OAuthType=0;"
            + "OAuthServiceAcctEmail="
            + authJson.get("client_email").getAsString()
            + ";OAuthPvtKey="
            + authJson.get("private_key").getAsString()
            + ";";
    validateConnection(connection_uri);
  }

  @Test
  public void testValidServiceAccountAuthenticationOAuthPvtKeyAsJson()
      throws SQLException, IOException {
    final JsonObject authJson = getAuthJson();

    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "ProjectId="
            + authJson.get("project_id").getAsString()
            + ";OAuthType=0;"
            + "OAuthServiceAcctEmail=;"
            + ";OAuthPvtKey="
            + authJson.toString()
            + ";";
    validateConnection(connection_uri);
  }

  // TODO(kirl): Enable this test when pipeline has p12 secret available.
  @Test
  @Ignore
  public void testValidServiceAccountAuthenticationP12() throws SQLException, IOException {
    final JsonObject authJson = getAuthJson();
    final String p12_file = requireEnvVar("SA_SECRET_P12");

    final String connectionUri =
        getBaseUri(0, authJson.get("project_id").getAsString())
            .append("OAuthServiceAcctEmail", authJson.get("client_email").getAsString())
            .append("OAuthPvtKeyPath", p12_file)
            .toString();
    validateConnection(connectionUri);
  }

  @Test
  @Ignore
  public void testValidGoogleUserAccountAuthentication() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAuthType=1;OAuthClientId=client_id;OAuthClientSecret=client_secret;";

    Connection connection = DriverManager.getConnection(connection_uri);
    assertNotNull(connection);
    assertFalse(connection.isClosed());
    assertEquals(
        "GOOGLE_USER_ACCOUNT",
        ((BigQueryConnection) connection).getAuthProperties().get("OAuthType"));

    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "SELECT repository_name FROM `bigquery-public-data.samples.github_timeline` LIMIT 50");

    assertEquals(50, resultSetRowCount(resultSet));
    connection.close();
  }

  @Test
  @Ignore
  public void testValidExternalAccountAuthentication() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAUTHTYPE=4;"
            + "BYOID_AudienceUri=//iam.googleapis.com/projects/<project>/locations/<location>/workloadIdentityPools/<pool>/providers/<provider>;"
            + "BYOID_SubjectTokenType=<type>;BYOID_CredentialSource={\"file\":\"/path/to/file\"};"
            + "BYOID_SA_Impersonation_Uri=<sa>;BYOID_TokenUri=<uri>;";

    Connection connection = DriverManager.getConnection(connection_uri);
    assertNotNull(connection);
    assertFalse(connection.isClosed());
    assertEquals(
        "EXTERNAL_ACCOUNT_AUTH",
        ((BigQueryConnection) connection).getAuthProperties().get("OAuthType"));

    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "SELECT repository_name FROM `bigquery-public-data.samples.github_timeline` LIMIT 50");

    assertEquals(50, resultSetRowCount(resultSet));
    connection.close();
  }

  @Test
  @Ignore
  public void testValidExternalAccountAuthenticationFromFile() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAUTHTYPE=4;"
            + "OAuthPvtKeyPath=/path/to/file;";

    Connection connection = DriverManager.getConnection(connection_uri);
    assertNotNull(connection);
    assertFalse(connection.isClosed());
    assertEquals(
        "EXTERNAL_ACCOUNT_AUTH",
        ((BigQueryConnection) connection).getAuthProperties().get("OAuthType"));

    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "SELECT repository_name FROM `bigquery-public-data.samples.github_timeline` LIMIT 50");

    assertEquals(50, resultSetRowCount(resultSet));
    connection.close();
  }

  @Test
  @Ignore
  public void testValidExternalAccountAuthenticationRawJson() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAUTHTYPE=4;OAuthPvtKey={\n"
            + "  \"universe_domain\": \"googleapis.com\",\n"
            + "  \"type\": \"external_account\",\n"
            + "  \"audience\":"
            + " \"//iam.googleapis.com/projects/<project>/locations/<location>/workloadIdentityPools/<pool>/providers/<provider>\",\n"
            + "  \"subject_token_type\": \"<type>\",\n"
            + "  \"token_url\": \"<url>\",\n"
            + "  \"credential_source\": {\n"
            + "    \"file\": \"/path/to/file\"\n"
            + "  },\n"
            + "  \"service_account_impersonation_url\": \"<sa>\"\n"
            + "};";

    Connection connection = DriverManager.getConnection(connection_uri);
    assertNotNull(connection);
    assertFalse(connection.isClosed());
    assertEquals(
        "EXTERNAL_ACCOUNT_AUTH",
        ((BigQueryConnection) connection).getAuthProperties().get("OAuthType"));

    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "SELECT repository_name FROM `bigquery-public-data.samples.github_timeline` LIMIT 50");

    assertEquals(50, resultSetRowCount(resultSet));
    connection.close();
  }

  // TODO(farhan): figure out how to programmatically generate an access token and test
  @Test
  @Ignore
  public void testValidPreGeneratedAccessTokenAuthentication() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAUTHTYPE=2;OAuthAccessToken=access_token;";

    Connection connection = DriverManager.getConnection(connection_uri);
    assertNotNull(connection);
    assertFalse(connection.isClosed());
    assertEquals(
        "PRE_GENERATED_TOKEN",
        ((BigQueryConnection) connection).getAuthProperties().get("OAuthType"));

    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "SELECT repository_name FROM `bigquery-public-data.samples.github_timeline` LIMIT 50");

    assertEquals(50, resultSetRowCount(resultSet));
    connection.close();
  }

  // TODO(obada): figure out how to programmatically generate a refresh token and test
  @Test
  @Ignore
  public void testValidRefreshTokenAuthentication() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAUTHTYPE=2;OAuthRefreshToken=refresh_token;"
            + ";OAuthClientId=client;OAuthClientSecret=secret;";

    Connection connection = DriverManager.getConnection(connection_uri);
    assertNotNull(connection);
    assertFalse(connection.isClosed());
    assertEquals(
        "PRE_GENERATED_TOKEN",
        ((BigQueryConnection) connection).getAuthProperties().get("OAuthType"));

    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "SELECT repository_name FROM `bigquery-public-data.samples.github_timeline` LIMIT 50");

    assertEquals(50, resultSetRowCount(resultSet));
    connection.close();
  }

  @Test
  public void testValidApplicationDefaultCredentialsAuthentication() throws SQLException {
    String connection_uri = getBaseUri(3, PROJECT_ID).toString();

    Connection connection = DriverManager.getConnection(connection_uri);
    assertNotNull(connection);
    assertFalse(connection.isClosed());
    assertEquals(
        "APPLICATION_DEFAULT_CREDENTIALS",
        ((BigQueryConnection) connection).getAuthProperties().get("OAuthType"));
    connection.close();
  }

  // This test is useing the same client email as a main authorization & impersonation.
  // It requires account to have 'tokenCreator' permission, see
  // https://cloud.google.com/docs/authentication/use-service-account-impersonation#required-roles
  @Test
  public void testServiceAccountAuthenticationWithImpersonation() throws IOException, SQLException {
    final JsonObject authJson = getAuthJson();

    String connection_uri =
        getBaseUri(0, authJson.get("project_id").getAsString())
            .append("OAuthServiceAcctEmail", authJson.get("client_email").getAsString())
            .append("OAuthPvtKey", authJson.get("private_key").getAsString())
            .append("ServiceAccountImpersonationEmail", authJson.get("client_email").getAsString())
            .toString();
    validateConnection(connection_uri);
  }

  // This test uses the same client email for the main authorization and a chain of impersonations.
  // It requires the account to have 'tokenCreator' permission on itself.
  @Test
  public void testServiceAccountAuthenticationWithChainedImpersonation()
      throws IOException, SQLException {
    final JsonObject authJson = getAuthJson();
    String clientEmail = authJson.get("client_email").getAsString();

    String connection_uri =
        getBaseUri(0, authJson.get("project_id").getAsString())
            .append("OAuthServiceAcctEmail", clientEmail)
            .append("OAuthPvtKey", authJson.get("private_key").getAsString())
            .append("ServiceAccountImpersonationEmail", clientEmail)
            .append("ServiceAccountImpersonationChain", clientEmail + "," + clientEmail)
            .toString();
    validateConnection(connection_uri);
  }

  @Test
  public void testFastQueryPathSmall() throws SQLException {
    String query =
        "SELECT DISTINCT repository_name FROM `bigquery-public-data.samples.github_timeline` LIMIT"
            + " 850";
    ResultSet jsonResultSet = bigQueryStatement.executeQuery(query);
    assertTrue(jsonResultSet.getClass().getName().contains("BigQueryJsonResultSet"));
    assertEquals(850, resultSetRowCount(jsonResultSet));
  }

  @Test
  public void testInvalidQuery() throws SQLException {
    String query = "SELECT *";

    try {
      bigQueryStatement.executeQuery(query);
      Assert.fail();
    } catch (BigQueryJdbcException e) {
      assertTrue(e.getMessage().contains("SELECT * must have a FROM clause"));
    }
  }

  @Test
  public void testDriver() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAUTHTYPE=3";

    Driver driver = BigQueryDriver.getRegisteredDriver();
    assertTrue(driver.acceptsURL(connection_uri));

    Connection connection = driver.connect(connection_uri, new Properties());
    assertNotNull(connection);
    Statement st = connection.createStatement();
    boolean rs =
        st.execute("Select * FROM `bigquery-public-data.samples.github_timeline` LIMIT 180");
    assertTrue(rs);
    connection.close();
  }

  @Test
  public void testDefaultDataset() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAUTHTYPE=3;DEFAULTDATASET=testDataset";

    Driver driver = BigQueryDriver.getRegisteredDriver();
    assertTrue(driver.acceptsURL(connection_uri));

    Connection connection = driver.connect(connection_uri, new Properties());
    assertNotNull(connection);
    assertEquals(
        DatasetId.of("testDataset"), ((BigQueryConnection) connection).getDefaultDataset());

    String connection_uri_null_default_dataset =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAUTHTYPE=3";

    assertTrue(driver.acceptsURL(connection_uri_null_default_dataset));

    Connection connection2 = driver.connect(connection_uri_null_default_dataset, new Properties());
    assertNotNull(connection2);
    assertNull(((BigQueryConnection) connection2).getDefaultDataset());
    connection.close();
    connection2.close();
  }

  @Test
  public void testDefaultDatasetWithProject() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAUTHTYPE=3;DEFAULTDATASET="
            + PROJECT_ID
            + ".testDataset";

    Driver driver = BigQueryDriver.getRegisteredDriver();
    assertTrue(driver.acceptsURL(connection_uri));

    Connection connection = driver.connect(connection_uri, new Properties());
    assertNotNull(connection);
    assertEquals(
        DatasetId.of(PROJECT_ID, "testDataset"),
        ((BigQueryConnection) connection).getDefaultDataset());
    connection.close();
  }

  @Test
  public void testLocation() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAUTHTYPE=3;LOCATION=EU";

    Driver driver = BigQueryDriver.getRegisteredDriver();
    assertTrue(driver.acceptsURL(connection_uri));

    Connection connection = driver.connect(connection_uri, new Properties());
    assertEquals(((BigQueryConnection) connection).getLocation(), "EU");

    Statement statement = connection.createStatement();

    // Query a dataset in the EU
    String query =
        "SELECT name FROM `bigquery-public-data.covid19_italy_eu.data_by_province` LIMIT 100";
    ResultSet resultSet = statement.executeQuery(query);
    assertEquals(100, resultSetRowCount(resultSet));

    String connection_uri_null_location =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAUTHTYPE=3";

    assertTrue(driver.acceptsURL(connection_uri_null_location));

    Connection connection2 = driver.connect(connection_uri_null_location, new Properties());
    assertNotNull(connection2);
    assertNull(((BigQueryConnection) connection2).getLocation());
    connection.close();
    connection2.close();
  }

  @Test
  public void testIncorrectLocation() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID="
            + PROJECT_ID
            + ";OAUTHTYPE=3;LOCATION=europe-west3";

    Driver driver = BigQueryDriver.getRegisteredDriver();

    Connection connection = driver.connect(connection_uri, new Properties());
    assertEquals(((BigQueryConnection) connection).getLocation(), "europe-west3");

    // Query a dataset in the US
    Statement statement = connection.createStatement();
    String query = "SELECT * FROM `bigquery-public-data.samples.github_timeline` LIMIT 180";
    BigQueryJdbcException ex =
        assertThrows(BigQueryJdbcException.class, () -> statement.executeQuery(query));
    BigQueryError error = ex.getBigQueryException().getError();
    assertNotNull(error);
    assertEquals("accessDenied", error.getReason());
    connection.close();
  }

  @Test
  public void testCreateStatementWithResultSetHoldabilityUnsupportedTypeForwardOnly() {
    assertThrows(
        BigQueryJdbcSqlFeatureNotSupportedException.class,
        () -> bigQueryConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, 1, 1));
  }

  @Test
  public void testCreateStatementWithResultSetHoldabilityUnsupportedConcurReadOnly() {
    assertThrows(
        BigQueryJdbcSqlFeatureNotSupportedException.class,
        () -> bigQueryConnection.createStatement(1, ResultSet.CONCUR_READ_ONLY, 1));
  }

  @Test
  public void testCreateStatementWithResultSetHoldabilityUnsupportedCloseCursorsAtCommit() {
    assertThrows(
        BigQueryJdbcSqlFeatureNotSupportedException.class,
        () -> bigQueryConnection.createStatement(1, 1, ResultSet.CLOSE_CURSORS_AT_COMMIT));
  }

  @Test
  public void testCreateStatementWithResultSetConcurrencyUnsupportedTypeForwardOnly() {
    assertThrows(
        BigQueryJdbcSqlFeatureNotSupportedException.class,
        () -> bigQueryConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, 1));
  }

  @Test
  public void testCreateStatementWithResultSetConcurrencyUnsupportedConcurReadOnly() {
    assertThrows(
        BigQueryJdbcSqlFeatureNotSupportedException.class,
        () -> bigQueryConnection.createStatement(1, ResultSet.CONCUR_READ_ONLY));
  }

  @Test
  public void testSetTransactionIsolationToNotSerializableThrowsNotSupported() {
    assertThrows(
        BigQueryJdbcSqlFeatureNotSupportedException.class,
        () -> bigQueryConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE + 1));
  }

  @Test
  public void testSetHoldabilityForNonCloseCursorsThrowsNotSupported() throws SQLException {
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());

    assertThrows(
        BigQueryJdbcSqlFeatureNotSupportedException.class,
        () -> connection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT + 1));
    connection.close();
  }

  @Test
  public void testCreateStatementWhenConnectionClosedThrows() throws SQLException {
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());

    connection.close();
    assertThrows(IllegalStateException.class, connection::createStatement);
  }

  @Test
  public void testCreateStatementWithResultSetHoldabilityWhenConnectionClosedThrows()
      throws SQLException {
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());

    connection.close();
    assertThrows(IllegalStateException.class, () -> connection.createStatement(1, 1, 1));
  }

  @Test
  public void testCreateStatementWithResultSetConcurrencyWhenConnectionClosedThrows()
      throws SQLException {
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());

    connection.close();
    assertThrows(IllegalStateException.class, () -> connection.createStatement(1, 1));
  }

  @Test
  public void testSetAutoCommitWithClosedConnectionThrowsIllegalState() throws SQLException {
    BigQueryConnection connection =
        (BigQueryConnection) DriverManager.getConnection(session_enabled_connection_uri);
    connection.close();
    assertThrows(IllegalStateException.class, () -> connection.setAutoCommit(true));
  }

  @Test
  public void testSetCommitToFalseWithoutSessionEnabledThrowsIllegalState() throws SQLException {
    BigQueryConnection connection =
        (BigQueryConnection) DriverManager.getConnection(connection_uri);
    assertThrows(IllegalStateException.class, () -> connection.setAutoCommit(false));
    connection.close();
  }

  @Test
  public void testCommitWithConnectionClosedThrowsIllegalState() throws SQLException {
    BigQueryConnection connection =
        (BigQueryConnection) DriverManager.getConnection(session_enabled_connection_uri);
    connection.close();
    assertThrows(IllegalStateException.class, connection::commit);
  }

  @Test
  public void testCommitToFalseWithoutSessionEnabledThrowsIllegalState() throws SQLException {
    BigQueryConnection connection =
        (BigQueryConnection) DriverManager.getConnection(connection_uri);
    assertThrows(IllegalStateException.class, connection::commit);
    connection.close();
  }

  @Test
  public void testCommitWithNoTransactionStartedThrowsIllegalState() throws SQLException {
    BigQueryConnection connection =
        (BigQueryConnection) DriverManager.getConnection(session_enabled_connection_uri);
    assertThrows(IllegalStateException.class, connection::commit);
    connection.close();
  }

  @Test
  public void testRollbackWithConnectionClosedThrowsIllegalState() throws SQLException {
    BigQueryConnection connection =
        (BigQueryConnection) DriverManager.getConnection(session_enabled_connection_uri);
    connection.close();
    assertThrows(IllegalStateException.class, connection::rollback);
  }

  @Test
  public void testRollbackToFalseWithoutSessionEnabledThrowsIllegalState() throws SQLException {
    BigQueryConnection connection =
        (BigQueryConnection) DriverManager.getConnection(connection_uri);
    assertThrows(IllegalStateException.class, connection::rollback);
    connection.close();
  }

  @Test
  public void testRollbackWithoutTransactionStartedThrowsIllegalState() throws SQLException {
    BigQueryConnection connection =
        (BigQueryConnection) DriverManager.getConnection(session_enabled_connection_uri);
    assertThrows(IllegalStateException.class, connection::rollback);
    connection.close();
  }

  @Test
  public void testGetLocationWhenConnectionClosedThrows() throws SQLException {
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());

    connection.close();
    assertThrows(
        IllegalStateException.class, () -> ((BigQueryConnection) connection).getLocation());
    connection.close();
  }

  @Test
  public void testGetDefaultDatasetWhenConnectionClosedThrows() throws SQLException {
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());

    connection.close();
    assertThrows(
        IllegalStateException.class, () -> ((BigQueryConnection) connection).getDefaultDataset());
  }

  @Test
  public void testGetAutocommitWhenConnectionClosedThrows() throws SQLException {
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());

    connection.close();
    assertThrows(IllegalStateException.class, connection::getAutoCommit);
  }

  @Test
  public void testSetAutocommitWhenConnectionClosedThrows() throws SQLException {
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());

    connection.close();
    assertThrows(IllegalStateException.class, () -> connection.setAutoCommit(true));
  }

  @Test
  public void testExecuteQueryWithInsert() throws SQLException {
    String TABLE_NAME = "JDBC_EXECUTE_UPDATE_TABLE_" + randomNumber;
    String createQuery =
        String.format(
            "CREATE OR REPLACE TABLE %s.%s (`StringField` STRING, `IntegerField` INTEGER);",
            DATASET, TABLE_NAME);
    String dropQuery = String.format("DROP TABLE %s.%s", DATASET, TABLE_NAME);

    assertEquals(0, bigQueryStatement.executeUpdate(createQuery));
    assertThrows(BigQueryJdbcException.class, () -> bigQueryStatement.executeQuery(dropQuery));
    bigQueryStatement.execute(String.format("DROP TABLE IF EXISTS %S.%s", DATASET, TABLE_NAME));
  }

  @Test
  public void testExecuteQueryWithMultipleReturns() throws SQLException {
    String query =
        String.format("SELECT * FROM bigquery-public-data.samples.github_timeline LIMIT 1;");

    assertThrows(BigQueryJdbcException.class, () -> bigQueryStatement.executeQuery(query + query));
  }

  @Test
  public void testExecuteUpdateWithSelect() throws SQLException {
    String selectQuery =
        String.format("SELECT * FROM bigquery-public-data.samples.github_timeline LIMIT 1;");

    assertThrows(BigQueryJdbcException.class, () -> bigQueryStatement.executeUpdate(selectQuery));
  }

  @Test
  public void testPreparedStatementThrowsSyntaxError() throws SQLException {
    String TABLE_NAME = "JDBC_PREPARED_SYNTAX_ERR_TABLE_" + randomNumber;
    String createQuery =
        String.format("CREATE OR REPLACE TABLE %s.%s (? STRING, ? INTEGER);", DATASET, TABLE_NAME);

    PreparedStatement preparedStatement = bigQueryConnection.prepareStatement(createQuery);
    preparedStatement.setString(1, "StringField");
    preparedStatement.setString(2, "IntegerField");
    assertThrows(BigQueryJdbcSqlSyntaxErrorException.class, preparedStatement::execute);

    bigQueryStatement.execute(String.format("DROP TABLE IF EXISTS %S.%s", DATASET, TABLE_NAME));
  }

  @Test
  public void testPreparedStatementThrowsJdbcException() throws SQLException {
    String TABLE_NAME = "JDBC_PREPARED_MISSING_PARAM_TABLE_" + randomNumber;
    String createQuery =
        String.format(
            "CREATE OR REPLACE TABLE %s.%s (StringField STRING, IntegerField INTEGER);",
            DATASET, TABLE_NAME);
    boolean createStatus = bigQueryStatement.execute(createQuery);
    assertFalse(createStatus);

    String insertQuery =
        String.format(
            "INSERT INTO %s.%s (StringField, IntegerField) " + "VALUES (?,?), (?,?);",
            DATASET, TABLE_NAME);
    PreparedStatement insertStmt = bigQueryConnection.prepareStatement(insertQuery);
    insertStmt.setString(1, "String1");
    insertStmt.setInt(2, 111);
    assertThrows(BigQueryJdbcException.class, insertStmt::execute);

    bigQueryStatement.execute(String.format("DROP TABLE IF EXISTS %S.%s", DATASET, TABLE_NAME));
  }

  @Test
  public void testSetFetchDirectionFetchReverseThrowsUnsupported() {
    assertThrows(
        BigQueryJdbcSqlFeatureNotSupportedException.class,
        () -> bigQueryStatement.setFetchDirection(ResultSet.FETCH_REVERSE));
  }

  @Test
  public void testSetFetchDirectionFetchUnknownThrowsUnsupported() {
    assertThrows(
        BigQueryJdbcSqlFeatureNotSupportedException.class,
        () -> bigQueryStatement.setFetchDirection(ResultSet.FETCH_UNKNOWN));
  }

  @Test
  public void testExecuteBatchQueryTypeSelectThrowsUnsupported() throws SQLException {
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    String query =
        "SELECT repository_name FROM `bigquery-public-data.samples.github_timeline` WHERE"
            + " repository_name LIKE 'X%' LIMIT 10";
    Statement statement = connection.createStatement();

    assertThrows(IllegalArgumentException.class, () -> statement.addBatch(query));
    connection.close();
  }

  @Test
  public void testValidExecuteBatch() throws SQLException {
    // setup
    String BATCH_TABLE = "JDBC_EXECUTE_BATCH_TABLE_" + random.nextInt(99);
    String createBatchTable =
        String.format(
            "CREATE OR REPLACE TABLE %s.%s (`id` INTEGER, `name` STRING, `age` INTEGER);",
            DATASET, BATCH_TABLE);
    bigQueryStatement.execute(createBatchTable);
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    // batch bypasses the 16 concurrent limit
    int[] results;
    for (int i = 0; i < 3; i++) {
      String insertQuery =
          "INSERT INTO "
              + DATASET
              + "."
              + BATCH_TABLE
              + " (id, name, age) "
              + "VALUES (12, 'Farhan', "
              + randomNumber
              + i
              + "); ";
      statement.addBatch(insertQuery);
    }
    results = statement.executeBatch();

    // assertions
    assertEquals(3, results.length);
    for (int updateCount : results) {
      assertEquals(1, updateCount);
    }
    bigQueryStatement.execute(String.format("DROP TABLE IF EXISTS %S.%s", DATASET, BATCH_TABLE));
    connection.close();
  }

  @Test
  public void testAddBatchWithoutSemicolon() throws SQLException {
    // setup
    String BATCH_TABLE = "JDBC_EXECUTE_BATCH_TABLE_MISSING_SEMICOLON_" + random.nextInt(99);
    String createBatchTable =
        String.format(
            "CREATE OR REPLACE TABLE %s.%s (`id` INTEGER, `name` STRING, `age` INTEGER);",
            DATASET, BATCH_TABLE);
    bigQueryStatement.execute(createBatchTable);
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    // batch bypasses the 16 concurrent limit
    String insertQuery =
        "INSERT INTO "
            + DATASET
            + "."
            + BATCH_TABLE
            + " (id, name, age) "
            + "VALUES (12, 'Farhan', 4)";
    statement.addBatch(insertQuery);
    statement.addBatch(insertQuery);
    int[] results = statement.executeBatch();

    // assertions
    assertEquals(2, results.length);
    for (int updateCount : results) {
      assertEquals(1, updateCount);
    }
    bigQueryStatement.execute(String.format("DROP TABLE IF EXISTS %S.%s", DATASET, BATCH_TABLE));
    connection.close();
  }

  @Test
  public void testEmptySqlToAddBatch() throws SQLException {
    // setup
    String BATCH_TABLE = "JDBC_EMPTY_EXECUTE_BATCH_TABLE_" + random.nextInt(99);
    String createBatchTable =
        String.format(
            "CREATE OR REPLACE TABLE %s.%s (`id` INTEGER, `name` STRING, `age` INTEGER);",
            DATASET, BATCH_TABLE);
    bigQueryStatement.execute(createBatchTable);
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    // batch bypasses the 16 concurrent limit
    String emptySql = "";
    statement.addBatch(emptySql);
    int[] results = statement.executeBatch();

    // assertions
    assertEquals(0, results.length);
    bigQueryStatement.execute(String.format("DROP TABLE IF EXISTS %S.%s", DATASET, BATCH_TABLE));
    connection.close();
  }

  @Test
  public void testEmptyExecuteBatch() throws SQLException {
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();
    int[] result = statement.executeBatch();

    assertEquals(0, result.length);
    connection.close();
  }

  @Test
  public void testAllValidStatementTypesForAddBatch() throws SQLException {
    // setup
    String BATCH_TABLE = "JDBC_EXECUTE_BATCH_TABLE_ALL_VALID_TYPES_" + random.nextInt(99);
    String createBatchTable =
        String.format(
            "CREATE OR REPLACE TABLE %s.%s (`id` INTEGER, `name` STRING, `age` INTEGER);",
            DATASET, BATCH_TABLE);
    bigQueryStatement.execute(createBatchTable);
    String insertQuery =
        "INSERT INTO "
            + DATASET
            + "."
            + BATCH_TABLE
            + " (id, name, age) "
            + "VALUES (12, 'Farhan', "
            + randomNumber
            + "); ";
    String updateQuery =
        String.format(
            "UPDATE %s.%s SET age = 13 WHERE age = %s;", DATASET, BATCH_TABLE, randomNumber);
    String deleteQuery =
        String.format("DELETE FROM %s.%s WHERE name='Farhan';", DATASET, BATCH_TABLE);
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    statement.addBatch(insertQuery);
    statement.addBatch(updateQuery);
    statement.addBatch(deleteQuery);
    int[] results = statement.executeBatch();

    // assertion
    for (int updateCount : results) {
      assertEquals(1, updateCount);
    }
    bigQueryStatement.execute(String.format("DROP TABLE IF EXISTS %S.%s", DATASET, BATCH_TABLE));
    connection.close();
  }

  @Test
  public void testIntervalDataTypeWithArrowResultSet() throws SQLException {
    String selectQuery =
        "select * from `DATATYPERANGETEST.RangeIntervalTestTable` order by intColumn limit 5000;";
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;ProjectId="
            + PROJECT_ID
            + ";MaxResults=500;HighThroughputActivationRatio=1;"
            + "HighThroughputMinTableSize=100;"
            + "EnableHighThroughputAPI=1;JobCreationMode=1;";

    // Read data via JDBC
    Connection connection = DriverManager.getConnection(connection_uri);
    Statement statement = connection.createStatement();

    ResultSet resultSet = statement.executeQuery(selectQuery);
    assertTrue(resultSet.getClass().getName().contains("BigQueryArrowResultSet"));
    resultSet.next();
    assertEquals("0-0 10 -12:30:0.0", resultSet.getString("intervalField"));

    // cleanup
    connection.close();
  }

  @Test
  public void testIntervalDataTypeWithJsonResultSet() throws SQLException {
    String selectQuery =
        "select * from `DATATYPERANGETEST.RangeIntervalTestTable` order by intColumn limit 10 ;";
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;ProjectId="
            + PROJECT_ID
            + ";MaxResults=500;HighThroughputActivationRatio=1;"
            + "HighThroughputMinTableSize=100;"
            + "EnableHighThroughputAPI=0;JobCreationMode=1;";

    // Read data via JDBC
    Connection connection = DriverManager.getConnection(connection_uri);
    Statement statement = connection.createStatement();

    ResultSet resultSet = statement.executeQuery(selectQuery);
    assertTrue(resultSet.getClass().getName().contains("BigQueryJsonResultSet"));
    resultSet.next();
    assertEquals("0-0 10 -12:30:0", resultSet.getString("intervalField"));

    // cleanup
    connection.close();
  }

  @Test
  public void testValidEndpointWithInvalidBQPortThrows() throws SQLException {
    String TABLE_NAME = "JDBC_REGIONAL_TABLE_" + randomNumber;
    String selectQuery = "select * from " + DATASET + "." + TABLE_NAME;
    String connection_uri =
        "jdbc:bigquery://https://googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";"
            + "EndpointOverrides=BIGQUERY=https://us-east4-bigquery.googleapis.com:12312312;";

    // Read data via JDBC
    Connection connection = DriverManager.getConnection(connection_uri);
    Statement statement = connection.createStatement();
    assertThrows(BigQueryJdbcException.class, () -> statement.executeQuery(selectQuery));
    connection.close();
  }

  @Test
  public void testLEPEndpointDataNotFoundThrows() throws SQLException {
    String DATASET = "JDBC_REGIONAL_DATASET";
    String TABLE_NAME = "REGIONAL_TABLE";
    String selectQuery = "select * from " + DATASET + "." + TABLE_NAME;
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";"
            + "EndpointOverrides=BIGQUERY=https://us-east5-bigquery.googleapis.com;";

    // Attempting read data via JDBC
    Connection connection = DriverManager.getConnection(connection_uri);
    Statement statement = connection.createStatement();
    assertThrows(BigQueryJdbcException.class, () -> statement.executeQuery(selectQuery));
    connection.close();
  }

  @Test
  public void testREPEndpointDataNotFoundThrows() throws SQLException {
    String DATASET = "JDBC_REGIONAL_DATASET";
    String TABLE_NAME = "REGIONAL_TABLE";
    String selectQuery = "select * from " + DATASET + "." + TABLE_NAME;
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";"
            + "EndpointOverrides=BIGQUERY=https://bigquery.us-east7.rep.googleapis.com;";

    // Attempting read data via JDBC
    Connection connection = DriverManager.getConnection(connection_uri);
    Statement statement = connection.createStatement();
    assertThrows(BigQueryJdbcException.class, () -> statement.executeQuery(selectQuery));
    connection.close();
  }

  @Test
  public void testDataSource() throws SQLException {
    DataSource ds = new DataSource();
    ds.setURL("jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;");
    ds.setOAuthType(3);

    try (Connection connection = ds.getConnection()) {
      assertFalse(connection.isClosed());
    }
  }

  @Test
  public void testDataSourceOAuthPvtKeyPath() throws SQLException, IOException {
    File tempFile = File.createTempFile("auth", ".json");
    tempFile.deleteOnExit();
    DataSource ds = new DataSource();
    ds.setURL("jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;");
    ds.setOAuthType(0);
    ds.setOAuthPvtKeyPath(tempFile.toPath().toString());
    assertEquals(0, ds.getOAuthType().intValue());
    assertEquals(tempFile.toPath().toString(), ds.getOAuthPvtKeyPath());
  }

  @Test
  public void testPreparedStatementSmallSelect() throws SQLException {
    String query =
        "SELECT * FROM `bigquery-public-data.samples.github_timeline` where repository_language=?"
            + " LIMIT 1000";
    PreparedStatement preparedStatement = bigQueryConnection.prepareStatement(query);
    preparedStatement.setString(1, "Java");

    ResultSet jsonResultSet = preparedStatement.executeQuery();

    int rowCount = resultSetRowCount(jsonResultSet);
    assertEquals(1000, rowCount);
    assertTrue(jsonResultSet.getClass().getName().contains("BigQueryJsonResultSet"));
  }

  @Test
  public void testValidDestinationTableSavesQueriesWithLegacySQL() throws SQLException {
    // setup
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";QueryDialect=BIG_QUERY;"
            + "AllowLargeResults=1;"
            + "LargeResultTable=destination_table_test_legacy;"
            + "LargeResultDataset=INTEGRATION_TESTS;";
    String selectLegacyQuery =
        "SELECT * FROM [bigquery-public-data.deepmind_alphafold.metadata] LIMIT 200;";
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    ResultSet resultSet = statement.executeQuery(selectLegacyQuery);

    // assertion
    assertNotNull(resultSet);
    String selectQuery = "SELECT * FROM `INTEGRATION_TESTS.destination_table_test_legacy`;";
    ResultSet actualResultSet = bigQueryStatement.executeQuery(selectQuery);
    assertTrue(0 < resultSetRowCount(actualResultSet));

    // clean up
    String deleteRows = "DELETE FROM `INTEGRATION_TESTS.destination_table_test_legacy` WHERE 1=1;";
    bigQueryStatement.execute(deleteRows);
    connection.close();
  }

  @Test
  public void testValidDestinationTableSavesQueriesWithStandardSQL() throws SQLException {
    // setup
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";QueryDialect=SQL;"
            + "LargeResultTable=destination_table_test;"
            + "LargeResultDataset=INTEGRATION_TESTS;";
    String selectLegacyQuery =
        "SELECT * FROM `bigquery-public-data.deepmind_alphafold.metadata` LIMIT 200;";
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    ResultSet resultSet = statement.executeQuery(selectLegacyQuery);

    // assertion
    assertNotNull(resultSet);
    String selectQuery = "SELECT * FROM INTEGRATION_TESTS.destination_table_test;";
    ResultSet actualResultSet = bigQueryStatement.executeQuery(selectQuery);
    assertEquals(200, resultSetRowCount(actualResultSet));

    // clean up
    String deleteRows = "DELETE FROM `INTEGRATION_TESTS.destination_table_test` WHERE 1=1;";
    bigQueryStatement.execute(deleteRows);
    connection.close();
  }

  @Test
  public void testDestinationTableAndDestinationDatasetThatDoesNotExistsCreates()
      throws SQLException {
    // setup
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";QueryDialect=BIG_QUERY;"
            + "AllowLargeResults=1;"
            + "LargeResultTable=FakeTable;"
            + "LargeResultDataset=FakeDataset;";
    String selectLegacyQuery =
        "SELECT * FROM [bigquery-public-data.deepmind_alphafold.metadata] LIMIT 200;";
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    ResultSet resultSet = statement.executeQuery(selectLegacyQuery);

    // assertion
    assertNotNull(resultSet);
    String separateQuery = "SELECT * FROM FakeDataset.FakeTable;";
    boolean result = bigQueryStatement.execute(separateQuery);
    assertTrue(result);

    // clean up
    bigQueryStatement.execute("DROP SCHEMA FakeDataset CASCADE;");
    connection.close();
  }

  @Test
  public void testDestinationTableWithMissingDestinationDatasetDefaults() throws SQLException {
    // setup
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";QueryDialect=BIG_QUERY;"
            + "AllowLargeResults=1;"
            + "LargeResultTable=FakeTable;";
    String selectLegacyQuery =
        "SELECT * FROM [bigquery-public-data.deepmind_alphafold.metadata] LIMIT 200;";
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    ResultSet resultSet = statement.executeQuery(selectLegacyQuery);

    // assertion
    assertNotNull(resultSet);
    String separateQuery = "SELECT * FROM _google_jdbc.FakeTable;";
    boolean result = bigQueryStatement.execute(separateQuery);
    assertTrue(result);
    connection.close();
  }

  @Test
  public void testNonSelectForLegacyDestinationTableThrows() throws SQLException {
    // setup
    String TRANSACTION_TABLE = "JDBC_TRANSACTION_TABLE" + random.nextInt(99);
    String createTransactionTable =
        String.format(
            "CREATE OR REPLACE TABLE %s.%s (`id` INTEGER, `name` STRING, `age` INTEGER);",
            DATASET, TRANSACTION_TABLE);
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";QueryDialect=BIG_QUERY;"
            + "AllowLargeResults=1;"
            + "LargeResultTable=destination_table_test;"
            + "LargeResultDataset=INTEGRATION_TESTS;";
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act & assertion
    assertThrows(BigQueryJdbcException.class, () -> statement.execute(createTransactionTable));
    connection.close();
  }

  @Test
  public void testNonSelectForStandardDestinationTableDoesNotThrow() throws SQLException {
    // setup
    String TRANSACTION_TABLE = "JDBC_TRANSACTION_TABLE" + random.nextInt(99);
    String createTransactionTable =
        String.format(
            "CREATE OR REPLACE TABLE %s.%s (`id` INTEGER, `name` STRING, `age` INTEGER);",
            DATASET, TRANSACTION_TABLE);
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";QueryDialect=SQL;"
            + "AllowLargeResults=1;"
            + "LargeResultTable=destination_table_test;"
            + "LargeResultDataset=INTEGRATION_TESTS;";
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act & assertion
    statement.execute(createTransactionTable);
    connection.close();
  }

  @Test
  public void testRangeDataTypeWithArrowResultSet() throws SQLException {
    String selectQuery =
        "select * from `DATATYPERANGETEST.RangeIntervalTestTable` order by intColumn limit 5000;";

    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;ProjectId="
            + PROJECT_ID
            + ";MaxResults=500;HighThroughputActivationRatio=1;"
            + "HighThroughputMinTableSize=100;"
            + "EnableHighThroughputAPI=1;JobCreationMode=1;";

    // Read data via JDBC
    Connection connection = DriverManager.getConnection(connection_uri);
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(selectQuery);
    assertTrue(resultSet.getClass().getName().contains("BigQueryArrowResultSet"));
    resultSet.next();
    assertEquals("[2024-07-14, 2024-09-23)", resultSet.getString("rangeField"));
    connection.close();
  }

  @Test
  public void testPrepareCallFailureResultSetType() throws SQLException {
    assertThrows(
        BigQueryJdbcSqlFeatureNotSupportedException.class,
        () ->
            this.bigQueryConnection.prepareCall(
                "call testProc", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
  }

  @Test
  public void testPrepareCallFailureResultSetConcurrency() throws SQLException {
    assertThrows(
        BigQueryJdbcSqlFeatureNotSupportedException.class,
        () ->
            this.bigQueryConnection.prepareCall(
                "call testProc", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
  }

  @Test
  public void testPrepareCallFailureResultSetHoldability() throws SQLException {
    assertThrows(
        BigQueryJdbcSqlFeatureNotSupportedException.class,
        () ->
            this.bigQueryConnection.prepareCall(
                "call testProc",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT));
  }

  // Integration tests for CallableStatement Setters and Getters

  @Test
  public void testPooledConnectionDataSourceSuccess() throws SQLException {
    String connectionUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;OAuthType=3;ProjectId=testProject;ConnectionPoolSize=20;ListenerPoolSize=20;";

    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionUrl);

    PooledConnection pooledConnection = pooledDataSource.getPooledConnection();
    assertNotNull(pooledConnection);
  }

  @Test
  public void testPooledConnectionDataSourceFailNoConnectionURl() throws SQLException {
    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();

    assertThrows(BigQueryJdbcException.class, () -> pooledDataSource.getPooledConnection());
  }

  @Test
  public void testPooledConnectionDataSourceFailInvalidConnectionURl() {
    String connectionUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;OAuthType=3;ProjectId=testProject;"
            + "ListenerPoolSize=invalid";
    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionUrl);

    assertThrows(NumberFormatException.class, () -> pooledDataSource.getPooledConnection());
  }

  @Test
  public void testPooledConnectionAddConnectionListener() throws SQLException {
    String connectionUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;OAuthType=3;ProjectId=testProject;ConnectionPoolSize=20;ListenerPoolSize=20;";

    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionUrl);

    PooledConnection pooledConnection = pooledDataSource.getPooledConnection();
    assertNotNull(pooledConnection);
    TestConnectionListener listener = new TestConnectionListener();
    pooledConnection.addConnectionEventListener(listener);
    assertEquals(0, listener.getConnectionClosedCount());
    assertEquals(0, listener.getConnectionErrorCount());
  }

  @Test
  public void testPooledConnectionRemoveConnectionListener() throws SQLException {
    String connectionUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;OAuthType=3;ProjectId=testProject;ConnectionPoolSize=20;ListenerPoolSize=20;";

    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionUrl);

    PooledConnection pooledConnection = pooledDataSource.getPooledConnection();
    assertNotNull(pooledConnection);
    TestConnectionListener listener = new TestConnectionListener();
    pooledConnection.removeConnectionEventListener(listener);
    assertEquals(0, listener.getConnectionClosedCount());
    assertEquals(0, listener.getConnectionErrorCount());
  }

  @Test
  public void testPooledConnectionConnectionClosed() throws SQLException {
    String connectionUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;OAuthType=3;ProjectId=testProject;ConnectionPoolSize=20;ListenerPoolSize=20;";

    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionUrl);

    PooledConnection pooledConnection = pooledDataSource.getPooledConnection();
    assertNotNull(pooledConnection);
    TestConnectionListener listener = new TestConnectionListener();
    pooledConnection.addConnectionEventListener(listener);
    assertEquals(0, listener.getConnectionClosedCount());
    assertEquals(0, listener.getConnectionErrorCount());

    Connection connection = pooledConnection.getConnection();
    assertNotNull(connection);
    assertFalse(connection.isClosed());

    connection.close();
    assertEquals(1, listener.getConnectionClosedCount());
    assertEquals(0, listener.getConnectionErrorCount());
  }

  @Test
  public void testPooledConnectionClose() throws SQLException {
    String connectionUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;OAuthType=3;ProjectId=testProject;ConnectionPoolSize=20;ListenerPoolSize=20;";

    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionUrl);

    PooledConnection pooledConnection = pooledDataSource.getPooledConnection();
    assertNotNull(pooledConnection);
    TestConnectionListener listener = new TestConnectionListener();
    pooledConnection.addConnectionEventListener(listener);
    assertEquals(0, listener.getConnectionClosedCount());
    assertEquals(0, listener.getConnectionErrorCount());

    pooledConnection.close();
    assertEquals(1, listener.getConnectionClosedCount());
    assertEquals(0, listener.getConnectionErrorCount());
  }

  @Test
  public void testPooledConnectionConnectionError() throws SQLException {
    String connectionUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;OAuthType=3;ProjectId=testProject;ConnectionPoolSize=20;ListenerPoolSize=20;";

    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionUrl);

    PooledConnection pooledConnection = pooledDataSource.getPooledConnection();
    assertNotNull(pooledConnection);
    TestConnectionListener listener = new TestConnectionListener();
    pooledConnection.addConnectionEventListener(listener);
    assertEquals(0, listener.getConnectionClosedCount());
    assertEquals(0, listener.getConnectionErrorCount());

    Connection connection = pooledConnection.getConnection();
    assertNotNull(connection);
    assertFalse(connection.isClosed());

    ExecutorService executor = Executors.newFixedThreadPool(3);
    connection.abort(executor);
    assertEquals(0, listener.getConnectionClosedCount());
    assertEquals(1, listener.getConnectionErrorCount());

    executor.shutdown();
    connection.close();
    pooledConnection.close();
  }

  @Test
  public void testPooledConnectionListenerAddListener() throws SQLException {
    String connectionUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;OAuthType=3;ProjectId=testProject;ConnectionPoolSize=20;ListenerPoolSize=20;";

    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionUrl);

    PooledConnection pooledConnection = pooledDataSource.getPooledConnection();
    assertNotNull(pooledConnection);
    PooledConnectionListener listener = new PooledConnectionListener(DEFAULT_CONN_POOL_SIZE);
    pooledConnection.addConnectionEventListener(listener);
    assertTrue(listener.isConnectionPoolEmpty());
    pooledConnection.close();
  }

  @Test
  public void testPooledConnectionListenerRemoveListener() throws SQLException {
    String connectionUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;OAuthType=3;ProjectId=testProject;ConnectionPoolSize=20;ListenerPoolSize=20;";

    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionUrl);

    PooledConnection pooledConnection = pooledDataSource.getPooledConnection();
    assertNotNull(pooledConnection);
    PooledConnectionListener listener = new PooledConnectionListener(DEFAULT_CONN_POOL_SIZE);
    pooledConnection.addConnectionEventListener(listener);
    assertTrue(listener.isConnectionPoolEmpty());

    pooledConnection.removeConnectionEventListener(listener);
    assertTrue(listener.isConnectionPoolEmpty());
    pooledConnection.close();
  }

  @Test
  public void testPooledConnectionListenerCloseConnection() throws SQLException {
    String connectionUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;OAuthType=3;ProjectId=testProject;ConnectionPoolSize=20;ListenerPoolSize=20;";

    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionUrl);

    PooledConnection pooledConnection = pooledDataSource.getPooledConnection();
    assertNotNull(pooledConnection);
    PooledConnectionListener listener = new PooledConnectionListener(DEFAULT_CONN_POOL_SIZE);
    pooledConnection.addConnectionEventListener(listener);
    assertTrue(listener.isConnectionPoolEmpty());

    Connection connection = pooledConnection.getConnection();
    assertNotNull(connection);
    assertFalse(connection.isClosed());

    connection.close();
    assertFalse(listener.isConnectionPoolEmpty());
    pooledConnection.close();
  }

  @Test
  public void testPooledConnectionListenerClosePooledConnection() throws SQLException {
    String connectionUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;OAuthType=3;ProjectId=testProject;ConnectionPoolSize=20;ListenerPoolSize=20;";

    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionUrl);

    PooledConnection pooledConnection = pooledDataSource.getPooledConnection();
    assertNotNull(pooledConnection);
    PooledConnectionListener listener = new PooledConnectionListener(DEFAULT_CONN_POOL_SIZE);
    pooledConnection.addConnectionEventListener(listener);
    assertTrue(listener.isConnectionPoolEmpty());

    pooledConnection.close();
    assertFalse(listener.isConnectionPoolEmpty());
  }

  @Test
  public void testPooledConnectionListenerConnectionError() throws SQLException {
    String connectionUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;OAuthType=3;ProjectId=testProject;ConnectionPoolSize=20;ListenerPoolSize=20;";

    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionUrl);

    PooledConnection pooledConnection = pooledDataSource.getPooledConnection();
    assertNotNull(pooledConnection);
    PooledConnectionListener listener = new PooledConnectionListener(DEFAULT_CONN_POOL_SIZE);
    pooledConnection.addConnectionEventListener(listener);
    assertTrue(listener.isConnectionPoolEmpty());

    Connection connection = pooledConnection.getConnection();
    assertNotNull(connection);
    assertFalse(connection.isClosed());

    ExecutorService executor = Executors.newFixedThreadPool(3);
    connection.abort(executor);
    assertTrue(listener.isConnectionPoolEmpty());

    executor.shutdown();
    connection.close();
    pooledConnection.close();
  }

  @Test
  public void testExecuteQueryWithConnectionPoolingEnabledDefaultPoolSize() throws SQLException {
    String connectionURL =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;ProjectId="
            + PROJECT_ID
            + ";";
    assertConnectionPoolingResults(connectionURL, DEFAULT_CONN_POOL_SIZE);
  }

  @Test
  public void testExecuteQueryWithConnectionPoolingEnabledCustomPoolSize() throws SQLException {
    String connectionURL =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;ProjectId="
            + PROJECT_ID
            + ";"
            + "ConnectionPoolSize="
            + CUSTOM_CONN_POOL_SIZE
            + ";";
    assertConnectionPoolingResults(connectionURL, CUSTOM_CONN_POOL_SIZE);
  }

  private void assertConnectionPoolingResults(String connectionURL, Long connectionPoolSize)
      throws SQLException {
    // Create Pooled Connection Datasource
    PooledConnectionDataSource pooledDataSource = new PooledConnectionDataSource();
    pooledDataSource.setURL(connectionURL);

    // Get pooled connection and ensure listner was added with default connection pool size.
    PooledConnection pooledConnection = pooledDataSource.getPooledConnection();
    assertNotNull(pooledConnection);
    PooledConnectionListener listener = pooledDataSource.getConnectionPoolManager();
    assertNotNull(listener);
    assertTrue(listener.isConnectionPoolEmpty());

    // Get Underlying physical connection
    Connection connection = pooledConnection.getConnection();
    assertNotNull(connection);
    assertFalse(connection.isClosed());

    // Execute query with physical connection
    String query =
        "SELECT DISTINCT repository_name FROM `bigquery-public-data.samples.github_timeline` LIMIT"
            + " 850";
    Statement statement = connection.createStatement();
    ResultSet jsonResultSet = statement.executeQuery(query);
    assertTrue(jsonResultSet.getClass().getName().contains("BigQueryJsonResultSet"));

    // Close physical connection
    connection.close();
    assertFalse(listener.isConnectionPoolEmpty());
    assertEquals(1, listener.getConnectionPoolCurrentCapacity());
    assertEquals(connectionPoolSize, listener.getConnectionPoolSize());

    // Reuse same physical connection.
    connection = pooledConnection.getConnection();
    assertNotNull(connection);
    assertFalse(connection.isClosed());
    assertFalse(listener.isConnectionPoolEmpty());
    assertEquals(1, listener.getConnectionPoolCurrentCapacity());
    assertEquals(connectionPoolSize, listener.getConnectionPoolSize());

    // Execute query with reusable physical connection
    jsonResultSet = statement.executeQuery(query);
    assertTrue(jsonResultSet.getClass().getName().contains("BigQueryJsonResultSet"));

    // Return connection back to the pool.
    connection.close();
    assertFalse(listener.isConnectionPoolEmpty());
    assertEquals(1, listener.getConnectionPoolCurrentCapacity());
    assertEquals(connectionPoolSize, listener.getConnectionPoolSize());
    pooledConnection.close();
  }

  public void testQueryPropertyDataSetProjectIdQueriesToCorrectDataset() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";QueryProperties=dataset_project_id="
            + PROJECT_ID
            + ";";
    String insertQuery =
        String.format(
            "INSERT INTO %s.%s (id, name, age) VALUES (15, 'Farhan', 25);",
            "INTEGRATION_TESTS", "Test_Table");
    String selectQuery =
        "SELECT * FROM `bigquery-devtools-drivers.INTEGRATION_TESTS.Test_Table` WHERE age=25;";
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    statement.execute(insertQuery);

    // assertions
    boolean result = statement.execute(selectQuery);
    assertTrue(result);

    // clean up
    String deleteQuery =
        String.format("DELETE FROM %s.%s WHERE age=25", "INTEGRATION_TESTS", "Test_Table");
    statement.execute(deleteQuery);
    connection.close();
  }

  @Test
  public void testQueryPropertyDataSetProjectIdQueriesToIncorrectDatasetThrows()
      throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";QueryProperties=dataset_project_id=bigquerytestdefault"
            + ";";
    String insertQuery =
        String.format(
            "INSERT INTO %s.%s (id, name, age) VALUES (15, 'Farhan', 25);",
            "INTEGRATION_TESTS", "Test_Table");
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act & assertion
    assertThrows(BigQueryJdbcException.class, () -> statement.execute(insertQuery));
    connection.close();
  }

  @Test
  public void testQueryPropertyTimeZoneQueries() throws SQLException {
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";QueryProperties=time_zone=America/New_York;";
    String query = "SELECT * FROM `bigquery-public-data.samples.github_timeline` LIMIT 180";
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    ResultSet resultSet = statement.executeQuery(query);

    // assertions
    assertNotNull(resultSet);
    assertTrue(resultSet.next());
    connection.close();
  }

  @Test
  public void testQueryPropertySessionIdSetsStatementSession()
      throws SQLException, InterruptedException {
    String sessionId = getSessionId();
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";QueryProperties=session_id="
            + sessionId
            + ";";
    String selectQuery =
        "INSERT INTO `bigquery-devtools-drivers.JDBC_INTEGRATION_DATASET.No_KMS_Test_table` (id,"
            + " name, age) VALUES (132, 'Batman', 531);";
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    boolean resultSet = statement.execute(selectQuery);

    // assertions
    assertFalse(resultSet);

    // clean up
    String deleteQuery =
        String.format("DELETE FROM %s.%s WHERE age=25", "INTEGRATION_TESTS", "Test_Table");
    statement.execute(deleteQuery);
    connection.close();
  }

  @Test
  public void testEncryptedTableWithKmsQueries() throws SQLException {
    // setup
    String KMSKeyName = requireEnvVar("KMS_RESOURCE_PATH");
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";KMSKeyName="
            + KMSKeyName
            + ";";
    String selectQuery = "SELECT * FROM `JDBC_INTEGRATION_DATASET.KMS_Test_table`;";
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    ResultSet resultSet = statement.executeQuery(selectQuery);

    // assertions for data not encrypted
    assertNotNull(resultSet);
    assertTrue(resultSet.next());
    assertEquals("Farhan", resultSet.getString("name"));
    connection.close();
  }

  @Test
  public void testIncorrectKmsThrows() throws SQLException {
    String KMSKeyName = requireEnvVar("KMS_RESOURCE_PATH");
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";KMSKeyName="
            + KMSKeyName
            + ";";
    String selectQuery =
        "INSERT INTO `bigquery-devtools-drivers.JDBC_INTEGRATION_DATASET.No_KMS_Test_table` (id,"
            + " name, age) VALUES (132, 'Batman', 531);";
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act & assertion
    assertThrows(BigQueryJdbcException.class, () -> statement.execute(selectQuery));
    connection.close();
  }

  @Test
  public void testQueryPropertyServiceAccountFollowsIamPermission() throws SQLException {
    final String SERVICE_ACCOUNT_EMAIL = requireEnvVar("SA_EMAIL");
    String connection_uri =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "OAuthType=3;"
            + "ProjectId="
            + PROJECT_ID
            + ";QueryProperties=service_account="
            + SERVICE_ACCOUNT_EMAIL
            + ";";
    Driver driver = BigQueryDriver.getRegisteredDriver();
    Connection connection = driver.connect(connection_uri, new Properties());
    Statement statement = connection.createStatement();

    // act
    ResultSet resultSet = statement.executeQuery(String.format(BASE_QUERY, 100));

    // assertions
    assertNotNull(resultSet);
    assertTrue(resultSet.next());
    connection.close();
  }

  @Test
  public void testMultipleTransactionsThrowsUnsupported() throws SQLException {
    BigQueryConnection connection =
        (BigQueryConnection) DriverManager.getConnection(session_enabled_connection_uri);
    connection.setAutoCommit(false);
    Statement statement = connection.createStatement();
    assertThrows(BigQueryJdbcException.class, () -> statement.execute("BEGIN TRANSACTION;"));
    connection.close();
  }

  // Private Helper functions
  private String getSessionId() throws InterruptedException {
    QueryJobConfiguration stubJobConfig =
        QueryJobConfiguration.newBuilder("Select 1;").setCreateSession(true).build();
    Job job = bigQuery.create(JobInfo.of(stubJobConfig));
    job = job.waitFor();
    Job stubJob = bigQuery.getJob(job.getJobId());
    return stubJob.getStatistics().getSessionInfo().getSessionId();
  }

  private void validate(
      String method,
      BiFunction<ResultSet, Integer, Object> getter,
      ImmutableMap<String, Object> expectedResult)
      throws Exception {

    try (Connection connection = DriverManager.getConnection(connection_uri);
        Connection connectionHTAPI =
            DriverManager.getConnection(
                connection_uri
                    + ";HighThroughputMinTableSize=0;HighThroughputActivationRatio=0;EnableHighThroughputAPI=1;");
        Statement statement = connection.createStatement();
        Statement statementHTAPI = connectionHTAPI.createStatement()) {

      String query =
          "SELECT * FROM INTEGRATION_TEST_FORMAT.all_bq_types WHERE stringField is not null";
      ResultSet resultSetRegular = statement.executeQuery(query);
      ResultSet resultSetArrow = statementHTAPI.executeQuery(query);
      resultSetRegular.next();
      resultSetArrow.next();

      for (int i = 1; i <= resultSetRegular.getMetaData().getColumnCount(); i++) {
        String columnName = resultSetRegular.getMetaData().getColumnName(i);

        String regularApiLabel =
            String.format("[Method: %s] [Column: %s] [API: Regular]", method, columnName);
        String htapiApiLabel =
            String.format("[Method: %s] [Column: %s] [API: HTAPI]", method, columnName);

        if (expectedResult.containsKey(columnName)) {
          Object expectedValue = expectedResult.get(columnName);

          assertEquals(regularApiLabel, expectedValue, getter.apply(resultSetRegular, i));
          assertEquals(htapiApiLabel, expectedValue, getter.apply(resultSetArrow, i));

        } else {
          String regularMsg = "Expected exception but got a value. " + regularApiLabel;
          assertEquals(regularMsg, EXCEPTION_REPLACEMENT, getter.apply(resultSetRegular, i));

          String htapiMsg = "Expected exception but got a value. " + htapiApiLabel;
          assertEquals(htapiMsg, EXCEPTION_REPLACEMENT, getter.apply(resultSetArrow, i));
        }
      }
    }
  }

  private int resultSetRowCount(ResultSet resultSet) throws SQLException {
    int rowCount = 0;
    while (resultSet.next()) {
      rowCount++;
    }
    return rowCount;
  }
}
