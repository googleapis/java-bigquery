/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigquery.jdbc;

import com.google.api.client.util.escape.CharEscapers;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.exception.BigQueryJdbcRuntimeException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class implements all the methods that parse Connection property values from the Connection
 * String.
 */
final class BigQueryJdbcUrlUtility {

  // TODO: Add all Connection options
  static final String ALLOW_LARGE_RESULTS_PROPERTY_NAME = "AllowLargeResults";
  static final String LARGE_RESULTS_TABLE_PROPERTY_NAME = "LargeResultTable";
  static final String LARGE_RESULTS_DATASET_PROPERTY_NAME = "LargeResultDataset";
  static final String UNSUPPORTED_HTAPI_FALLBACK_PROPERTY_NAME = "UnsupportedHTAPIFallback";
  static final boolean DEFAULT_UNSUPPORTED_HTAPI_FALLBACK_VALUE = true;
  static final String DESTINATION_DATASET_EXPIRATION_TIME_PROPERTY_NAME =
      "LargeResultsDatasetExpirationTime";
  static final long DEFAULT_DESTINATION_DATASET_EXPIRATION_TIME_VALUE = 3600000L;
  static final boolean DEFAULT_ALLOW_LARGE_RESULTS = true;
  static final String QUERY_DIALECT_PROPERTY_NAME = "QueryDialect";
  static final String DEFAULT_QUERY_DIALECT_VALUE = "SQL";
  static final String UNIVERSE_DOMAIN_OVERRIDE_PROPERTY_NAME = "universeDomain";
  static final String DEFAULT_UNIVERSE_DOMAIN_VALUE = "googleapis.com";
  static final String PROJECT_ID_PROPERTY_NAME = "ProjectId";
  static final String DEFAULT_DATASET_PROPERTY_NAME = "DefaultDataset";
  static final String OAUTH_TYPE_PROPERTY_NAME = "OAuthType";
  static final String HTAPI_ACTIVATION_RATIO_PROPERTY_NAME = "HighThroughputActivationRatio";
  static final String KMS_KEY_NAME_PROPERTY_NAME = "KMSKeyName";
  static final String QUERY_PROPERTIES_NAME = "QueryProperties";
  static final int DEFAULT_HTAPI_ACTIVATION_RATIO_VALUE =
      2; // TODO: to adjust this value before private preview based on performance testing.
  static final String HTAPI_MIN_TABLE_SIZE_PROPERTY_NAME = "HighThroughputMinTableSize";
  static final int DEFAULT_HTAPI_MIN_TABLE_SIZE_VALUE = 100;
  static final int DEFAULT_OAUTH_TYPE_VALUE = -1;
  static final String LOCATION_PROPERTY_NAME = "Location";
  static final String ENDPOINT_OVERRIDES_PROPERTY_NAME = "EndpointOverrides";
  static final String PRIVATE_SERVICE_CONNECT_PROPERTY_NAME = "PrivateServiceConnectUris";
  static final String OAUTH_SA_IMPERSONATION_EMAIL_PROPERTY_NAME =
      "ServiceAccountImpersonationEmail";
  static final String DEFAULT_OAUTH_SA_IMPERSONATION_EMAIL_VALUE = null;
  static final String OAUTH_SA_IMPERSONATION_CHAIN_PROPERTY_NAME =
      "ServiceAccountImpersonationChain";
  static final String DEFAULT_OAUTH_SA_IMPERSONATION_CHAIN_VALUE = null;
  static final String OAUTH_SA_IMPERSONATION_SCOPES_PROPERTY_NAME =
      "ServiceAccountImpersonationScopes";
  static final String DEFAULT_OAUTH_SA_IMPERSONATION_SCOPES_VALUE =
      "https://www.googleapis.com/auth/bigquery";
  static final String OAUTH_SA_IMPERSONATION_TOKEN_LIFETIME_PROPERTY_NAME =
      "ServiceAccountImpersonationTokenLifetime";
  static final String DEFAULT_OAUTH_SA_IMPERSONATION_TOKEN_LIFETIME_VALUE = "3600";
  static final String OAUTH_SA_EMAIL_PROPERTY_NAME = "OAuthServiceAcctEmail";
  static final String OAUTH_PVT_KEY_PATH_PROPERTY_NAME = "OAuthPvtKeyPath";
  static final String OAUTH_P12_PASSWORD_PROPERTY_NAME = "OAuthP12Password";
  static final String DEFAULT_OAUTH_P12_PASSWORD_VALUE = "notasecret";
  static final String OAUTH_PVT_KEY_PROPERTY_NAME = "OAuthPvtKey";
  static final String OAUTH2_TOKEN_URI_PROPERTY_NAME = "OAUTH2";
  static final String HTAPI_ENDPOINT_OVERRIDE_PROPERTY_NAME = "READ_API";
  static final String BIGQUERY_ENDPOINT_OVERRIDE_PROPERTY_NAME = "BIGQUERY";
  static final String STS_ENDPOINT_OVERRIDE_PROPERTY_NAME = "STS";
  static final String OAUTH_ACCESS_TOKEN_PROPERTY_NAME = "OAuthAccessToken";
  static final String OAUTH_REFRESH_TOKEN_PROPERTY_NAME = "OAuthRefreshToken";
  static final String OAUTH_CLIENT_ID_PROPERTY_NAME = "OAuthClientId";
  static final String OAUTH_CLIENT_SECRET_PROPERTY_NAME = "OAuthClientSecret";
  static final String ENABLE_HTAPI_PROPERTY_NAME = "EnableHighThroughputAPI";
  static final String PROXY_HOST_PROPERTY_NAME = "ProxyHost";
  static final String PROXY_PORT_PROPERTY_NAME = "ProxyPort";
  static final String PROXY_USER_ID_PROPERTY_NAME = "ProxyUid";
  static final String PROXY_PASSWORD_PROPERTY_NAME = "ProxyPwd";
  static final String HTTP_CONNECT_TIMEOUT_PROPERTY_NAME = "HttpConnectTimeout";
  static final String HTTP_READ_TIMEOUT_PROPERTY_NAME = "HttpReadTimeout";
  static final boolean DEFAULT_ENABLE_HTAPI_VALUE = false;
  static final boolean DEFAULT_ENABLE_SESSION_VALUE = false;
  static final int DEFAULT_LOG_LEVEL = 0;
  static final String LOG_LEVEL_PROPERTY_NAME = "LogLevel";
  static final String LOG_PATH_PROPERTY_NAME = "LogPath";
  static final String LOG_LEVEL_ENV_VAR = "BIGQUERY_JDBC_LOG_LEVEL";
  static final String LOG_PATH_ENV_VAR = "BIGQUERY_JDBC_LOG_PATH";
  static final String ENABLE_SESSION_PROPERTY_NAME = "EnableSession";
  static final String DEFAULT_LOG_PATH = "";
  static final String USE_QUERY_CACHE_PROPERTY_NAME = "UseQueryCache";
  static final boolean DEFAULT_USE_QUERY_CACHE = true;
  static final String JOB_CREATION_MODE_PROPERTY_NAME = "JobCreationMode";
  static final int DEFAULT_JOB_CREATION_MODE = 2;
  static final String MAX_RESULTS_PROPERTY_NAME = "MaxResults";
  static final long DEFAULT_MAX_RESULTS_VALUE = 10000;
  static final String BYOID_AUDIENCE_URI_PROPERTY_NAME = "BYOID_AudienceUri";
  static final String BYOID_CREDENTIAL_SOURCE_PROPERTY_NAME = "BYOID_CredentialSource";
  static final String BYOID_POOL_USER_PROJECT_PROPERTY_NAME = "BYOID_PoolUserProject";
  static final String BYOID_SA_IMPERSONATION_URI_PROPERTY_NAME = "BYOID_SA_Impersonation_Uri";
  static final String BYOID_SUBJECT_TOKEN_TYPE_PROPERTY_NAME = "BYOID_SubjectTokenType";
  static final String BYOID_TOKEN_URI_PROPERTY_NAME = "BYOID_TokenUri";
  static final String PARTNER_TOKEN_PROPERTY_NAME = "PartnerToken";
  static final String METADATA_FETCH_THREAD_COUNT_PROPERTY_NAME = "MetaDataFetchThreadCount";
  static final int DEFAULT_METADATA_FETCH_THREAD_COUNT_VALUE = 32;
  static final String RETRY_TIMEOUT_IN_SECS_PROPERTY_NAME = "Timeout";
  static final long DEFAULT_RETRY_TIMEOUT_IN_SECS_VALUE = 0L;
  static final String JOB_TIMEOUT_PROPERTY_NAME = "JobTimeout";
  static final long DEFAULT_JOB_TIMEOUT_VALUE = 0L;
  static final String RETRY_INITIAL_DELAY_PROPERTY_NAME = "RetryInitialDelay";
  static final long DEFAULT_RETRY_INITIAL_DELAY_VALUE = 0L;
  static final String RETRY_MAX_DELAY_PROPERTY_NAME = "RetryMaxDelay";
  static final long DEFAULT_RETRY_MAX_DELAY_VALUE = 0L;
  static final String ADDITIONAL_PROJECTS_PROPERTY_NAME = "AdditionalProjects";
  // Applicable only for connection pooling.
  static final String CONNECTION_POOL_SIZE_PROPERTY_NAME = "ConnectionPoolSize";
  static final long DEFAULT_CONNECTION_POOL_SIZE_VALUE = 10L;
  static final String LISTENER_POOL_SIZE_PROPERTY_NAME = "ListenerPoolSize";
  static final long DEFAULT_LISTENER_POOL_SIZE_VALUE = 10L;
  static final String ENABLE_WRITE_API_PROPERTY_NAME = "EnableWriteAPI";
  static final boolean DEFAULT_ENABLE_WRITE_API_VALUE = false;
  static final String SWA_APPEND_ROW_COUNT_PROPERTY_NAME = "SWA_AppendRowCount";
  static final int DEFAULT_SWA_APPEND_ROW_COUNT_VALUE = 1000;
  static final String SWA_ACTIVATION_ROW_COUNT_PROPERTY_NAME = "SWA_ActivationRowCount";
  static final int DEFAULT_SWA_ACTIVATION_ROW_COUNT_VALUE = 3;
  private static final BigQueryJdbcCustomLogger LOG =
      new BigQueryJdbcCustomLogger(BigQueryJdbcUrlUtility.class.getName());
  static final String FILTER_TABLES_ON_DEFAULT_DATASET_PROPERTY_NAME =
      "FilterTablesOnDefaultDataset";
  static final boolean DEFAULT_FILTER_TABLES_ON_DEFAULT_DATASET_VALUE = false;
  static final String REQUEST_GOOGLE_DRIVE_SCOPE_PROPERTY_NAME = "RequestGoogleDriveScope";
  static final String SSL_TRUST_STORE_PROPERTY_NAME = "SSLTrustStore";
  static final String SSL_TRUST_STORE_PWD_PROPERTY_NAME = "SSLTrustStorePwd";
  static final int DEFAULT_REQUEST_GOOGLE_DRIVE_SCOPE_VALUE = 0;
  static final String MAX_BYTES_BILLED_PROPERTY_NAME = "MaximumBytesBilled";
  static final Long DEFAULT_MAX_BYTES_BILLED_VALUE = 0L;
  static final String LABELS_PROPERTY_NAME = "Labels";
  static final List<String> OVERRIDE_PROPERTIES =
      Arrays.asList(
          BIGQUERY_ENDPOINT_OVERRIDE_PROPERTY_NAME,
          OAUTH2_TOKEN_URI_PROPERTY_NAME,
          HTAPI_ENDPOINT_OVERRIDE_PROPERTY_NAME,
          STS_ENDPOINT_OVERRIDE_PROPERTY_NAME);
  static final String REQUEST_REASON_PROPERTY_NAME = "RequestReason";
  static final List<String> BYOID_PROPERTIES =
      Arrays.asList(
          BYOID_AUDIENCE_URI_PROPERTY_NAME,
          BYOID_CREDENTIAL_SOURCE_PROPERTY_NAME,
          BYOID_POOL_USER_PROJECT_PROPERTY_NAME,
          BYOID_SA_IMPERSONATION_URI_PROPERTY_NAME,
          BYOID_SUBJECT_TOKEN_TYPE_PROPERTY_NAME,
          BYOID_TOKEN_URI_PROPERTY_NAME);

  static Set<BigQueryConnectionProperty> PROXY_PROPERTIES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  BigQueryConnectionProperty.newBuilder()
                      .setName(PROXY_HOST_PROPERTY_NAME)
                      .setDescription("The host name of the proxy server.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(PROXY_PORT_PROPERTY_NAME)
                      .setDescription(
                          "The port number of the proxy server to connect to. No defaulting"
                              + " behavior happens.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(PROXY_USER_ID_PROPERTY_NAME)
                      .setDescription("The user name for an authenticated proxy server.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(PROXY_PASSWORD_PROPERTY_NAME)
                      .setDescription("The password for an authenticated proxy server.")
                      .build())));

  static Set<BigQueryConnectionProperty> AUTH_PROPERTIES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_TYPE_PROPERTY_NAME)
                      .setDescription(
                          "This option specifies how the connector obtains or provides the"
                              + " credentials for OAuth\n"
                              + "2.0 authentication")
                      .setDefaultValue(String.valueOf(DEFAULT_OAUTH_TYPE_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_SA_EMAIL_PROPERTY_NAME)
                      .setDescription(
                          "The Service Account email use for Service Account Authentication.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_PVT_KEY_PATH_PROPERTY_NAME)
                      .setDescription(
                          "The location of the credentials file used for this connection.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_PVT_KEY_PROPERTY_NAME)
                      .setDescription("The OAuth private key used for this connection.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_REFRESH_TOKEN_PROPERTY_NAME)
                      .setDescription(
                          "The pre-generated refresh token to be used with BigQuery for"
                              + " authentication.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_ACCESS_TOKEN_PROPERTY_NAME)
                      .setDescription(
                          "The pre-generated access token to be used with BigQuery for"
                              + " authentication.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_CLIENT_ID_PROPERTY_NAME)
                      .setDescription(
                          "The client ID to be used for user authentication or to refresh"
                              + " pre-generated tokens.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_CLIENT_SECRET_PROPERTY_NAME)
                      .setDescription(
                          "The client secret to be used for user authentication or to refresh"
                              + " pre-generated tokens.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_SA_IMPERSONATION_EMAIL_PROPERTY_NAME)
                      .setDescription("The service account email to be impersonated.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_SA_IMPERSONATION_CHAIN_PROPERTY_NAME)
                      .setDescription(
                          "Comma separated list of service account emails in the impersonation"
                              + " chain.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_SA_IMPERSONATION_SCOPES_PROPERTY_NAME)
                      .setDescription(
                          "Comma separated list of OAuth2 scopes to use with impersonated account.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_SA_IMPERSONATION_TOKEN_LIFETIME_PROPERTY_NAME)
                      .setDescription("Impersonated account token lifetime.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(OAUTH_P12_PASSWORD_PROPERTY_NAME)
                      .setDescription("Password for p12 secret file.")
                      .build())));

  static Set<BigQueryConnectionProperty> VALID_PROPERTIES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  BigQueryConnectionProperty.newBuilder()
                      .setName(MAX_BYTES_BILLED_PROPERTY_NAME)
                      .setDescription(
                          " Limits the bytes billed for this query. Queries with bytes billed above"
                              + " this limit will fail (without incurring a charge). If"
                              + " unspecified, the project default is used.")
                      .setDefaultValue(String.valueOf(DEFAULT_MAX_BYTES_BILLED_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(CONNECTION_POOL_SIZE_PROPERTY_NAME)
                      .setDescription("Connection pool size if connection pooling is enabled.")
                      .setDefaultValue(String.valueOf(DEFAULT_CONNECTION_POOL_SIZE_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(LISTENER_POOL_SIZE_PROPERTY_NAME)
                      .setDescription("Listener pool size if connection pooling is enabled.")
                      .setDefaultValue(String.valueOf(DEFAULT_LISTENER_POOL_SIZE_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(RETRY_INITIAL_DELAY_PROPERTY_NAME)
                      .setDescription("Initial delay, in seconds, before the first retry.")
                      .setDefaultValue(String.valueOf(DEFAULT_RETRY_INITIAL_DELAY_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(RETRY_MAX_DELAY_PROPERTY_NAME)
                      .setDescription("Max limit for the retry delay, in seconds.")
                      .setDefaultValue(String.valueOf(DEFAULT_RETRY_MAX_DELAY_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(RETRY_TIMEOUT_IN_SECS_PROPERTY_NAME)
                      .setDescription(
                          "The length of time, in seconds, for which the connector retries a failed"
                              + " API call before timing out.")
                      .setDefaultValue(String.valueOf(DEFAULT_RETRY_TIMEOUT_IN_SECS_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(JOB_TIMEOUT_PROPERTY_NAME)
                      .setDescription(
                          "Job timeout (in seconds) after which the job is cancelled on the server")
                      .setDefaultValue(String.valueOf(DEFAULT_JOB_TIMEOUT_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(UNSUPPORTED_HTAPI_FALLBACK_PROPERTY_NAME)
                      .setDescription(
                          "This option determines whether the connector uses the REST API or"
                              + " returns an error when encountering fetch workflows unsupported by"
                              + " the High-Throughput API.")
                      .setDefaultValue(String.valueOf(DEFAULT_UNSUPPORTED_HTAPI_FALLBACK_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(DESTINATION_DATASET_EXPIRATION_TIME_PROPERTY_NAME)
                      .setDescription(
                          "The expiration time (in milliseconds) for tables in a user-specified"
                              + " large result dataset.")
                      .setDefaultValue(
                          String.valueOf(DEFAULT_DESTINATION_DATASET_EXPIRATION_TIME_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(UNIVERSE_DOMAIN_OVERRIDE_PROPERTY_NAME)
                      .setDescription(
                          "The name of the partner-operated cloud which is a new instance of Google"
                              + " production, known as a Trusted Partner Cloud universe.")
                      .setDefaultValue(DEFAULT_UNIVERSE_DOMAIN_VALUE)
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(PROJECT_ID_PROPERTY_NAME)
                      .setDescription("A globally unique identifier for your project.")
                      .setDefaultValue(BigQueryOptions.getDefaultProjectId())
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(LOG_PATH_PROPERTY_NAME)
                      .setDescription(
                          "The directory where the connector saves log files (when logging is"
                              + " enabled).")
                      .setDefaultValue(DEFAULT_LOG_PATH)
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(DEFAULT_DATASET_PROPERTY_NAME)
                      .setDescription(
                          "This default dataset for query execution. If this option is set, queries"
                              + " with unqualified \n"
                              + "table names will run against this dataset.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(LOCATION_PROPERTY_NAME)
                      .setDescription(
                          "The location where datasets are created/queried. The location will be"
                              + " determined\n"
                              + " automatically by BigQuery if not specified.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(ENABLE_HTAPI_PROPERTY_NAME)
                      .setDescription(
                          "Enables or disables Read API usage in the Driver. Disabled by default.")
                      .setDefaultValue(String.valueOf(DEFAULT_ENABLE_HTAPI_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(HTAPI_ACTIVATION_RATIO_PROPERTY_NAME)
                      .setDescription(
                          "Connector switches to BigQuery Storage API when the number of pages"
                              + " exceed this value.")
                      .setDefaultValue(String.valueOf(DEFAULT_HTAPI_ACTIVATION_RATIO_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(KMS_KEY_NAME_PROPERTY_NAME)
                      .setDescription(
                          "The KMS key name tells BigQuery which key to use when encrypting or"
                              + " decrypting your data.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(QUERY_PROPERTIES_NAME)
                      .setDescription(
                          "Connection-level properties to customize query behavior.") // TODO:
                      // Figure out
                      // a clean way
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(LABELS_PROPERTY_NAME)
                      .setDescription(
                          "Labels associated with the query to organize and group query jobs.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(HTAPI_MIN_TABLE_SIZE_PROPERTY_NAME)
                      .setDescription(
                          "If the number of total rows exceeds this value, the connector switches"
                              + " to the BigQuery Storage API for faster processing.")
                      .setDefaultValue(String.valueOf(DEFAULT_HTAPI_MIN_TABLE_SIZE_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(ENABLE_SESSION_PROPERTY_NAME)
                      .setDescription(
                          "Enable to capture your SQL activities or enable multi statement"
                              + " transactions. Disabled by default.")
                      .setDefaultValue(String.valueOf(DEFAULT_ENABLE_SESSION_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(LOG_LEVEL_PROPERTY_NAME)
                      .setDescription(
                          "Sets the Log Level for the Driver. Set to Level.OFF by default.")
                      .setDefaultValue(String.valueOf(DEFAULT_LOG_LEVEL))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(USE_QUERY_CACHE_PROPERTY_NAME)
                      .setDescription("Enables or disables Query caching. Set to true by default.")
                      .setDefaultValue(String.valueOf(DEFAULT_USE_QUERY_CACHE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(QUERY_DIALECT_PROPERTY_NAME)
                      .setDescription(
                          "Parameter for selecting if the queries should use standard or legacy SQL"
                              + " syntax.")
                      .setDefaultValue(DEFAULT_QUERY_DIALECT_VALUE)
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(ALLOW_LARGE_RESULTS_PROPERTY_NAME)
                      .setDescription(
                          "Enabled by default, must be used with legacy SQL. Used for setting"
                              + " destination table & dataset.")
                      .setDefaultValue(String.valueOf(DEFAULT_ALLOW_LARGE_RESULTS))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(LARGE_RESULTS_TABLE_PROPERTY_NAME)
                      .setDescription("The destination table where queries are saved.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(LARGE_RESULTS_DATASET_PROPERTY_NAME)
                      .setDescription("The destination dataset where queries are saved.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(JOB_CREATION_MODE_PROPERTY_NAME)
                      .setDescription(
                          "Enables or disables Stateless Query mode. Set to false by default.")
                      .setDefaultValue(String.valueOf(DEFAULT_JOB_CREATION_MODE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(MAX_RESULTS_PROPERTY_NAME)
                      .setDescription("Maximum number of results per page")
                      .setDefaultValue(String.valueOf(DEFAULT_MAX_RESULTS_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(BYOID_AUDIENCE_URI_PROPERTY_NAME)
                      .setDescription(
                          "Used for External Account Authentication. Corresponds to the audience"
                              + " property\n"
                              + " in the external account configuration file.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(BYOID_CREDENTIAL_SOURCE_PROPERTY_NAME)
                      .setDescription(
                          "Used for External Account Authentication. The file location or the URI"
                              + " of\n"
                              + " the subject token. Corresponds to the credential_source property"
                              + " in\n"
                              + " the external account configuration file.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(BYOID_POOL_USER_PROJECT_PROPERTY_NAME)
                      .setDescription(
                          "Used for External Account Authentication. The project number associated"
                              + " with\n"
                              + " the workforce pool. Corresponds to the"
                              + " workforce_pool_user_project\n"
                              + " property in the external account configuration file.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(BYOID_SA_IMPERSONATION_URI_PROPERTY_NAME)
                      .setDescription(
                          "Used for External Account Authentication. The service account email."
                              + " Only\n"
                              + " present when service account impersonation is used. Corresponds"
                              + " to\n"
                              + " the service_account_impersonation_url property in the external"
                              + " account\n"
                              + " configuration file.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(BYOID_SUBJECT_TOKEN_TYPE_PROPERTY_NAME)
                      .setDescription(
                          "Used for External Account Authentication. The subject token type."
                              + " Corresponds\n"
                              + " to the subject_token_type property in the external account"
                              + " configuration file.")
                      .setDefaultValue("urn:ietf:params:oauth:tokentype:id_token")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(BYOID_TOKEN_URI_PROPERTY_NAME)
                      .setDescription(
                          "Used for External Account Authentication. The URI used to generate"
                              + " authentication\n"
                              + " tokens. Corresponds to the token_url property in the external"
                              + " account\n"
                              + " configuration file.")
                      .setDefaultValue("https://sts.googleapis.com/v1/token")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(PARTNER_TOKEN_PROPERTY_NAME)
                      .setDescription("The partner name and environment.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(METADATA_FETCH_THREAD_COUNT_PROPERTY_NAME)
                      .setDescription(
                          "The number of threads used to call a DatabaseMetaData method.")
                      .setDefaultValue(String.valueOf(DEFAULT_METADATA_FETCH_THREAD_COUNT_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(ENABLE_WRITE_API_PROPERTY_NAME)
                      .setDescription(
                          "Enables or disables Write API usage for bulk inserts in the Driver."
                              + " Disabled by default.")
                      .setDefaultValue(String.valueOf(DEFAULT_ENABLE_WRITE_API_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(SWA_ACTIVATION_ROW_COUNT_PROPERTY_NAME)
                      .setDescription(
                          "Connector switches to BigQuery Storage Write API when the number of rows"
                              + " for executeBatch insert exceed this value. Do not change unless"
                              + " necessary.")
                      .setDefaultValue(String.valueOf(DEFAULT_SWA_ACTIVATION_ROW_COUNT_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(SWA_APPEND_ROW_COUNT_PROPERTY_NAME)
                      .setDescription("Size of the write stream. Do not change unless necessary.")
                      .setDefaultValue(String.valueOf(DEFAULT_SWA_APPEND_ROW_COUNT_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(ADDITIONAL_PROJECTS_PROPERTY_NAME)
                      .setDescription(
                          "A comma-separated list of Google Cloud project IDs that can be accessed"
                              + " for querying, in addition to the primary project specified in the"
                              + " connection.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(FILTER_TABLES_ON_DEFAULT_DATASET_PROPERTY_NAME)
                      .setDescription(
                          "If true and DefaultDataset is set, DatabaseMetaData.getTables() and"
                              + " .getColumns() will filter results based on the DefaultDataset"
                              + " when catalog/schema patterns are null or wildcards.")
                      .setDefaultValue(
                          String.valueOf(DEFAULT_FILTER_TABLES_ON_DEFAULT_DATASET_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(REQUEST_GOOGLE_DRIVE_SCOPE_PROPERTY_NAME)
                      .setDescription(
                          "Enables or disables whether the connector requests access to Google"
                              + " Drive. Set to false (0) by default.")
                      .setDefaultValue(String.valueOf(DEFAULT_REQUEST_GOOGLE_DRIVE_SCOPE_VALUE))
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(SSL_TRUST_STORE_PROPERTY_NAME)
                      .setDescription(
                          "The full path of the Java TrustStore containing the server certificate"
                              + " for one-way SSL authentication.\n"
                              + "If the trust store requires a password, provide it using the"
                              + " property SSLTrustStorePwd.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(SSL_TRUST_STORE_PWD_PROPERTY_NAME)
                      .setDescription(
                          "The password for accessing the Java TrustStore that is specified using"
                              + " the property SSLTrustStore.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(HTTP_CONNECT_TIMEOUT_PROPERTY_NAME)
                      .setDescription(
                          "The timeout (in milliseconds) for establishing a connection to the"
                              + " server.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(HTTP_READ_TIMEOUT_PROPERTY_NAME)
                      .setDescription("The timeout (in milliseconds) when reading from the server.")
                      .build(),
                  BigQueryConnectionProperty.newBuilder()
                      .setName(REQUEST_REASON_PROPERTY_NAME)
                      .setDescription(
                          "Reason for the request, which is passed as the x-goog-request-reason"
                              + " header.")
                      .build())));

  private BigQueryJdbcUrlUtility() {}

  /**
   * Parses a URI property from the given URI.
   *
   * @param uri The URI to parse.
   * @param property The name of the property to parse.
   * @return The String value of the property, or the default value if the property is not found.
   */
  static String parseUriProperty(String uri, String property) {
    try {
      Map<String, String> props = parseUrlProperties(uri);

      for (Map.Entry<String, String> entry : props.entrySet()) {
        if (entry.getKey().equalsIgnoreCase(property)) {
          return entry.getValue();
        }
      }
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new BigQueryJdbcRuntimeException(e.getMessage());
    }
    return null;
  }

  /**
   * Appends the given properties to the given URL.
   *
   * @param url The URL to append the properties to.
   * @param properties The properties to append.
   * @return The string value of the updated URL.
   */
  static String appendPropertiesToURL(String url, String callerClassName, Properties properties) {
    LOG.finest("++enter++  " + callerClassName);
    StringBuilder urlBuilder = new StringBuilder(url);
    for (Entry<Object, Object> entry : properties.entrySet()) {
      if (entry.getValue() != null && !"".equals(entry.getValue())) {
        LOG.finest("Appending %s with value %s to URL", entry.getKey(), entry.getValue());
        urlBuilder.append(";").append(entry.getKey()).append("=").append(entry.getValue());
      }
    }
    return urlBuilder.toString();
  }

  public static String parsePartnerTokenProperty(String url, String callerClassName) {
    LOG.finest("++enter++\t" + callerClassName);
    // This property is expected to be set by partners only. For more details on exact format
    // supported, refer b/396086960
    String regex =
        PARTNER_TOKEN_PROPERTY_NAME + "=\\s*\\(\\s*(GPN:[^;]*?)\\s*(?:;\\s*([^)]*?))?\\s*\\)";
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(url);

    if (matcher.find()) {
      String gpnPart = matcher.group(1);
      String environmentPart = matcher.group(2);
      StringBuilder partnerToken = new StringBuilder(" (");
      partnerToken.append(gpnPart);
      if (environmentPart != null && !environmentPart.trim().isEmpty()) {
        partnerToken.append("; ");
        partnerToken.append(environmentPart);
      }
      partnerToken.append(")");
      return partnerToken.toString();
    }
    return null;
  }

  public static Level parseLogLevel(String logLevelString) {
    int logLevel = logLevelString != null ? Integer.parseInt(logLevelString) : DEFAULT_LOG_LEVEL;
    switch (logLevel) {
      case 8:
        return Level.ALL;
      case 7:
        return Level.FINEST;
      case 6:
        return Level.FINER;
      case 5:
        return Level.FINE;
      case 4:
        return Level.CONFIG;
      case 3:
        return Level.INFO;
      case 2:
        return Level.WARNING;
      case 1:
        return Level.SEVERE;
      case 0:
      default:
        LOG.info("%s value not provided, defaulting to %s.", LOG_LEVEL_PROPERTY_NAME, Level.OFF);
        return Level.OFF;
    }
  }

  static Map<String, String> parseOverridePropertiesString(String overridePropertiesString) {
    Map<String, String> overrideProps = new HashMap<>();
    for (String property : OVERRIDE_PROPERTIES) {
      Pattern propertyPattern = Pattern.compile(String.format("(?i)%s=(.*?)(?:[,;]|$)", property));
      Matcher propertyMatcher = propertyPattern.matcher(overridePropertiesString);
      if (propertyMatcher.find() && propertyMatcher.groupCount() >= 1) {
        overrideProps.put(property, propertyMatcher.group(1));
      }
    }
    return overrideProps;
  }

  private static boolean convertStrToIntBoolean(String value) {
    if ("1".equals(value)) return true;
    if ("0".equals(value)) return false;
    return Boolean.parseBoolean(value);
  }

  public static Map<String, String> parseUrlProperties(String url) throws IllegalArgumentException {
    Map<String, String> properties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    if (url == null || url.isEmpty()) {
      return properties;
    }

    int sep = url.indexOf(';');
    if (sep == -1) {
      return properties;
    }

    String[] parts = url.substring(sep + 1).split(";");
    for (String part : parts) {
      if (part.trim().isEmpty()) {
        continue;
      }
      int eqIdx = part.indexOf('=');
      if (eqIdx != -1) {
        String key = part.substring(0, eqIdx).trim();
        String val = part.substring(eqIdx + 1).trim();
        properties.put(key, CharEscapers.decodeUriPath(val));
      }
    }
    return properties;
  }

  private static final java.util.Map<String, java.util.function.BiConsumer<DataSource, String>>
      PROPERTY_SETTERS = new java.util.HashMap<>();

  static {
    PROPERTY_SETTERS.put("projectid", (ds, val) -> ds.setProjectId(val));
    PROPERTY_SETTERS.put("defaultdataset", (ds, val) -> ds.setDefaultDataset(val));
    PROPERTY_SETTERS.put("oauthtype", (ds, val) -> ds.setOAuthType(Integer.parseInt(val)));
    PROPERTY_SETTERS.put(
        "enablehighthroughputapi",
        (ds, val) -> ds.setEnableHighThroughputAPI(convertStrToIntBoolean(val)));
    PROPERTY_SETTERS.put(
        "highthroughputmintablesize",
        (ds, val) -> ds.setHighThroughputMinTableSize(Integer.parseInt(val)));
    PROPERTY_SETTERS.put(
        "highthroughputactivationratio",
        (ds, val) -> ds.setHighThroughputActivationRatio(Integer.parseInt(val)));
    PROPERTY_SETTERS.put(
        "unsupportedhtapifallback",
        (ds, val) -> ds.setUnsupportedHTAPIFallback(convertStrToIntBoolean(val)));
    PROPERTY_SETTERS.put("kmskeyname", (ds, val) -> ds.setKmsKeyName(val));
    PROPERTY_SETTERS.put(
        "queryproperties",
        (ds, val) -> {
          Map<String, String> querypropertiesMap = new java.util.HashMap<>();
          for (String kv : val.split(",")) {
            String[] kvParts = kv.split("=");
            if (kvParts.length == 2) {
              querypropertiesMap.put(kvParts[0].trim(), kvParts[1].trim());
            }
          }
          ds.setQueryProperties(querypropertiesMap);
        });
    PROPERTY_SETTERS.put("loglevel", (ds, val) -> ds.setLogLevel(val));
    PROPERTY_SETTERS.put(
        "enablesession", (ds, val) -> ds.setEnableSession(convertStrToIntBoolean(val)));
    PROPERTY_SETTERS.put("logpath", (ds, val) -> ds.setLogPath(val));
    PROPERTY_SETTERS.put("oauthserviceacctemail", (ds, val) -> ds.setOAuthServiceAcctEmail(val));
    PROPERTY_SETTERS.put("oauthpvtkeypath", (ds, val) -> ds.setOAuthPvtKeyPath(val));
    PROPERTY_SETTERS.put("oauthpvtkey", (ds, val) -> ds.setOAuthPvtKey(val));
    PROPERTY_SETTERS.put("oauthaccesstoken", (ds, val) -> ds.setOAuthAccessToken(val));
    PROPERTY_SETTERS.put("oauthrefreshtoken", (ds, val) -> ds.setOAuthRefreshToken(val));
    PROPERTY_SETTERS.put(
        "usequerycache", (ds, val) -> ds.setUseQueryCache(convertStrToIntBoolean(val)));
    PROPERTY_SETTERS.put("querydialect", (ds, val) -> ds.setQueryDialect(val));
    PROPERTY_SETTERS.put(
        "allowlargeresults", (ds, val) -> ds.setAllowLargeResults(convertStrToIntBoolean(val)));
    PROPERTY_SETTERS.put("largeresulttable", (ds, val) -> ds.setDestinationTable(val));
    PROPERTY_SETTERS.put("largeresultdataset", (ds, val) -> ds.setDestinationDataset(val));
    PROPERTY_SETTERS.put(
        "largeresultsdatasetexpirationtime",
        (ds, val) -> ds.setDestinationDatasetExpirationTime(Long.parseLong(val)));
    PROPERTY_SETTERS.put("universedomain", (ds, val) -> ds.setUniverseDomain(val));
    PROPERTY_SETTERS.put("proxyhost", (ds, val) -> ds.setProxyHost(val));
    PROPERTY_SETTERS.put("proxyport", (ds, val) -> ds.setProxyPort(val));
    PROPERTY_SETTERS.put("proxyuid", (ds, val) -> ds.setProxyUid(val));
    PROPERTY_SETTERS.put("proxypwd", (ds, val) -> ds.setProxyPwd(val));
    PROPERTY_SETTERS.put("oauthclientid", (ds, val) -> ds.setOAuthClientId(val));
    PROPERTY_SETTERS.put("oauthclientsecret", (ds, val) -> ds.setOAuthClientSecret(val));
    PROPERTY_SETTERS.put(
        "jobcreationmode", (ds, val) -> ds.setJobCreationMode(Integer.parseInt(val)));
    PROPERTY_SETTERS.put("maxresults", (ds, val) -> ds.setMaxResults(Long.parseLong(val)));
    PROPERTY_SETTERS.put("partnertoken", (ds, val) -> ds.setPartnerToken(val));
    PROPERTY_SETTERS.put(
        "enablewriteapi", (ds, val) -> ds.setEnableWriteAPI(convertStrToIntBoolean(val)));
    PROPERTY_SETTERS.put("additionalprojects", (ds, val) -> ds.setAdditionalProjects(val));
    PROPERTY_SETTERS.put(
        "filtertablesondefaultdataset",
        (ds, val) -> ds.setFilterTablesOnDefaultDataset(convertStrToIntBoolean(val)));
    PROPERTY_SETTERS.put(
        "requestgoogledrivescope",
        (ds, val) -> ds.setRequestGoogleDriveScope(Integer.parseInt(val)));
    PROPERTY_SETTERS.put(
        "metadatafetchthreadcount",
        (ds, val) -> ds.setMetadataFetchThreadCount(Integer.parseInt(val)));
    PROPERTY_SETTERS.put("ssltruststore", (ds, val) -> ds.setSSLTrustStorePath(val));
    PROPERTY_SETTERS.put("ssltruststorepwd", (ds, val) -> ds.setSSLTrustStorePassword(val));
    PROPERTY_SETTERS.put(
        "labels",
        (ds, val) -> {
          Map<String, String> labelsMap = new java.util.HashMap<>();
          for (String kv : val.split(",")) {
            String[] kvParts = kv.split("=");
            if (kvParts.length == 2) {
              labelsMap.put(kvParts[0].trim(), kvParts[1].trim());
            }
          }
          ds.setLabels(labelsMap);
        });
    PROPERTY_SETTERS.put("requestreason", (ds, val) -> ds.setRequestReason(val));
    PROPERTY_SETTERS.put(
        "maximumbytesbilled", (ds, val) -> ds.setMaximumBytesBilled(Long.parseLong(val)));
    PROPERTY_SETTERS.put("timeout", (ds, val) -> ds.setRetryTimeoutInSecs(Long.parseLong(val)));
    PROPERTY_SETTERS.put("jobtimeout", (ds, val) -> ds.setJobTimeout(Long.parseLong(val)));
    PROPERTY_SETTERS.put(
        "retryinitialdelay", (ds, val) -> ds.setRetryInitialDelayInSecs(Long.parseLong(val)));
    PROPERTY_SETTERS.put(
        "retrymaxdelay", (ds, val) -> ds.setRetryMaxDelayInSecs(Long.parseLong(val)));
    PROPERTY_SETTERS.put(
        "httpconnecttimeout", (ds, val) -> ds.setHttpConnectTimeout(Integer.parseInt(val)));
    PROPERTY_SETTERS.put(
        "httpreadtimeout", (ds, val) -> ds.setHttpReadTimeout(Integer.parseInt(val)));
    PROPERTY_SETTERS.put(
        "swa_appendrowcount", (ds, val) -> ds.setSwaAppendRowCount(Integer.parseInt(val)));
    PROPERTY_SETTERS.put(
        "swa_activationrowcount", (ds, val) -> ds.setSwaActivationRowCount(Integer.parseInt(val)));
    PROPERTY_SETTERS.put(
        "connectionpoolsize", (ds, val) -> ds.setConnectionPoolSize(Long.parseLong(val)));
    PROPERTY_SETTERS.put(
        "listenerpoolsize", (ds, val) -> ds.setListenerPoolSize(Long.parseLong(val)));
    PROPERTY_SETTERS.put("location", (ds, val) -> ds.setLocation(val));
  }

  public static void setDataSourceProperties(DataSource ds, Map<String, String> urlProps)
      throws IllegalArgumentException {
    for (Map.Entry<String, String> entry : urlProps.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      java.util.function.BiConsumer<DataSource, String> setter =
          PROPERTY_SETTERS.get(key.toLowerCase());
      if (setter != null) {
        setter.accept(ds, val);
      } else {
        boolean found =
            BYOID_PROPERTIES.stream().anyMatch(key::equalsIgnoreCase)
                || OVERRIDE_PROPERTIES.stream().anyMatch(key::equalsIgnoreCase);
        if (!found
            && !key.equalsIgnoreCase("Location")
            && !key.equalsIgnoreCase("OAuthType")
            && !key.equalsIgnoreCase("ProjectId")
            && !key.equalsIgnoreCase("ServiceAccountImpersonationEmail")
            && !key.equalsIgnoreCase("ServiceAccountImpersonationScopes")
            && !key.equalsIgnoreCase("EndpointOverrides")
            && !key.equalsIgnoreCase("ServiceAccountImpersonationTokenLifetime")
            && !key.equalsIgnoreCase("universeDomain")) {
          throw new IllegalArgumentException("Unknown connection property: " + key);
        }
      }
    }
  }
}
