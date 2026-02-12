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

package com.google.cloud.bigquery.jdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertNull;

import java.util.Map;
import java.util.Properties;
import org.junit.Test;

public class BigQueryJdbcUrlUtilityTest {

  @Test
  public void testParsePropertyWithNoDefault() {
    String url =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "ProjectId=MyBigQueryProject;"
            + "OAuthAccessToken=RedactedToken";

    String result = BigQueryJdbcUrlUtility.parseUriProperty(url, "OAuthType");
    assertThat(result).isNull();
  }

  @Test
  public void testParsePropertyWithDefault() {
    String url =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "ProjectId=MyBigQueryProject;"
            + "OAuthAccessToken=RedactedToken";

    String result = BigQueryJdbcUrlUtility.parseUriProperty(url, "OAuthType");
    assertThat(result).isEqualTo(null);
  }

  @Test
  public void testParsePropertyWithValue() {
    String url =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "ProjectId=MyBigQueryProject;"
            + "OAuthAccessToken=RedactedToken";

    String result = BigQueryJdbcUrlUtility.parseUriProperty(url, "ProjectId");
    assertThat(result).isEqualTo("MyBigQueryProject");
  }

  @Test
  public void testParsePropertyWithValueCaseInsensitive() {
    String url =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "PROJECTID=MyBigQueryProject;"
            + "OAuthAccessToken=RedactedToken";

    String result = BigQueryJdbcUrlUtility.parseUriProperty(url, "ProjectId");
    assertThat(result).isEqualTo("MyBigQueryProject");
  }

  @Test
  public void testAppendPropertiesToURL() {
    String url =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "ProjectId=MyBigQueryProject;"
            + "OAuthAccessToken=RedactedToken";
    Properties properties = new Properties();
    properties.setProperty("OAuthType", "3");

    String updatedUrl = BigQueryJdbcUrlUtility.appendPropertiesToURL(url, null, properties);
    assertThat(updatedUrl.contains("OAuthType=3"));
  }

  @Test
  public void testConnectionPropertiesFromURI() {
    String connection_uri =
        "bigquery://https://www.googleapis.com/bigquery/v2:443;PROJECTID=testProject;OAUTHTYPE=3;DEFAULTDATASET=testDataset;LOCATION=us-central1";

    assertThat(BigQueryJdbcUrlUtility.parseUriProperty(connection_uri, "OAUTHTYPE")).isEqualTo("3");
    assertThat(BigQueryJdbcUrlUtility.parseUriProperty(connection_uri, "LOCATION"))
        .isEqualTo("us-central1");
  }

  @Test
  public void testConnectionPropertiesFromURIMultiline() {
    String connection_uri =
        "bigquery://https://www.googleapis.com/bigquery/v2:443;AdditionalProjects=value1\n"
            + "value2\n"
            + ";";

    assertThat(BigQueryJdbcUrlUtility.parseUriProperty(connection_uri, "AdditionalProjects"))
        .isEqualTo("value1\nvalue2");
  }

  @Test
  public void testConnectionPropertiesFromURIMultilineNoSemicolon() {
    String connection_uri =
        "bigquery://https://www.googleapis.com/bigquery/v2:443;AdditionalProjects=value1\nvalue2";

    assertThat(BigQueryJdbcUrlUtility.parseUriProperty(connection_uri, "AdditionalProjects"))
        .isEqualTo("value1\nvalue2");
  }

  @Test
  public void testParseOverridePropertiesString() {
    String overrideString =
        "BIGQUERY=https://bigquery-myprivateserver.p.googleapis.com,"
            + "READ_API=https://bigquerystorage-myprivateserver.p.googleapis.com:443";

    Map<String, String> parsed =
        BigQueryJdbcUrlUtility.parseOverridePropertiesString(overrideString);

    assertThat(parsed.get("BIGQUERY"))
        .isEqualTo("https://bigquery-myprivateserver.p.googleapis.com");
    assertThat(parsed.get("READ_API"))
        .isEqualTo("https://bigquerystorage-myprivateserver.p.googleapis.com:443");
  }

  @Test
  public void testParseOverridePropertiesString_AllTypes() {
    String overrideString =
        "BIGQUERY=https://bigquery-myprivateserver.p.googleapis.com,"
            + "READ_API=https://bigquerystorage-myprivateserver.p.googleapis.com:443,"
            + "OAUTH2=https://oauth2-myprivateserver.p.googleapis.com,"
            + "STS=https://sts-myprivateserver.p.googleapis.com";

    Map<String, String> parsed =
        BigQueryJdbcUrlUtility.parseOverridePropertiesString(overrideString);

    assertThat(parsed.get("BIGQUERY"))
        .isEqualTo("https://bigquery-myprivateserver.p.googleapis.com");
    assertThat(parsed.get("READ_API"))
        .isEqualTo("https://bigquerystorage-myprivateserver.p.googleapis.com:443");
    assertThat(parsed.get("OAUTH2")).isEqualTo("https://oauth2-myprivateserver.p.googleapis.com");
    assertThat(parsed.get("STS")).isEqualTo("https://sts-myprivateserver.p.googleapis.com");
  }

  @Test
  public void testParseOverridePropertiesString_CaseInsensitive() {
    String overrideString =
        "bigQuery=https://bigquery-myprivateserver.p.googleapis.com,"
            + "READ_API=https://bigquerystorage-myprivateserver.p.googleapis.com:443";

    Map<String, String> parsed =
        BigQueryJdbcUrlUtility.parseOverridePropertiesString(overrideString);

    assertThat(parsed.get("BIGQUERY"))
        .isEqualTo("https://bigquery-myprivateserver.p.googleapis.com");
    assertThat(parsed.get("READ_API"))
        .isEqualTo("https://bigquerystorage-myprivateserver.p.googleapis.com:443");
  }

  @Test
  public void testParseOverridePropertiesString_IgnoresUnknown() {
    String overrideString = "BIGQUERY=https://bq.example.com,UNKNOWN=https://unknown.example.com";

    Map<String, String> parsed =
        BigQueryJdbcUrlUtility.parseOverridePropertiesString(overrideString);

    assertThat(parsed.get("BIGQUERY")).isEqualTo("https://bq.example.com");
    assertThat(parsed).doesNotContainKey("UNKNOWN");
  }

  @Test
  public void testParsePartnerTokenProperty() {
    // Case with partner name and environment
    String url =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "PartnerToken=(GPN:partner_company; dev);ProjectId=MyBigQueryProject;";
    String expected = " (GPN:partner_company; dev)";
    String result =
        BigQueryJdbcUrlUtility.parsePartnerTokenProperty(url, "testParsePartnerTokenProperty");
    assertThat(result).isEqualTo(expected);

    // Case with only partner name
    url =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "PartnerToken=(GPN:another_partner);ProjectId=MyBigQueryProject;";
    expected = " (GPN:another_partner)";
    result = BigQueryJdbcUrlUtility.parsePartnerTokenProperty(url, "testParsePartnerTokenProperty");
    assertThat(result).isEqualTo(expected);

    // Case when PartnerToken property is not present
    url =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "ProjectId=MyBigQueryProject;";
    result = BigQueryJdbcUrlUtility.parsePartnerTokenProperty(url, "testParsePartnerTokenProperty");
    assertNull(result);

    // Case when PartnerToken property is present but empty
    url = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PartnerToken=();";
    result = BigQueryJdbcUrlUtility.parsePartnerTokenProperty(url, "testParsePartnerTokenProperty");
    assertNull(result);

    // Case when PartnerToken property is present but without partner name
    url = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;PartnerToken=(env);";
    result = BigQueryJdbcUrlUtility.parsePartnerTokenProperty(url, "testParsePartnerTokenProperty");
    assertNull(result);

    // Case with extra spaces around the values
    url =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "PartnerToken= ( GPN: partner_name ; test_env ) ;";
    expected = " (GPN: partner_name; test_env)";
    result = BigQueryJdbcUrlUtility.parsePartnerTokenProperty(url, "testParsePartnerTokenProperty");
    assertThat(result).isEqualTo(expected);
  }
}
