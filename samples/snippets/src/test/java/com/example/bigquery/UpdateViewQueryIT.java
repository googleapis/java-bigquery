/*
 * Copyright 2020 Google LLC
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

package com.example.bigquery;

import static com.google.common.truth.Truth.assertThat;
import static junit.framework.TestCase.assertNotNull;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class UpdateViewQueryIT {

  private final Logger log = Logger.getLogger(this.getClass().getName());
  private String tableName;
  private String viewName;
  private ByteArrayOutputStream bout;
  private PrintStream out;
  private PrintStream originalPrintStream;

  private static final String BIGQUERY_DATASET_NAME = requireEnvVar("BIGQUERY_DATASET_NAME");

  private static String requireEnvVar(String varName) {
    String value = System.getenv(varName);
    assertNotNull(
        "Environment variable " + varName + " is required to perform these tests.",
        System.getenv(varName));
    return value;
  }

  @BeforeClass
  public static void checkRequirements() {
    requireEnvVar("BIGQUERY_DATASET_NAME");
  }

  @Before
  public void setUp() {
    bout = new ByteArrayOutputStream();
    out = new PrintStream(bout);
    originalPrintStream = System.out;
    System.setOut(out);

    tableName = "MY_TABLE_NAME_TEST_" + UUID.randomUUID().toString().substring(0, 8);
    viewName = "MY_VIEW_NAME_TEST_" + UUID.randomUUID().toString().substring(0, 8);

    // create a test table.
    Schema schema =
        Schema.of(
            Field.of("timestampField", StandardSQLTypeName.TIMESTAMP),
            Field.of("stringField", StandardSQLTypeName.STRING),
            Field.of("booleanField", StandardSQLTypeName.BOOL));
    CreateTable.createTable(BIGQUERY_DATASET_NAME, tableName, schema);

    // create a test view
    String query =
        String.format(
            "SELECT timestampField, stringField, booleanField FROM %s.%s",
            BIGQUERY_DATASET_NAME, tableName);
    CreateView.createView(BIGQUERY_DATASET_NAME, viewName, query);
  }

  @After
  public void tearDown() {
    // Clean up
    DeleteTable.deleteTable(BIGQUERY_DATASET_NAME, viewName);
    DeleteTable.deleteTable(BIGQUERY_DATASET_NAME, tableName);
    // restores print statements in the original method
    System.out.flush();
    System.setOut(originalPrintStream);
    log.log(Level.INFO, "\n" + bout.toString());
  }

  @Test
  public void testUpdateViewQuery() {
    String updateQuery =
        String.format(
            "SELECT TimestampField, StringField FROM %s.%s", BIGQUERY_DATASET_NAME, tableName);
    UpdateViewQuery.updateViewQuery(BIGQUERY_DATASET_NAME, viewName, updateQuery);
    assertThat(bout.toString()).contains("View query updated successfully");
  }
}
