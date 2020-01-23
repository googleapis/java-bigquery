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
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RelaxTableQueryIT {
  private ByteArrayOutputStream bout;
  private PrintStream out;

  private static final String BIGQUERY_PROJECT_ID = System.getenv("BIGQUERY_PROJECT_ID");
  private static final String BIGQUERY_DATASET_NAME = System.getenv("BIGQUERY_DATASET_NAME");
  private static final String TABLE_NAME =
      "RELAX_TABLE_QUERY_TEST_" + UUID.randomUUID().toString().replace('-', '_');

  private static void requireEnvVar(String varName) {
    assertNotNull(
        "Environment variable " + varName + " is required to perform these tests.",
        System.getenv(varName));
  }

  @BeforeClass
  public static void checkRequirements() {
    requireEnvVar("BIGQUERY_PROJECT_ID");
    requireEnvVar("BIGQUERY_DATASET_NAME");
  }

  @Before
  public void setUp() throws Exception {
    System.out.println("BIGQUERY_DATASET_NAME: " + BIGQUERY_DATASET_NAME);
    System.out.println("TABLE_NAME: " + TABLE_NAME);

    bout = new ByteArrayOutputStream();
    out = new PrintStream(bout);

    CreateTable.createTable(
        BIGQUERY_DATASET_NAME,
        TABLE_NAME,
        Schema.of(
            Field.newBuilder("word", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
            Field.newBuilder("word_count", LegacySQLTypeName.STRING)
                .setMode(Field.Mode.REQUIRED)
                .build(),
            Field.newBuilder("corpus", LegacySQLTypeName.STRING)
                .setMode(Field.Mode.REQUIRED)
                .build(),
            Field.newBuilder("corpus_date", LegacySQLTypeName.STRING)
                .setMode(Field.Mode.REQUIRED)
                .build()));
    System.setOut(out);
  }

  @After
  public void tearDown() {
    DeleteTable.deleteTable(BIGQUERY_DATASET_NAME, TABLE_NAME);
    System.setOut(null);
  }

  @Test
  public void testRelaxTableQuery() throws Exception {
    RelaxTableQuery.relaxTableQuery(BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_NAME, TABLE_NAME);
    assertThat(bout.toString())
        .contains("Successfully relaxed all columns in destination table during query job");
  }
}
