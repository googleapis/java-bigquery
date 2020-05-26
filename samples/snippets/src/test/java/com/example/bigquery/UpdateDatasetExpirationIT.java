/*
 * Copyright 2019 Google LLC
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

import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class UpdateDatasetExpirationIT {
  private ByteArrayOutputStream bout;
  private PrintStream out;

  private static final String GOOGLE_CLOUD_PROJECT = System.getenv("GOOGLE_CLOUD_PROJECT");

  private static void requireEnvVar(String varName) {
    assertNotNull(
        "Environment variable " + varName + " is required to perform these tests.",
        System.getenv(varName));
  }

  @BeforeClass
  public static void checkRequirements() {
    requireEnvVar("GOOGLE_CLOUD_PROJECT");
  }

  @Before
  public void setUp() throws Exception {
    bout = new ByteArrayOutputStream();
    out = new PrintStream(bout);
    System.setOut(out);
  }

  @After
  public void tearDown() {
    System.setOut(null);
  }

  @Test
  public void updateDatasetExpiration() {
    String generatedDatasetName = RemoteBigQueryHelper.generateDatasetName();
    // Create a dataset in order to modify its expiration
    CreateDataset.createDataset(generatedDatasetName);

    Long newExpiration = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
    // Modify dataset's expiration
    UpdateDatasetExpiration.updateDatasetExpiration(generatedDatasetName, newExpiration);
    assertThat(bout.toString()).contains("Dataset description updated successfully");

    // Clean up
    DeleteDataset.deleteDataset(GOOGLE_CLOUD_PROJECT, generatedDatasetName);
  }
}
