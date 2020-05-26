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

import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.Acl.Role;
import com.google.cloud.bigquery.Acl.User;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class UpdateDatasetAccessIT {
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
  public void updateDatasetAccess() {
    String generatedDatasetName = RemoteBigQueryHelper.generateDatasetName();
    // Create a dataset in order to modify its ACL
    CreateDataset.createDataset(generatedDatasetName);

    Acl newEntry = Acl.of(new User("sample.bigquery.dev@gmail.com"), Role.READER);
    // Modify dataset's ACL
    UpdateDatasetAccess.updateDatasetAccess(generatedDatasetName, newEntry);
    assertThat(bout.toString()).contains("Dataset Access Control updated successfully");

    // Clean up
    DeleteDataset.deleteDataset(GOOGLE_CLOUD_PROJECT, generatedDatasetName);
  }
}
