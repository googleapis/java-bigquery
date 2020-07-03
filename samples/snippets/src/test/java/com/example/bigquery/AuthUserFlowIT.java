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

import com.google.cloud.storage.contrib.nio.CloudStorageFileSystem;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AuthUserFlowIT {

  private ByteArrayOutputStream bout;
  private PrintStream out;

  private static final String GCS_BUCKET = System.getenv("GCS_BUCKET");

  private static String requireEnvVar(String varName) {
    String value = System.getenv(varName);
    assertNotNull(
        "Environment variable " + varName + " is required to perform these tests.",
        System.getenv(varName));
    return value;
  }

  @BeforeClass
  public static void checkRequirements() {
    requireEnvVar("GCS_BUCKET");
  }

  @Before
  public void setUp() {
    bout = new ByteArrayOutputStream();
    out = new PrintStream(bout);
    System.setOut(out);
  }

  @After
  public void tearDown() {
    System.setOut(null);
  }

  @Test
  public void testAuthUserFlow() throws IOException {
    try (CloudStorageFileSystem fs = CloudStorageFileSystem.forBucket(GCS_BUCKET)) {
      Path credentialsPath = fs.getPath("client_secret.json");
      List<String> scopes = ImmutableList.of("https://www.googleapis.com/auth/bigquery");
      AuthUserFlow.authUserFlow(credentialsPath, scopes);
    }
    assertThat(bout.toString()).contains("Success! Dataset ID");
  }
}
