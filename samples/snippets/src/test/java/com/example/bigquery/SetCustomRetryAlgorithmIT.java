/*
 * Copyright 2025 Google LLC
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

import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.cloud.ExceptionHandler;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SetCustomRetryAlgorithmIT {
  private final Logger log = Logger.getLogger(this.getClass().getName());
  private ByteArrayOutputStream bout;
  private PrintStream out;
  private PrintStream originalPrintStream;

  private static final String PROJECT_ID = requireEnvVar("GOOGLE_CLOUD_PROJECT");

  private static String requireEnvVar(String varName) {
    String value = System.getenv(varName);
    assertNotNull(
        "Environment variable " + varName + " is required to perform these tests.",
        System.getenv(varName));
    return value;
  }

  @Before
  public void setUp() throws Exception {
    bout = new ByteArrayOutputStream();
    out = new PrintStream(bout);
    originalPrintStream = System.out;
    System.setOut(out);
  }

  @After
  public void tearDown() {
    System.out.flush();
    System.setOut(originalPrintStream);
    log.log(Level.INFO, "\n" + bout.toString());
  }

  @Test
  public void testSetCustomRetryAlgorithm() {
    ResultRetryAlgorithm<?> retryAlgorithm =
        ExceptionHandler.newBuilder()
            .abortOn(RuntimeException.class)
            .retryOn(TimeoutException.class)
            .addInterceptors(SetCustomRetryAlgorithm.EXCEPTION_HANDLER_INTERCEPTOR)
            .build();

    SetCustomRetryAlgorithm.setCustomRetryAlgorithm(PROJECT_ID, retryAlgorithm);
    assertThat(bout.toString().contains("com.google.cloud.ExceptionHandler"));
  }
}
