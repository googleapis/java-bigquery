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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryWithArrayParametersIT {

  private final Logger log = Logger.getLogger(this.getClass().getName());
  private ByteArrayOutputStream bout;
  private PrintStream out;
  private PrintStream originalPrintStream;

  @Before
  public void setUp() {
    bout = new ByteArrayOutputStream();
    out = new PrintStream(bout);
    originalPrintStream = System.out;
    System.setOut(out);
  }

  @After
  public void tearDown() {
    // restores print statements in the original method
    System.out.flush();
    System.setOut(originalPrintStream);
    log.log(Level.INFO, "\n" + bout.toString());
  }

  @Test
  public void testQueryWithArrayParameters() {
    String gender = "M";
    String[] states = {"WA", "WI", "WV", "WY"};
    String query =
        "SELECT name, sum(number) as count\n"
            + "FROM `bigquery-public-data.usa_names.usa_1910_2013`\n"
            + "WHERE gender = @gender\n"
            + "AND state IN UNNEST(@states)\n"
            + "GROUP BY name\n"
            + "ORDER BY count DESC\n"
            + "LIMIT 10;";
    QueryWithArrayParameters.queryWithArrayParameters(query, gender, states);
    assertThat(bout.toString()).contains("Query with arrays parameters performed successfully");
  }
}
