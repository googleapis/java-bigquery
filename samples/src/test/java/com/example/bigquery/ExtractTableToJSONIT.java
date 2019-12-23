package com.example.bigquery;

import static com.google.common.truth.Truth.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExtractTableToJSONIT {
  private ByteArrayOutputStream bout;
  private PrintStream out;

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
  public void testExtractTableToJSON() {
    // Extract table content to GCS in CSV format
    ExtractTableToJSON.extractTableToJSON("CSV", "gs://my-bucket/extractTest.csv");
    assertThat(bout.toString()).contains("Table extraction job completed successfully");
  }
}
