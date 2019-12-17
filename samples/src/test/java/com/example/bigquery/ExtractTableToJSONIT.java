package com.example.bigquery;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.*;

import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
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
    String generatedDatasetName = RemoteBigQueryHelper.generateDatasetName();
    CreateDataset.createDataset(generatedDatasetName);

    ExtractTableToJSON.extractTableToJSON(generatedDatasetName, "my_table","CSV", "gs://my-bucket/extractTest.csv");
    assertThat(bout.toString())
        .contains("table extraction job completed successfully");
  }
}