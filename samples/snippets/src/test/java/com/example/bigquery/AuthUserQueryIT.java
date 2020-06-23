package com.example.bigquery;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AuthUserQueryIT {

  private ByteArrayOutputStream bout;
  private PrintStream out;

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
  public void testAuthUserFlow() {
    // TODO(stephaniewang526): Replace client_secret.json
    /*File credentialsPath = new File("\"path/to/your/client_secret.json");
    List<String> scopes = ImmutableList.of("https://www.googleapis.com/auth/bigquery");
    String query =
        "SELECT name, SUM(number) as total"
            + "  FROM `bigquery-public-data.usa_names.usa_1910_current`"
            + "  WHERE name = 'William'"
            + "  GROUP BY name;";
    AuthUserQuery.authUserQuery(credentialsPath, scopes, query);
    assertThat(bout.toString()).contains("Query performed successfully.");*/
  }
}
