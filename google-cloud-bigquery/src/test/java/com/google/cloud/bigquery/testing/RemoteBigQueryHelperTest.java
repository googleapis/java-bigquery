/*
 * Copyright 2015 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigquery.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.http.HttpTransportOptions;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.threeten.bp.Duration;

@RunWith(MockitoJUnitRunner.class)
public class RemoteBigQueryHelperTest {

  private static final String DATASET_NAME = "dataset-name";
  private static final String PROJECT_ID = "project-id";
  private static final String JSON_KEY =
      "{\n"
          + "  \"private_key_id\": \"somekeyid\",\n"
          + "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\n"
          + "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC+K2hSuFpAdrJI\\n"
          + "nCgcDz2M7t7bjdlsadsasad+fvRSW6TjNQZ3p5LLQY1kSZRqBqylRkzteMOyHgaR\\n"
          + "0Pmxh3ILCND5men43j3h4eDbrhQBuxfEMalkG92sL+PNQSETY2tnvXryOvmBRwa/\\n"
          + "QP/9dJfIkIDJ9Fw9N4Bhhhp6mCcRpdQjV38H7JsyJ7lih/oNjECgYAt\\n"
          + "knddadwkwewcVxHFhcZJO+XWf6ofLUXpRwiTZakGMn8EE1uVa2LgczOjwWHGi99MFjxSer5m9\\n"
          + "1tCa3/KEGKiS/YL71JvjwX3mb+cewlkcmweBKZHM2JPTk0ZednFSpVZMtycjkbLa\\n"
          + "dYOS8V85AgMBewECggEBAKksaldajfDZDV6nGqbFjMiizAKJolr/M3OQw16K6o3/\\n"
          + "0S31xIe3sSlgW0+UbYlF4U8KifhManD1apVSC3csafaspP4RZUHFhtBywLO9pR5c\\n"
          + "r6S5aLp+gPWFyIp1pfXbWGvc5VY/v9x7ya1VEa6rXvLsKupSeWAW4tMj3eo/64ge\\n"
          + "sdaceaLYw52KeBYiT6+vpsnYrEkAHO1fF/LavbLLOFJmFTMxmsNaG0tuiJHgjshB\\n"
          + "82DpMCbXG9YcCgI/DbzuIjsdj2JC1cascSP//3PmefWysucBQe7Jryb6NQtASmnv\\n"
          + "CdDw/0jmZTEjpe4S1lxfHplAhHFtdgYTvyYtaLZiVVkCgYEA8eVpof2rceecw/I6\\n"
          + "5ng1q3Hl2usdWV/4mZMvR0fOemacLLfocX6IYxT1zA1FFJlbXSRsJMf/Qq39mOR2\\n"
          + "SpW+hr4jCoHeRVYLgsbggtrevGmILAlNoqCMpGZ6vDmJpq6ECV9olliDvpPgWOP+\\n"
          + "mYPDreFBGxWvQrADNbRt2dmGsrsCgYEAyUHqB2wvJHFqdmeBsaacewzV8x9WgmeX\\n"
          + "gUIi9REwXlGDW0Mz50dxpxcKCAYn65+7TCnY5O/jmL0VRxU1J2mSWyWTo1C+17L0\\n"
          + "3fUqjxL1pkefwecxwecvC+gFFYdJ4CQ/MHHXU81Lwl1iWdFCd2UoGddYaOF+KNeM\\n"
          + "HC7cmqra+JsCgYEAlUNywzq8nUg7282E+uICfCB0LfwejuymR93CtsFgb7cRd6ak\\n"
          + "ECR8FGfCpH8ruWJINllbQfcHVCX47ndLZwqv3oVFKh6pAS/vVI4dpOepP8++7y1u\\n"
          + "coOvtreXCX6XqfrWDtKIvv0vjlHBhhhp6mCcRpdQjV38H7JsyJ7lih/oNjECgYAt\\n"
          + "kndj5uNl5SiuVxHFhcZJO+XWf6ofLUregtevZakGMn8EE1uVa2AY7eafmoU/nZPT\\n"
          + "00YB0TBATdCbn/nBSuKDESkhSg9s2GEKQZG5hBmL5uCMfo09z3SfxZIhJdlerreP\\n"
          + "J7gSidI12N+EZxYd4xIJh/HFDgp7RRO87f+WJkofMQKBgGTnClK1VMaCRbJZPriw\\n"
          + "EfeFCoOX75MxKwXs6xgrw4W//AYGGUjDt83lD6AZP6tws7gJ2IwY/qP7+lyhjEqN\\n"
          + "HtfPZRGFkGZsdaksdlaksd323423d+15/UvrlRSFPNj1tWQmNKkXyRDW4IG1Oa2p\\n"
          + "rALStNBx5Y9t0/LQnFI4w3aG\\n"
          + "-----END PRIVATE KEY-----\\n"
          + "\",\n"
          + "  \"client_email\": \"someclientid@developer.gserviceaccount.com\",\n"
          + "  \"client_id\": \"someclientid.apps.googleusercontent.com\",\n"
          + "  \"type\": \"service_account\"\n"
          + "}";
  private static final InputStream JSON_KEY_STREAM = new ByteArrayInputStream(JSON_KEY.getBytes());

  @Test
  public void testForceDelete() throws InterruptedException, ExecutionException {
    BigQuery bigqueryMock = Mockito.mock(BigQuery.class);
    Mockito.when(bigqueryMock.delete(DATASET_NAME, DatasetDeleteOption.deleteContents()))
        .thenReturn(true);
    assertTrue(RemoteBigQueryHelper.forceDelete(bigqueryMock, DATASET_NAME));
    Mockito.verify(bigqueryMock).delete(DATASET_NAME, DatasetDeleteOption.deleteContents());
  }

  @Test
  public void testCreateFromStream() {
    RemoteBigQueryHelper helper = RemoteBigQueryHelper.create(PROJECT_ID, JSON_KEY_STREAM);
    BigQueryOptions options = helper.getOptions();
    assertEquals(PROJECT_ID, options.getProjectId());
    assertEquals(60000, ((HttpTransportOptions) options.getTransportOptions()).getConnectTimeout());
    assertEquals(60000, ((HttpTransportOptions) options.getTransportOptions()).getReadTimeout());
    assertEquals(10, options.getRetrySettings().getMaxAttempts());
    assertEquals(Duration.ofMillis(30000), options.getRetrySettings().getMaxRetryDelay());
    assertEquals(Duration.ofMillis(120000), options.getRetrySettings().getTotalTimeout());
    assertEquals(Duration.ofMillis(250), options.getRetrySettings().getInitialRetryDelay());
  }
}
