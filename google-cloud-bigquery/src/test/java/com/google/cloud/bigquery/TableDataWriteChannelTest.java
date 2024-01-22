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

package com.google.cloud.bigquery;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.spi.BigQueryRpcFactory;
import com.google.cloud.bigquery.spi.v2.BigQueryRpc;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TableDataWriteChannelTest {

  private static final String UPLOAD_ID = "uploadid";
  private static final TableId TABLE_ID = TableId.of("dataset", "table");
  private static final WriteChannelConfiguration LOAD_CONFIGURATION =
      WriteChannelConfiguration.newBuilder(TABLE_ID)
          .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
          .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
          .setFormatOptions(FormatOptions.json())
          .setIgnoreUnknownValues(true)
          .setMaxBadRecords(10)
          .build();
  private static final int MIN_CHUNK_SIZE = 256 * 1024;
  private static final int DEFAULT_CHUNK_SIZE = 60 * MIN_CHUNK_SIZE;
  private static final int CUSTOM_CHUNK_SIZE = 4 * MIN_CHUNK_SIZE;
  private static final Random RANDOM = new Random();
  private static final LoadJobConfiguration JOB_CONFIGURATION =
      LoadJobConfiguration.of(TABLE_ID, "URI");
  private static final JobInfo JOB_INFO = JobInfo.of(JobId.of(), JOB_CONFIGURATION);

  private static final String FAKE_JSON_CRED =
      "{\n"
          + "  \"private_key_id\": \"somekeyid\",\n"
          + "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggS"
          + "kAgEAAoIBAQC+K2hSuFpAdrJI\\nnCgcDz2M7t7bjdlsadsasad+fvRSW6TjNQZ3p5LLQY1kSZRqBqylRkzteMOyHg"
          + "aR\\n0Pmxh3ILCND5men43j3h4eDbrhQBuxfEMalkG92sL+PNQSETY2tnvXryOvmBRwa/\\nQP/9dJfIkIDJ9Fw9N4"
          + "Bhhhp6mCcRpdQjV38H7JsyJ7lih/oNjECgYAt\\nknddadwkwewcVxHFhcZJO+XWf6ofLUXpRwiTZakGMn8EE1uVa2"
          + "LgczOjwWHGi99MFjxSer5m9\\n1tCa3/KEGKiS/YL71JvjwX3mb+cewlkcmweBKZHM2JPTk0ZednFSpVZMtycjkbLa"
          + "\\ndYOS8V85AgMBewECggEBAKksaldajfDZDV6nGqbFjMiizAKJolr/M3OQw16K6o3/\\n0S31xIe3sSlgW0+UbYlF"
          + "4U8KifhManD1apVSC3csafaspP4RZUHFhtBywLO9pR5c\\nr6S5aLp+gPWFyIp1pfXbWGvc5VY/v9x7ya1VEa6rXvL"
          + "sKupSeWAW4tMj3eo/64ge\\nsdaceaLYw52KeBYiT6+vpsnYrEkAHO1fF/LavbLLOFJmFTMxmsNaG0tuiJHgjshB\\"
          + "n82DpMCbXG9YcCgI/DbzuIjsdj2JC1cascSP//3PmefWysucBQe7Jryb6NQtASmnv\\nCdDw/0jmZTEjpe4S1lxfHp"
          + "lAhHFtdgYTvyYtaLZiVVkCgYEA8eVpof2rceecw/I6\\n5ng1q3Hl2usdWV/4mZMvR0fOemacLLfocX6IYxT1zA1FF"
          + "JlbXSRsJMf/Qq39mOR2\\nSpW+hr4jCoHeRVYLgsbggtrevGmILAlNoqCMpGZ6vDmJpq6ECV9olliDvpPgWOP+\\nm"
          + "YPDreFBGxWvQrADNbRt2dmGsrsCgYEAyUHqB2wvJHFqdmeBsaacewzV8x9WgmeX\\ngUIi9REwXlGDW0Mz50dxpxcK"
          + "CAYn65+7TCnY5O/jmL0VRxU1J2mSWyWTo1C+17L0\\n3fUqjxL1pkefwecxwecvC+gFFYdJ4CQ/MHHXU81Lwl1iWdF"
          + "Cd2UoGddYaOF+KNeM\\nHC7cmqra+JsCgYEAlUNywzq8nUg7282E+uICfCB0LfwejuymR93CtsFgb7cRd6ak\\nECR"
          + "8FGfCpH8ruWJINllbQfcHVCX47ndLZwqv3oVFKh6pAS/vVI4dpOepP8++7y1u\\ncoOvtreXCX6XqfrWDtKIvv0vjl"
          + "HBhhhp6mCcRpdQjV38H7JsyJ7lih/oNjECgYAt\\nkndj5uNl5SiuVxHFhcZJO+XWf6ofLUregtevZakGMn8EE1uVa"
          + "2AY7eafmoU/nZPT\\n00YB0TBATdCbn/nBSuKDESkhSg9s2GEKQZG5hBmL5uCMfo09z3SfxZIhJdlerreP\\nJ7gSi"
          + "dI12N+EZxYd4xIJh/HFDgp7RRO87f+WJkofMQKBgGTnClK1VMaCRbJZPriw\\nEfeFCoOX75MxKwXs6xgrw4W//AYG"
          + "GUjDt83lD6AZP6tws7gJ2IwY/qP7+lyhjEqN\\nHtfPZRGFkGZsdaksdlaksd323423d+15/UvrlRSFPNj1tWQmNKk"
          + "XyRDW4IG1Oa2p\\nrALStNBx5Y9t0/LQnFI4w3aG\\n-----END PRIVATE KEY-----\\n\",\n"
          + "  \"project_id\": \"someprojectid\",\n"
          + "  \"client_email\": \"someclientid@developer.gserviceaccount.com\",\n"
          + "  \"client_id\": \"someclientid.apps.googleusercontent.com\",\n"
          + "  \"type\": \"service_account\",\n"
          + "  \"universe_domain\": \"googleapis.com\"\n"
          + "}";

  private BigQueryOptions options;
  private BigQueryRpcFactory rpcFactoryMock;
  private BigQueryRpc bigqueryRpcMock;
  private BigQueryFactory bigqueryFactoryMock;
  private BigQuery bigqueryMock;
  private Job job;

  @Captor private ArgumentCaptor<byte[]> capturedBuffer;
  @Captor private ArgumentCaptor<Long> capturedPosition;

  private TableDataWriteChannel writer;

  static GoogleCredentials loadCredentials(String credentialFile) {
    try {
      InputStream keyStream = new ByteArrayInputStream(credentialFile.getBytes());
      return GoogleCredentials.fromStream(keyStream);
    } catch (IOException e) {
      fail("Couldn't create fake JSON credentials.");
    }
    return null;
  }

  @Before
  public void setUp() {
    rpcFactoryMock = mock(BigQueryRpcFactory.class);
    bigqueryRpcMock = mock(BigQueryRpc.class);
    bigqueryFactoryMock = mock(BigQueryFactory.class);
    bigqueryMock = mock(BigQuery.class);
    when(bigqueryMock.getOptions()).thenReturn(options);
    job = new Job(bigqueryMock, new JobInfo.BuilderImpl(JOB_INFO));
    when(rpcFactoryMock.create(any(BigQueryOptions.class))).thenReturn(bigqueryRpcMock);
    when(bigqueryFactoryMock.create(any(BigQueryOptions.class))).thenReturn(bigqueryMock);
    options =
        BigQueryOptions.newBuilder()
            .setProjectId("projectid")
            .setServiceRpcFactory(rpcFactoryMock)
            .setServiceFactory(bigqueryFactoryMock)
            .setCredentials(loadCredentials(FAKE_JSON_CRED))
            .build();
  }

  @Test
  public void testCreate() {
    when(bigqueryRpcMock.open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb())))
        .thenReturn(UPLOAD_ID);
    writer = new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION);
    assertTrue(writer.isOpen());
    assertNull(writer.getJob());
    verify(bigqueryRpcMock)
        .open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb()));
  }

  @Test
  public void testCreateRetryableError() {
    BigQueryException exception = new BigQueryException(new SocketException("Socket closed"));
    when(bigqueryRpcMock.open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb())))
        .thenThrow(exception)
        .thenReturn(UPLOAD_ID);
    writer = new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION);
    assertTrue(writer.isOpen());
    assertNull(writer.getJob());
    verify(bigqueryRpcMock, times(2))
        .open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb()));
  }

  @Test
  public void testCreateNonRetryableError() throws IOException {
    RuntimeException ex = new RuntimeException("expected");
    when(bigqueryRpcMock.open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb())))
        .thenThrow(ex);
    try (TableDataWriteChannel channel =
        new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION)) {
      Assert.fail();
    } catch (RuntimeException expected) {
      Assert.assertEquals("java.lang.RuntimeException: expected", expected.getMessage());
    }
    verify(bigqueryRpcMock)
        .open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb()));
  }

  @Test
  public void testWriteWithoutFlush() throws IOException {
    when(bigqueryRpcMock.open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb())))
        .thenReturn(UPLOAD_ID);
    writer = new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION);
    assertEquals(MIN_CHUNK_SIZE, writer.write(ByteBuffer.allocate(MIN_CHUNK_SIZE)));
    assertNull(writer.getJob());
    verify(bigqueryRpcMock)
        .open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb()));
  }

  @Test
  public void testWriteWithFlush() throws IOException {
    when(bigqueryRpcMock.open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb())))
        .thenReturn(UPLOAD_ID);
    when(bigqueryRpcMock.write(
            eq(UPLOAD_ID),
            capturedBuffer.capture(),
            eq(0),
            eq(0L),
            eq(CUSTOM_CHUNK_SIZE),
            eq(false)))
        .thenReturn(null);
    writer = new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION);
    writer.setChunkSize(CUSTOM_CHUNK_SIZE);
    ByteBuffer buffer = randomBuffer(CUSTOM_CHUNK_SIZE);
    assertEquals(CUSTOM_CHUNK_SIZE, writer.write(buffer));
    assertArrayEquals(buffer.array(), capturedBuffer.getValue());
    assertNull(writer.getJob());
    verify(bigqueryRpcMock)
        .open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb()));
    verify(bigqueryRpcMock)
        .write(
            eq(UPLOAD_ID),
            capturedBuffer.capture(),
            eq(0),
            eq(0L),
            eq(CUSTOM_CHUNK_SIZE),
            eq(false));
  }

  @Test
  public void testWritesAndFlush() throws IOException {
    when(bigqueryRpcMock.open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb())))
        .thenReturn(UPLOAD_ID);
    when(bigqueryRpcMock.write(
            eq(UPLOAD_ID),
            capturedBuffer.capture(),
            eq(0),
            eq(0L),
            eq(DEFAULT_CHUNK_SIZE),
            eq(false)))
        .thenReturn(null);
    writer = new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION);
    ByteBuffer[] buffers = new ByteBuffer[DEFAULT_CHUNK_SIZE / MIN_CHUNK_SIZE];
    for (int i = 0; i < buffers.length; i++) {
      buffers[i] = randomBuffer(MIN_CHUNK_SIZE);
      assertEquals(MIN_CHUNK_SIZE, writer.write(buffers[i]));
    }
    for (int i = 0; i < buffers.length; i++) {
      assertArrayEquals(
          buffers[i].array(),
          Arrays.copyOfRange(
              capturedBuffer.getValue(), MIN_CHUNK_SIZE * i, MIN_CHUNK_SIZE * (i + 1)));
    }
    assertNull(writer.getJob());
    verify(bigqueryRpcMock)
        .open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb()));
    verify(bigqueryRpcMock)
        .write(
            eq(UPLOAD_ID),
            capturedBuffer.capture(),
            eq(0),
            eq(0L),
            eq(DEFAULT_CHUNK_SIZE),
            eq(false));
  }

  @Test
  public void testCloseWithoutFlush() throws IOException {
    when(bigqueryRpcMock.open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb())))
        .thenReturn(UPLOAD_ID);
    when(bigqueryRpcMock.write(
            eq(UPLOAD_ID), capturedBuffer.capture(), eq(0), eq(0L), eq(0), eq(true)))
        .thenReturn(job.toPb());
    writer = new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION);
    assertTrue(writer.isOpen());
    writer.close();
    assertArrayEquals(new byte[0], capturedBuffer.getValue());
    assertTrue(!writer.isOpen());
    assertEquals(job, writer.getJob());
    verify(bigqueryRpcMock)
        .open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb()));
    verify(bigqueryRpcMock)
        .write(eq(UPLOAD_ID), capturedBuffer.capture(), eq(0), eq(0L), eq(0), eq(true));
  }

  @Test
  public void testCloseWithFlush() throws IOException {
    when(bigqueryRpcMock.open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb())))
        .thenReturn(UPLOAD_ID);
    ByteBuffer buffer = randomBuffer(MIN_CHUNK_SIZE);
    when(bigqueryRpcMock.write(
            eq(UPLOAD_ID), capturedBuffer.capture(), eq(0), eq(0L), eq(MIN_CHUNK_SIZE), eq(true)))
        .thenReturn(job.toPb());
    writer = new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION);
    assertTrue(writer.isOpen());
    writer.write(buffer);
    writer.close();
    assertEquals(DEFAULT_CHUNK_SIZE, capturedBuffer.getValue().length);
    assertArrayEquals(buffer.array(), Arrays.copyOf(capturedBuffer.getValue(), MIN_CHUNK_SIZE));
    assertTrue(!writer.isOpen());
    assertEquals(job, writer.getJob());
    verify(bigqueryRpcMock)
        .open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb()));
    verify(bigqueryRpcMock)
        .write(
            eq(UPLOAD_ID), capturedBuffer.capture(), eq(0), eq(0L), eq(MIN_CHUNK_SIZE), eq(true));
  }

  @Test
  public void testWriteClosed() throws IOException {
    when(bigqueryRpcMock.open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb())))
        .thenReturn(UPLOAD_ID);
    when(bigqueryRpcMock.write(
            eq(UPLOAD_ID), capturedBuffer.capture(), eq(0), eq(0L), eq(0), eq(true)))
        .thenReturn(job.toPb());
    writer = new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION);
    writer.close();
    assertEquals(job, writer.getJob());
    try {
      writer.write(ByteBuffer.allocate(MIN_CHUNK_SIZE));
      fail("Expected TableDataWriteChannel write to throw IOException");
    } catch (IOException ex) {
      // expected
    }
    verify(bigqueryRpcMock)
        .open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb()));
    verify(bigqueryRpcMock)
        .write(eq(UPLOAD_ID), capturedBuffer.capture(), eq(0), eq(0L), eq(0), eq(true));
  }

  @Test
  public void testSaveAndRestore() throws IOException {
    when(bigqueryRpcMock.open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb())))
        .thenReturn(UPLOAD_ID);
    when(bigqueryRpcMock.write(
            eq(UPLOAD_ID),
            capturedBuffer.capture(),
            eq(0),
            capturedPosition.capture(),
            eq(DEFAULT_CHUNK_SIZE),
            eq(false)))
        .thenReturn(null);
    ByteBuffer buffer1 = randomBuffer(DEFAULT_CHUNK_SIZE);
    ByteBuffer buffer2 = randomBuffer(DEFAULT_CHUNK_SIZE);
    writer = new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION);
    assertEquals(DEFAULT_CHUNK_SIZE, writer.write(buffer1));
    assertArrayEquals(buffer1.array(), capturedBuffer.getAllValues().get(0));
    assertEquals(new Long(0L), capturedPosition.getAllValues().get(0));
    assertNull(writer.getJob());
    RestorableState<WriteChannel> writerState = writer.capture();
    WriteChannel restoredWriter = writerState.restore();
    assertEquals(DEFAULT_CHUNK_SIZE, restoredWriter.write(buffer2));
    assertArrayEquals(buffer2.array(), capturedBuffer.getAllValues().get(1));
    assertEquals(new Long(DEFAULT_CHUNK_SIZE), capturedPosition.getAllValues().get(1));
    verify(bigqueryRpcMock)
        .open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb()));
    verify(bigqueryRpcMock, times(2))
        .write(
            eq(UPLOAD_ID),
            capturedBuffer.capture(),
            eq(0),
            capturedPosition.capture(),
            eq(DEFAULT_CHUNK_SIZE),
            eq(false));
  }

  @Test
  public void testSaveAndRestoreClosed() throws IOException {
    when(bigqueryRpcMock.open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb())))
        .thenReturn(UPLOAD_ID);
    when(bigqueryRpcMock.write(
            eq(UPLOAD_ID), capturedBuffer.capture(), eq(0), eq(0L), eq(0), eq(true)))
        .thenReturn(job.toPb());
    writer = new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION);
    writer.close();
    assertEquals(job, writer.getJob());
    RestorableState<WriteChannel> writerState = writer.capture();
    RestorableState<WriteChannel> expectedWriterState =
        TableDataWriteChannel.StateImpl.builder(options, LOAD_CONFIGURATION, UPLOAD_ID, job)
            .setBuffer(null)
            .setChunkSize(DEFAULT_CHUNK_SIZE)
            .setIsOpen(false)
            .setPosition(0)
            .build();
    WriteChannel restoredWriter = writerState.restore();
    assertArrayEquals(new byte[0], capturedBuffer.getValue());
    assertEquals(expectedWriterState, restoredWriter.capture());
    verify(bigqueryRpcMock)
        .open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb()));
    verify(bigqueryRpcMock)
        .write(eq(UPLOAD_ID), capturedBuffer.capture(), eq(0), eq(0L), eq(0), eq(true));
  }

  @Test
  public void testStateEquals() {
    when(bigqueryRpcMock.open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb())))
        .thenReturn(UPLOAD_ID);
    writer = new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION);
    // avoid closing when you don't want partial writes upon failure
    @SuppressWarnings("resource")
    WriteChannel writer2 =
        new TableDataWriteChannel(options, JOB_INFO.getJobId(), LOAD_CONFIGURATION);
    RestorableState<WriteChannel> state = writer.capture();
    RestorableState<WriteChannel> state2 = writer2.capture();
    assertEquals(state, state2);
    assertEquals(state.hashCode(), state2.hashCode());
    assertEquals(state.toString(), state2.toString());
    verify(bigqueryRpcMock, times(2))
        .open(
            new com.google.api.services.bigquery.model.Job()
                .setJobReference(JOB_INFO.getJobId().toPb())
                .setConfiguration(LOAD_CONFIGURATION.toPb()));
  }

  private static ByteBuffer randomBuffer(int size) {
    byte[] byteArray = new byte[size];
    RANDOM.nextBytes(byteArray);
    return ByteBuffer.wrap(byteArray);
  }
}
