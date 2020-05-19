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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.cloud.BaseServiceException;
import com.google.cloud.RetryHelper.RetryHelperException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryExceptionTest {

  @Test
  public void testBigQueryException() {
    BigQueryException exception = new BigQueryException(500, "message");
    assertEquals(500, exception.getCode());
    assertEquals("message", exception.getMessage());
    assertNull(exception.getReason());
    assertNull(exception.getError());
    assertTrue(exception.isRetryable());

    exception = new BigQueryException(502, "message");
    assertEquals(502, exception.getCode());
    assertEquals("message", exception.getMessage());
    assertNull(exception.getReason());
    assertNull(exception.getError());
    assertTrue(exception.isRetryable());

    exception = new BigQueryException(503, "message");
    assertEquals(503, exception.getCode());
    assertEquals("message", exception.getMessage());
    assertNull(exception.getReason());
    assertNull(exception.getError());
    assertTrue(exception.isRetryable());

    exception = new BigQueryException(504, "message");
    assertEquals(504, exception.getCode());
    assertEquals("message", exception.getMessage());
    assertNull(exception.getReason());
    assertNull(exception.getError());
    assertTrue(exception.isRetryable());

    exception = new BigQueryException(400, "message");
    assertEquals(400, exception.getCode());
    assertEquals("message", exception.getMessage());
    assertNull(exception.getReason());
    assertNull(exception.getError());
    assertFalse(exception.isRetryable());

    BigQueryError error = new BigQueryError("reason", null, null);
    exception = new BigQueryException(504, "message", error);
    assertEquals(504, exception.getCode());
    assertEquals("message", exception.getMessage());
    assertEquals("reason", exception.getReason());
    assertEquals(error, exception.getError());
    assertTrue(exception.isRetryable());

    IOException cause = new SocketTimeoutException("socketTimeoutMessage");
    exception = new BigQueryException(cause);
    assertEquals(BigQueryException.UNKNOWN_CODE, exception.getCode());
    assertNull(exception.getReason());
    assertEquals("socketTimeoutMessage", exception.getMessage());
    assertEquals(cause, exception.getCause());
    assertTrue(exception.isRetryable());
    assertSame(cause, exception.getCause());

    exception = new BigQueryException(504, "message", cause);
    assertEquals(504, exception.getCode());
    assertEquals("message", exception.getMessage());
    assertNull(exception.getReason());
    assertNull(exception.getError());
    assertTrue(exception.isRetryable());
    assertSame(cause, exception.getCause());

    HttpResponseException httpResponseException =
        new HttpResponseException.Builder(404, "Service Unavailable", new HttpHeaders()).build();
    exception = new BigQueryException(httpResponseException);
    assertEquals(404, exception.getCode());
    assertFalse(exception.isRetryable());

    httpResponseException = new HttpResponseException.Builder(504, null, new HttpHeaders()).build();
    exception = new BigQueryException(httpResponseException);
    assertEquals(504, exception.getCode());
    assertTrue(exception.isRetryable());

    httpResponseException = new HttpResponseException.Builder(503, null, new HttpHeaders()).build();
    exception = new BigQueryException(httpResponseException);
    assertEquals(503, exception.getCode());
    assertTrue(exception.isRetryable());

    httpResponseException = new HttpResponseException.Builder(502, null, new HttpHeaders()).build();
    exception = new BigQueryException(httpResponseException);
    assertEquals(502, exception.getCode());
    assertTrue(exception.isRetryable());

    httpResponseException = new HttpResponseException.Builder(500, null, new HttpHeaders()).build();
    exception = new BigQueryException(httpResponseException);
    assertEquals(500, exception.getCode());
    assertTrue(exception.isRetryable());
  }

  @Test
  public void testTranslateAndThrow() throws Exception {
    Exception cause = new BigQueryException(503, "message");
    RetryHelperException exceptionMock = mock(RetryHelperException.class);
    when(exceptionMock.getCause()).thenReturn(cause);
    try {
      BigQueryException.translateAndThrow(exceptionMock);
    } catch (BaseServiceException ex) {
      assertEquals(503, ex.getCode());
      assertEquals("message", ex.getMessage());
      assertTrue(ex.isRetryable());
    } finally {
      verify(exceptionMock, times(2)).getCause();
    }
    cause = new IllegalArgumentException("message");
    exceptionMock = mock(RetryHelperException.class);
    when(exceptionMock.getMessage()).thenReturn("message");
    when(exceptionMock.getCause()).thenReturn(cause);
    try {
      BigQueryException.translateAndThrow(exceptionMock);
    } catch (BaseServiceException ex) {
      assertEquals(BigQueryException.UNKNOWN_CODE, ex.getCode());
      assertEquals("message", ex.getMessage());
      assertFalse(ex.isRetryable());
      assertSame(cause, ex.getCause());
    } finally {
      verify(exceptionMock).getMessage();
      verify(exceptionMock, times(2)).getCause();
    }
  }
}
