/*
 * Copyright 2026 Google LLC
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

/**
 * Utility class for storing and retrieving retry attempt count using thread-local storage.
 * This allows retry information to be accessed by HTTP/gRPC tracing interceptors during
 * retry operations.
 * 
 * <p>Thread-local storage is used because GAX retry framework executes retries on the same
 * thread, making this mechanism both simple and reliable.
 */
public class RetryContext {

  /**
   * Thread-local storage for the current retry attempt number.
   * Value 0 means the first attempt (no retries yet).
   * Value 1 means first retry (second attempt total).
   * Value N means Nth retry (N+1 attempts total).
   */
  private static final ThreadLocal<Integer> RETRY_ATTEMPT = ThreadLocal.withInitial(() -> 0);

  /**
   * Stores the retry attempt count in thread-local storage.
   * 
   * @param attemptCount The attempt number (0 = first attempt, 1 = first retry, etc.)
   */
  public static void setRetryAttempt(int attemptCount) {
    RETRY_ATTEMPT.set(attemptCount);
  }

  /**
   * Retrieves the retry attempt count from thread-local storage.
   * 
   * @return The retry attempt count, or 0 if not set (meaning first attempt)
   */
  public static int getRetryAttempt() {
    Integer attemptCount = RETRY_ATTEMPT.get();
    return attemptCount != null ? attemptCount : 0;
  }

  /**
   * Clears the retry attempt count from thread-local storage.
   * Should be called when retry operations complete to prevent memory leaks.
   */
  public static void clearRetryAttempt() {
    RETRY_ATTEMPT.remove();
  }

  /**
   * Checks if the current thread is processing a retry (attempt count > 0).
   * 
   * @return true if this is a retry attempt, false if this is the first attempt
   */
  public static boolean isRetry() {
    return getRetryAttempt() > 0;
  }

  private RetryContext() {
    // Utility class, prevent instantiation
  }
}
