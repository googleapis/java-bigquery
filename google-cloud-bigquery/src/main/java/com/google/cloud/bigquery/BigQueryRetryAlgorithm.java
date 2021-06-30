/*
 * Copyright 2021 Google LLC
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google LLC nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.google.cloud.bigquery;

import com.google.api.gax.retrying.RetryAlgorithm;
import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.api.gax.retrying.TimedRetryAlgorithm;
import com.google.api.gax.retrying.ResultRetryAlgorithmWithContext;
import com.google.api.gax.retrying.TimedRetryAlgorithmWithContext;
import com.google.api.gax.retrying.RetryingContext;
import com.google.api.gax.retrying.TimedAttemptSettings;

import java.util.concurrent.CancellationException;

import static com.google.common.base.Preconditions.checkNotNull;

public class BigQueryRetryAlgorithm<ResponseT> extends RetryAlgorithm<ResponseT> {
    private final BigQueryRetryConfig bigQueryRetryConfig;
    private final ResultRetryAlgorithm<ResponseT> resultAlgorithm;
    private final TimedRetryAlgorithm timedAlgorithm;
    private final ResultRetryAlgorithmWithContext<ResponseT> resultAlgorithmWithContext;
    private final TimedRetryAlgorithmWithContext timedAlgorithmWithContext;

    public BigQueryRetryAlgorithm(ResultRetryAlgorithm<ResponseT> resultAlgorithm, TimedRetryAlgorithm timedAlgorithm, BigQueryRetryConfig bigQueryRetryConfig){
        super(resultAlgorithm, timedAlgorithm);
        this.bigQueryRetryConfig = checkNotNull(bigQueryRetryConfig);
        this.resultAlgorithm = checkNotNull(resultAlgorithm);
        this.timedAlgorithm = checkNotNull(timedAlgorithm);
        this.resultAlgorithmWithContext = null;
        this.timedAlgorithmWithContext = null;
    }

    @Override
    public boolean shouldRetry(
            RetryingContext context,
            Throwable previousThrowable,
            ResponseT previousResponse,
            TimedAttemptSettings nextAttemptSettings)
            throws CancellationException {
        return shouldRetryBasedOnResult(context, previousThrowable, previousResponse)//TODO - Use bigQueryRetryConfig to retry the error
                && shouldRetryBasedOnTiming(context, nextAttemptSettings);
    }

    /*Duplicating this method as it can not be inherited from the RetryAlgorithm due to the default access modifier*/
    boolean shouldRetryBasedOnResult(
            RetryingContext context, Throwable previousThrowable, ResponseT previousResponse) {
        if (resultAlgorithmWithContext != null && context != null) {
            return resultAlgorithmWithContext.shouldRetry(context, previousThrowable, previousResponse);
        }
        return getResultAlgorithm().shouldRetry(previousThrowable, previousResponse);
    }

    /*Duplicating this method as it can not be inherited from the RetryAlgorithm due to the private access modifier*/
    private boolean shouldRetryBasedOnTiming(
            RetryingContext context, TimedAttemptSettings nextAttemptSettings) {
        if (nextAttemptSettings == null) {
            return false;
        }
        if (timedAlgorithmWithContext != null && context != null) {
            return timedAlgorithmWithContext.shouldRetry(context, nextAttemptSettings);
        }
        return getTimedAlgorithm().shouldRetry(nextAttemptSettings);
    }
}
