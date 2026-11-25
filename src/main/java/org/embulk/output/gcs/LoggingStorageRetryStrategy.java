/*
 * Copyright 2018 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.output.gcs;

import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.api.gax.retrying.ResultRetryAlgorithmWithContext;
import com.google.api.gax.retrying.RetryingContext;
import com.google.api.gax.retrying.TimedAttemptSettings;
import com.google.cloud.storage.StorageRetryStrategy;

import java.io.Serializable;
import java.util.concurrent.CancellationException;

/**
 * Reports every failure the client decides to retry, so transient errors show up in the job log.
 */
final class LoggingStorageRetryStrategy implements StorageRetryStrategy
{
    interface RetryListener extends Serializable
    {
        void onRetry(Throwable failure);
    }

    private final StorageRetryStrategy delegate;
    private final RetryListener listener;

    LoggingStorageRetryStrategy(StorageRetryStrategy delegate, RetryListener listener)
    {
        this.delegate = delegate;
        this.listener = listener;
    }

    @Override
    public ResultRetryAlgorithm<?> getIdempotentHandler()
    {
        return observe(delegate.getIdempotentHandler());
    }

    @Override
    public ResultRetryAlgorithm<?> getNonidempotentHandler()
    {
        return observe(delegate.getNonidempotentHandler());
    }

    private <R> ResultRetryAlgorithm<R> observe(ResultRetryAlgorithm<R> algorithm)
    {
        return new Observing<>(algorithm, listener);
    }

    private static final class Observing<R> implements ResultRetryAlgorithmWithContext<R>
    {
        private final ResultRetryAlgorithm<R> delegate;
        private final RetryListener listener;

        private Observing(ResultRetryAlgorithm<R> delegate, RetryListener listener)
        {
            this.delegate = delegate;
            this.listener = listener;
        }

        @Override
        public TimedAttemptSettings createNextAttempt(Throwable failure, R response, TimedAttemptSettings previous)
        {
            return delegate.createNextAttempt(failure, response, previous);
        }

        @Override
        @SuppressWarnings("unchecked")
        public TimedAttemptSettings createNextAttempt(RetryingContext context, Throwable failure, R response, TimedAttemptSettings previous)
        {
            if (delegate instanceof ResultRetryAlgorithmWithContext) {
                return ((ResultRetryAlgorithmWithContext<R>) delegate).createNextAttempt(context, failure, response, previous);
            }
            return delegate.createNextAttempt(failure, response, previous);
        }

        @Override
        public boolean shouldRetry(Throwable failure, R response) throws CancellationException
        {
            return report(failure, delegate.shouldRetry(failure, response));
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean shouldRetry(RetryingContext context, Throwable failure, R response) throws CancellationException
        {
            boolean retry = delegate instanceof ResultRetryAlgorithmWithContext
                    ? ((ResultRetryAlgorithmWithContext<R>) delegate).shouldRetry(context, failure, response)
                    : delegate.shouldRetry(failure, response);
            return report(failure, retry);
        }

        private boolean report(Throwable failure, boolean retry)
        {
            if (retry && failure != null) {
                listener.onRetry(failure);
            }
            return retry;
        }
    }
}
