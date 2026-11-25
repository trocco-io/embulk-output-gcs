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

import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageRetryStrategy;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The wrapper reports exactly the failures the client's own strategy decides to retry.
 */
public class TestLoggingStorageRetryStrategy
{
    @Test
    public void reportsRetriedFailuresAndStaysSilentForPermanentOnes()
    {
        List<Throwable> reported = new ArrayList<>();
        LoggingStorageRetryStrategy strategy = new LoggingStorageRetryStrategy(
                StorageRetryStrategy.getUniformStorageRetryStrategy(), reported::add);
        StorageException transientError = new StorageException(503, "backend error");
        StorageException permanentError = new StorageException(403, "forbidden");

        assertTrue(strategy.getIdempotentHandler().shouldRetry(transientError, null));
        assertFalse(strategy.getIdempotentHandler().shouldRetry(permanentError, null));
        assertTrue(strategy.getNonidempotentHandler().shouldRetry(transientError, null));
        assertFalse(strategy.getNonidempotentHandler().shouldRetry(permanentError, null));

        assertEquals(Arrays.asList(transientError, transientError), reported);
    }

    @Test
    public void successfulResultsAreNotReported()
    {
        List<Throwable> reported = new ArrayList<>();
        LoggingStorageRetryStrategy strategy = new LoggingStorageRetryStrategy(
                StorageRetryStrategy.getUniformStorageRetryStrategy(), reported::add);

        assertFalse(strategy.getIdempotentHandler().shouldRetry(null, null));
        assertTrue(reported.isEmpty());
    }
}
