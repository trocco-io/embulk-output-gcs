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

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageRetryStrategy;
import com.sun.net.httpserver.HttpServer;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The client's retry budget is built from max_connection_retry, initial_retry_interval_millis and
 * maximum_retry_interval_millis, with the same shape as the plugin's former retry loop: a number of
 * attempts and growing waits, no time cap.
 */
public class TestGcsAuthenticationRetrySettings
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void defaultsGiveTenRetriesWithWaitsDoublingFromHalfASecondToThirtySeconds()
    {
        RetrySettings settings = GcsAuthentication.retrySettings(task(null, null, null));

        assertEquals(11, settings.getMaxAttempts());
        assertEquals(Duration.ofMillis(500), settings.getInitialRetryDelayDuration());
        assertEquals(2.0, settings.getRetryDelayMultiplier(), 0.0);
        assertEquals(Duration.ofSeconds(30), settings.getMaxRetryDelayDuration());
        assertEquals(Duration.ZERO, settings.getTotalTimeoutDuration());
    }

    @Test
    public void configuredValuesAreUsed()
    {
        RetrySettings settings = GcsAuthentication.retrySettings(task(3, 250, 4000));

        assertEquals(4, settings.getMaxAttempts());
        assertEquals(Duration.ofMillis(250), settings.getInitialRetryDelayDuration());
        assertEquals(Duration.ofMillis(4000), settings.getMaxRetryDelayDuration());
        assertEquals(Duration.ZERO, settings.getTotalTimeoutDuration());
    }

    /**
     * Against the real client and an endpoint that answers 503 to everything: exactly max_connection_retry + 1
     * requests go out and the client's own budget summary names attempts only.
     */
    @Test
    public void theClientRetriesMaxConnectionRetryTimesWithNoTimeCap() throws IOException
    {
        AtomicInteger requests = new AtomicInteger();
        HttpServer endpoint = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        endpoint.createContext("/", exchange -> {
            requests.incrementAndGet();
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        });
        endpoint.start();
        try {
            List<Throwable> retried = new ArrayList<>();
            Storage client = StorageOptions.newBuilder()
                    .setHost("http://127.0.0.1:" + endpoint.getAddress().getPort())
                    .setProjectId("none")
                    .setCredentials(NoCredentials.getInstance())
                    .setRetrySettings(GcsAuthentication.retrySettings(task(2, 1, 2)))
                    .setStorageRetryStrategy(new LoggingStorageRetryStrategy(
                            StorageRetryStrategy.getUniformStorageRetryStrategy(), retried::add))
                    .build().getService();

            try {
                client.list("test-bucket", Storage.BlobListOption.pageSize(1));
                fail("the endpoint answers 503 to everything");
            }
            catch (StorageException ex) {
                assertEquals(503, ex.getCode());
                assertEquals("first attempt plus max_connection_retry retries", 3, requests.get());
                String budget = budgetSummary(ex);
                assertTrue(budget, budget.contains("attempts: 3, maxAttempts: 3"));
                assertFalse("no time cap: " + budget, budget.contains("timeout"));
            }
        }
        finally {
            endpoint.stop(0);
        }
    }

    // The client attaches its retry decision to the final error as a suppressed comment.
    private static String budgetSummary(StorageException ex)
    {
        return Arrays.stream(ex.getSuppressed())
                .map(Throwable::getMessage)
                .filter(message -> message != null && message.contains("maxAttempts"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no retry budget summary on " + ex));
    }

    private static PluginTask task(Integer maxConnectionRetry, Integer initialMillis, Integer maximumMillis)
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", "test-bucket")
                .set("path_prefix", "data")
                .set("file_ext", ".csv");
        if (maxConnectionRetry != null) {
            config.set("max_connection_retry", maxConnectionRetry);
        }
        if (initialMillis != null) {
            config.set("initial_retry_interval_millis", initialMillis);
        }
        if (maximumMillis != null) {
            config.set("maximum_retry_interval_millis", maximumMillis);
        }
        return CONFIG_MAPPER.map(config, PluginTask.class);
    }
}
