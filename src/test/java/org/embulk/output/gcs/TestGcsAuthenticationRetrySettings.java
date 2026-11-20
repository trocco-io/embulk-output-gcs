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
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;

import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertEquals;

/**
 * The client's retry budget is built from max_connection_retry, initial_retry_interval_millis and
 * maximum_retry_interval_millis, with the same shape as the plugin's former retry loop.
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
        assertEquals(Duration.ofMillis(11L * (30000 + GcsAuthentication.READ_TIMEOUT_MILLIS)), settings.getTotalTimeoutDuration());
    }

    @Test
    public void configuredValuesAreUsed()
    {
        RetrySettings settings = GcsAuthentication.retrySettings(task(3, 250, 4000));

        assertEquals(4, settings.getMaxAttempts());
        assertEquals(Duration.ofMillis(250), settings.getInitialRetryDelayDuration());
        assertEquals(Duration.ofMillis(4000), settings.getMaxRetryDelayDuration());
        assertEquals(Duration.ofMillis(4L * (4000 + GcsAuthentication.READ_TIMEOUT_MILLIS)), settings.getTotalTimeoutDuration());
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
