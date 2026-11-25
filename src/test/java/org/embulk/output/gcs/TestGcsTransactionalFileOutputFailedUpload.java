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

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.sun.net.httpserver.HttpServer;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.spi.Buffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * After an upload fails, Embulk's formatter still flushes and finishes the file while closing. Those
 * calls must be no-ops: no second error, no "uploaded" log, nothing in the task report. The real
 * client talks to a local endpoint that accepts the resumable-session start and answers every chunk
 * upload with 503, so no credentials are needed.
 */
public class TestGcsTransactionalFileOutputFailedUpload
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private HttpServer endpoint;
    private final AtomicInteger chunkUploads = new AtomicInteger();

    @Before
    public void startEndpoint() throws IOException
    {
        endpoint = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        endpoint.createContext("/", exchange -> {
            try (InputStream body = exchange.getRequestBody()) {
                byte[] sink = new byte[64 * 1024];
                while (body.read(sink) != -1) {
                    // drain, so the client sees the status instead of a broken pipe
                }
            }
            if ("POST".equals(exchange.getRequestMethod())) {
                // Resumable session start: hand out the session URL.
                exchange.getResponseHeaders().add("Location", baseUrl() + "/upload/session");
                exchange.sendResponseHeaders(200, -1);
            }
            else {
                chunkUploads.incrementAndGet();
                exchange.sendResponseHeaders(503, -1);
            }
            exchange.close();
        });
        endpoint.start();
    }

    @After
    public void stopEndpoint()
    {
        endpoint.stop(0);
    }

    @Test
    public void flushAndFinishAfterAFailedUploadAreIgnored()
    {
        Storage client = StorageOptions.newBuilder()
                .setHost(baseUrl())
                .setProjectId("none")
                .setCredentials(NoCredentials.getInstance())
                .setRetrySettings(StorageOptions.getDefaultRetrySettings().toBuilder().setMaxAttempts(1).build())
                .build().getService();
        GcsTransactionalFileOutput output = new GcsTransactionalFileOutput(task(), client, 0);
        output.nextFile();

        // More than one upload chunk, so the client sends data and gets the 503.
        try {
            output.add(buffer(20 * 1024 * 1024));
            fail("the upload should have failed");
        }
        catch (RuntimeException expected) {
            assertEquals(1, chunkUploads.get());
        }

        // What Embulk does next while closing the formatter and the output.
        output.add(buffer(16));
        output.abort();
        output.finish();
        output.close();

        TaskReport report = output.commit();
        List<?> files = report.get(List.class, "files");
        assertTrue("nothing was uploaded, so nothing is reported: " + files, files.isEmpty());
        assertEquals("the abandoned file was not touched again", 1, chunkUploads.get());
    }

    private String baseUrl()
    {
        return "http://127.0.0.1:" + endpoint.getAddress().getPort();
    }

    private Buffer buffer(int length)
    {
        char[] chars = new char[length];
        Arrays.fill(chars, 'x');
        byte[] bytes = new String(chars).getBytes(StandardCharsets.UTF_8);
        Buffer buffer = runtime.getBufferAllocator().allocate(bytes.length);
        buffer.setBytes(0, bytes, 0, bytes.length);
        buffer.limit(bytes.length);
        return buffer;
    }

    private static PluginTask task()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", "test-bucket")
                .set("path_prefix", "data")
                .set("file_ext", ".csv");
        return CONFIG_MAPPER.map(config, PluginTask.class);
    }
}
