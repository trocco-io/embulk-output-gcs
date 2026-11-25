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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * delete_in_advance deletes the objects directly under the prefix and skips the directory entries the
 * client returns for sub-prefixes. The real client talks to a local endpoint that answers the listing
 * and records the deletes, so no credentials are needed.
 */
public class TestGcsOutputPluginDeleteFiles
{
    private static final String LISTING = "{\"kind\":\"storage#objects\","
            + "\"items\":["
            + "{\"kind\":\"storage#object\",\"bucket\":\"test-bucket\",\"name\":\"out/data.000.01.csv\",\"generation\":\"1\"},"
            + "{\"kind\":\"storage#object\",\"bucket\":\"test-bucket\",\"name\":\"out/data.001.01.csv\",\"generation\":\"1\"}],"
            + "\"prefixes\":[\"out/data_archive/\",\"out/data_empty/\"]}";

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private HttpServer endpoint;
    private final List<String> listQueries = new ArrayList<>();
    private final List<String> deleted = new ArrayList<>();

    @Before
    public void startEndpoint() throws IOException
    {
        endpoint = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        endpoint.createContext("/", exchange -> {
            String path = URLDecoder.decode(exchange.getRequestURI().getRawPath(), "UTF-8");
            try (InputStream body = exchange.getRequestBody()) {
                byte[] sink = new byte[1024];
                while (body.read(sink) != -1) {
                    // drain
                }
            }
            // Close after each reply: on a reused connection the client did not receive the reply to the DELETE.
            exchange.getResponseHeaders().add("Connection", "close");
            if ("GET".equals(exchange.getRequestMethod()) && path.endsWith("/b/test-bucket/o")) {
                listQueries.add(URLDecoder.decode(exchange.getRequestURI().getRawQuery(), "UTF-8"));
                byte[] body = LISTING.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            }
            else if ("DELETE".equals(exchange.getRequestMethod())) {
                deleted.add(path.substring(path.indexOf("/b/test-bucket/o/") + "/b/test-bucket/o/".length()));
                // 200 rather than the service's 204: this server sends no response for a bodyless 204,
                // and the client accepts any 2xx.
                exchange.sendResponseHeaders(200, -1);
            }
            else {
                exchange.sendResponseHeaders(404, -1);
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
    public void onlyTheObjectsDirectlyUnderThePrefixAreDeleted()
    {
        Storage client = StorageOptions.newBuilder()
                .setHost("http://127.0.0.1:" + endpoint.getAddress().getPort())
                .setProjectId("none")
                .setCredentials(NoCredentials.getInstance())
                .setRetrySettings(StorageOptions.getDefaultRetrySettings().toBuilder().setMaxAttempts(1).build())
                .build().getService();

        new GcsOutputPlugin().deleteFiles(client, task());

        assertEquals(1, listQueries.size());
        assertTrue("lists with the prefix: " + listQueries.get(0), listQueries.get(0).contains("prefix=out/data"));
        assertTrue("stops at the next \"/\": " + listQueries.get(0), listQueries.get(0).contains("delimiter=/"));
        assertEquals("objects only, no directory entries",
                Arrays.asList("out/data.000.01.csv", "out/data.001.01.csv"), deleted);
    }

    private static PluginTask task()
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", "test-bucket")
                .set("path_prefix", "out/data")
                .set("file_ext", ".csv")
                .set("delete_in_advance", true);
        return CONFIG_MAPPER.map(config, PluginTask.class);
    }
}
