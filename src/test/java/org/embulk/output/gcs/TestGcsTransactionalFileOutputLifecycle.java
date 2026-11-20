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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Buffer;
import org.embulk.util.config.units.LocalFile;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;

/**
 * File lifecycle against a real bucket, in the order Embulk drives it: nextFile/add/finish/close on
 * success, nextFile/add/abort/close when writing or finishing the file itself failed (an upstream
 * failure still finishes the file through the CSV formatter). Needs GCP_JSON_KEYFILE (service account
 * key path) and GCP_BUCKET; skips otherwise. Writes under a unique prefix and deletes it afterwards.
 */
public class TestGcsTransactionalFileOutputLifecycle
{
    private static String KEYFILE;
    private static String BUCKET;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private Storage client;
    private PluginTask task;
    private String prefix;

    @BeforeClass
    public static void readEnvironment()
    {
        KEYFILE = env("GCP_JSON_KEYFILE");
        BUCKET = env("GCP_BUCKET");
        assumeTrue(KEYFILE != null && BUCKET != null);
    }

    @Before
    public void setUp() throws Exception
    {
        prefix = "embulk-output-gcs-test/" + UUID.randomUUID() + "/data";
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", BUCKET)
                .set("path_prefix", prefix)
                .set("file_ext", ".csv")
                .set("auth_method", "json_key")
                .set("json_keyfile", Optional.of(LocalFile.ofContent(Files.readAllBytes(Paths.get(KEYFILE)))));
        task = CONFIG_MAPPER.map(config, PluginTask.class);
        client = new GcsOutputPlugin().createClient(task);
    }

    @After
    public void deleteTestObjects()
    {
        if (client == null) {
            return;
        }
        for (Blob blob : client.list(BUCKET, Storage.BlobListOption.prefix(prefix)).iterateAll()) {
            blob.delete();
        }
    }

    @Test
    public void successfulTaskWritesTheObjectAndNumbersItFromOne()
    {
        GcsTransactionalFileOutput output = new GcsTransactionalFileOutput(task, client, 0);
        output.nextFile();
        output.add(buffer("id,name\n1,alice\n"));
        output.finish();
        output.close();

        assertEquals("id,name\n1,alice\n", read(prefix + ".000.01.csv"));
        assertNull(client.get(BlobId.of(BUCKET, prefix + ".000.00.csv")));
    }

    @Test
    public void secondFileInTheSameTaskGetsTheNextNumber()
    {
        GcsTransactionalFileOutput output = new GcsTransactionalFileOutput(task, client, 3);
        output.nextFile();
        output.add(buffer("first"));
        output.nextFile();
        output.add(buffer("second"));
        output.finish();
        output.close();

        assertEquals("first", read(prefix + ".003.01.csv"));
        assertEquals("second", read(prefix + ".003.02.csv"));
    }

    @Test
    public void failureBeforeFinishLeavesThePreviousRunsObjectUntouched()
    {
        String name = prefix + ".000.01.csv";
        client.create(BlobInfo.newBuilder(BUCKET, name).build(), "previous run".getBytes(StandardCharsets.UTF_8));

        GcsTransactionalFileOutput output = new GcsTransactionalFileOutput(task, client, 0);
        output.nextFile();
        // More than one upload chunk, so part of the data has really been sent to the session
        // before the task fails.
        output.add(buffer(repeat('x', 20 * 1024 * 1024)));
        output.abort();
        output.close();

        assertEquals("previous run", read(name));
    }

    private Buffer buffer(String text)
    {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        Buffer buffer = runtime.getBufferAllocator().allocate(bytes.length);
        buffer.setBytes(0, bytes, 0, bytes.length);
        buffer.limit(bytes.length);
        return buffer;
    }

    private String read(String name)
    {
        return new String(client.readAllBytes(BlobId.of(BUCKET, name)), StandardCharsets.UTF_8);
    }

    private static String repeat(char c, int length)
    {
        char[] chars = new char[length];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    // GitHub Actions expands a missing repository secret to an empty string, which must count as "not set".
    private static String env(String name)
    {
        return Optional.ofNullable(System.getenv(name)).filter(value -> !value.isEmpty()).orElse(null);
    }
}
