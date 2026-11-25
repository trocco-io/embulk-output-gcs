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
import org.embulk.util.config.units.LocalFile;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/**
 * delete_in_advance against a real bucket: only the objects directly under the prefix are deleted;
 * folder placeholders (zero-byte "name/" objects) and objects inside sub-folders stay. Needs
 * GCP_JSON_KEYFILE (service account key path) and GCP_BUCKET, skips otherwise; writes under a unique
 * prefix and deletes it afterwards.
 */
public class TestGcsOutputPluginDeleteFilesOnBucket
{
    private static String KEYFILE;
    private static String BUCKET;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private Storage client;
    private String base;

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
        base = "embulk-output-gcs-test/" + UUID.randomUUID() + "/";
        client = new GcsOutputPlugin().createClient(task(base));
    }

    @After
    public void deleteTestObjects()
    {
        for (Blob blob : client.list(BUCKET, Storage.BlobListOption.prefix(base)).iterateAll()) {
            blob.delete();
        }
    }

    @Test
    public void foldersWhoseNamesStartWithTheFileNamePrefixStay() throws Exception
    {
        String prefix = base + "out";
        create(prefix + ".000.01.csv", prefix + "_full/", prefix + "_full/x.csv", prefix + "_empty/", base + "other.csv");

        new GcsOutputPlugin().deleteFiles(task(prefix));

        assertEquals(Arrays.asList("other.csv", "out_empty/", "out_full/", "out_full/x.csv"), remaining());
    }

    @Test
    public void foldersUnderAFolderPrefixStay() throws Exception
    {
        String prefix = base + "data/";
        // The prefix folder's own placeholder is directly under the prefix and is deleted with the data.
        create(prefix, prefix + "data.000.01.csv", prefix + "archive/", prefix + "archive/old.csv", prefix + "empty/");

        new GcsOutputPlugin().deleteFiles(task(prefix));

        assertEquals(Arrays.asList("data/archive/", "data/archive/old.csv", "data/empty/"), remaining());
    }

    private void create(String... names)
    {
        for (String name : names) {
            byte[] content = name.endsWith("/") ? new byte[0] : "x\n".getBytes(StandardCharsets.UTF_8);
            client.create(BlobInfo.newBuilder(BlobId.of(BUCKET, name)).build(), content);
        }
    }

    // Names under the test prefix, relative to it, in the service's listing order.
    private List<String> remaining()
    {
        List<String> names = new ArrayList<>();
        for (Blob blob : client.list(BUCKET, Storage.BlobListOption.prefix(base)).iterateAll()) {
            names.add(blob.getName().substring(base.length()));
        }
        return names;
    }

    private static PluginTask task(String prefix) throws Exception
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", BUCKET)
                .set("path_prefix", prefix)
                .set("file_ext", ".csv")
                .set("auth_method", "json_key")
                .set("delete_in_advance", true)
                .set("json_keyfile", Optional.of(LocalFile.ofContent(Files.readAllBytes(Paths.get(KEYFILE)))));
        return CONFIG_MAPPER.map(config, PluginTask.class);
    }

    private static String env(String name)
    {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? null : value;
    }
}
