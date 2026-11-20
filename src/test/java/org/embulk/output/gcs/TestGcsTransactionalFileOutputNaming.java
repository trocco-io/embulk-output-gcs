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

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.junit.Rule;
import org.junit.Test;

import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertEquals;

/**
 * Object naming per task: "{path_prefix}{sequence_format}{file_ext}" with the task number first and a
 * file counter that starts at 1. No credentials needed: names are computed without touching the client.
 */
public class TestGcsTransactionalFileOutputNaming
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void firstFileOfATaskIsNumberOneWithTheDefaultFormat()
    {
        GcsTransactionalFileOutput output = new GcsTransactionalFileOutput(task("trocco/out/data", ".csv", null), null, 0);

        assertEquals("trocco/out/data.000.01.csv", output.nextBlobName());
        assertEquals("trocco/out/data.000.02.csv", output.nextBlobName());
    }

    @Test
    public void taskIndexIsTheFirstNumberAndFileCounterTheSecond()
    {
        GcsTransactionalFileOutput output = new GcsTransactionalFileOutput(task("trocco/out/data", ".csv", null), null, 5);

        assertEquals("trocco/out/data.005.01.csv", output.nextBlobName());
    }

    @Test
    public void customSequenceFormatGetsTheSameNumbers()
    {
        GcsTransactionalFileOutput output = new GcsTransactionalFileOutput(task("trocco/out/data", ".csv", "_%d_%d"), null, 2);

        assertEquals("trocco/out/data_2_1.csv", output.nextBlobName());
        assertEquals("trocco/out/data_2_2.csv", output.nextBlobName());
    }

    @Test
    public void leadingDotsAndSlashesAreStrippedAsBefore()
    {
        GcsTransactionalFileOutput output = new GcsTransactionalFileOutput(task("/trocco/out/data", ".csv", null), null, 0);

        assertEquals("trocco/out/data.000.01.csv", output.nextBlobName());
    }

    private static PluginTask task(String pathPrefix, String fileExt, String sequenceFormat)
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", "test-bucket")
                .set("path_prefix", pathPrefix)
                .set("file_ext", fileExt);
        if (sequenceFormat != null) {
            config.set("sequence_format", sequenceFormat);
        }
        return CONFIG_MAPPER.map(config, PluginTask.class);
    }
}
