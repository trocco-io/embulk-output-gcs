/*
 * Copyright 2026 The Embulk project
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
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.units.LocalFile;
import org.junit.Rule;
import org.junit.Test;

import java.net.UnknownHostException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Optional;

import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER;
import static org.embulk.output.gcs.GcsOutputPlugin.CONFIG_MAPPER_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNoException;

/**
 * Real end-to-end check of how a rejected credential is handled, against Google's real token endpoint.
 *
 * A syntactically valid service account key for an account that does not exist is sent to
 * https://oauth2.googleapis.com/token, which answers 400 invalid_grant. The current Google client reports that as
 * StorageException(code 0) wrapping the HTTP 400, and the plugin must treat it as a configuration error (4xx):
 * fail immediately with ConfigException, without burning the max_connection_retry attempts.
 *
 * Before the status lookup through the cause chain was added, this call retried max_connection_retry times
 * (about 170 s with the defaults) and then surfaced as a RuntimeException instead of a ConfigException.
 * The retry interval is set long on purpose so that any retry would show up as elapsed time.
 * Needs network access; skips itself when the token endpoint cannot be resolved.
 */
public class TestGcsAuthenticationRejectedCredential
{
    // One attempt takes well under 10 s even on a slow machine (measured ~4.5 s on an x86_64 JDK under Rosetta:
    // class loading of the bundled client dominates). A single wrongly retried attempt would add this whole wait.
    private static final int RETRY_WAIT_MILLIS = 20000;
    private static final int MAX_RETRY = 3;

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void rejectedCredentialIsConfigErrorAndNotRetried() throws Exception
    {
        ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("type", "gcs")
                .set("bucket", "embulk-output-gcs-test-nonexistent-bucket")
                .set("path_prefix", "test")
                .set("file_ext", ".csv")
                .set("auth_method", "json_key")
                .set("json_keyfile", Optional.of(LocalFile.ofContent(unregisteredServiceAccountKey())))
                .set("application_name", "embulk-output-gcs-test")
                .set("max_connection_retry", MAX_RETRY)
                .set("initial_retry_interval_millis", RETRY_WAIT_MILLIS)
                .set("maximum_retry_interval_millis", RETRY_WAIT_MILLIS);
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        long startedAt = System.currentTimeMillis();
        try {
            new GcsAuthentication(task).getGcsClient();
            fail("a credential Google does not know must not produce a client");
        }
        catch (ConfigException expected) {
            long elapsedMillis = System.currentTimeMillis() - startedAt;
            assertEquals("the HTTP status behind the failure", 400, GcsAuthentication.httpStatusCode(expected));
            assertTrue("a 4xx must not be retried, but it took " + elapsedMillis + " ms (one retry wait is " + RETRY_WAIT_MILLIS + " ms)",
                    elapsedMillis < RETRY_WAIT_MILLIS);
        }
        catch (RuntimeException other) {
            if (hasCause(other, UnknownHostException.class)) {
                assumeNoException("no network access to oauth2.googleapis.com", other);
            }
            throw other;
        }
    }

    /** A well-formed service account key whose RSA key pair is generated here, so Google cannot know the account. */
    private static byte[] unregisteredServiceAccountKey() throws Exception
    {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        // The key goes into a JSON string, so line breaks are written as the two characters '\\' 'n' (JSON escape).
        String base64 = Base64.getEncoder().encodeToString(keyPair.getPrivate().getEncoded());
        StringBuilder pem = new StringBuilder("-----BEGIN PRIVATE KEY-----\\n");
        for (int i = 0; i < base64.length(); i += 64) {
            pem.append(base64, i, Math.min(i + 64, base64.length())).append("\\n");
        }
        pem.append("-----END PRIVATE KEY-----\\n");
        String json = "{"
                + "\"type\":\"service_account\","
                + "\"project_id\":\"embulk-output-gcs-test\","
                + "\"private_key_id\":\"test\","
                + "\"private_key\":\"" + pem + "\","
                + "\"client_email\":\"unregistered@embulk-output-gcs-test.iam.gserviceaccount.com\","
                + "\"client_id\":\"1\","
                + "\"token_uri\":\"https://oauth2.googleapis.com/token\""
                + "}";
        return json.getBytes("UTF-8");
    }

    private static boolean hasCause(final Throwable throwable, final Class<? extends Throwable> type)
    {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }
}
