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

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.cloud.storage.StorageException;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestHttpStatusCode
{
    private static HttpResponseException http(final int code)
    {
        return new HttpResponseException.Builder(code, "fake", new HttpHeaders()).build();
    }

    @Test
    public void credentialFailureWrappedWithCodeZero()
    {
        // Current Google client: StorageException(code 0) -> IOException(auth) -> HttpResponseException 400
        StorageException ex = new StorageException(0, "Error getting access token", new IOException("auth", http(400)));
        assertEquals(400, GcsAuthentication.httpStatusCode(ex));
    }

    @Test
    public void storageExceptionWithCode()
    {
        assertEquals(404, GcsAuthentication.httpStatusCode(new StorageException(404, "Not Found")));
        assertEquals(503, GcsAuthentication.httpStatusCode(new StorageException(503, "Unavailable")));
    }

    @Test
    public void plainHttpResponseException()
    {
        assertEquals(401, GcsAuthentication.httpStatusCode(http(401)));
    }

    @Test
    public void noHttpStatusAnywhere()
    {
        assertEquals(0, GcsAuthentication.httpStatusCode(new RuntimeException("boom", new IOException("io"))));
        assertEquals(0, GcsAuthentication.httpStatusCode(null));
    }
}
