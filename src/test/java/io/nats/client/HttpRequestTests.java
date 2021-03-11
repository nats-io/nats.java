// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import io.nats.client.impl.Headers;

public class HttpRequestTests {
    @Test
    public void testDefaults() {
        HttpRequest request = new HttpRequest();
        assertEquals("GET", request.getMethod());
        assertEquals("/", request.getURI());
        assertEquals("1.0", request.getVersion());
        assertEquals(new Headers(), request.getHeaders());
        assertEquals(
            "GET / HTTP/1.0\r\n" +
            "\r\n",
            request.toString());
    }

    @Test
    public void testSetters() {
        HttpRequest request = new HttpRequest()
            .method("PUT")
            .uri("/fun")
            .version("1.1");
        request.getHeaders()
            .add("One", "1")
            .add("Two", "2");
        assertEquals("PUT", request.getMethod());
        assertEquals("/fun", request.getURI());
        assertEquals("1.1", request.getVersion());
        assertEquals(
            "PUT /fun HTTP/1.1\r\n" +
            "One: 1\r\n" +
            "Two: 2\r\n" +
            "\r\n",
            request.toString());
    }

    @Test
    public void testNulls() {
        HttpRequest request = new HttpRequest();
        assertThrows(IllegalArgumentException.class, () -> {
            request.method(null);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            request.uri(null);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            request.version(null);
        });
    }
}
