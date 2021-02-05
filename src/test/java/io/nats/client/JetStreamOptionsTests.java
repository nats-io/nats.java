// Copyright 2020 The NATS Authors
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

import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamOptionsTests extends TestBase {

    @Test
    public void testAffirmative() {
        JetStreamOptions jso = JetStreamOptions.defaultOptions();
        assertNull(jso.getPrefix());
        Assertions.assertEquals(Options.DEFAULT_CONNECTION_TIMEOUT, jso.getRequestTimeout());

        jso = JetStreamOptions.builder()
                .prefix("pre")
                .requestTimeout(Duration.ofSeconds(42))
                .build();
        assertEquals("pre", jso.getPrefix());
        assertEquals(Duration.ofSeconds(42), jso.getRequestTimeout());
    }


    @Test
    public void testInvalidPrefix() {
        assertThrows(IllegalArgumentException.class, () -> JetStreamOptions.builder().prefix(">").build());
        assertThrows(IllegalArgumentException.class, () -> JetStreamOptions.builder().prefix("*").build());
    }
}