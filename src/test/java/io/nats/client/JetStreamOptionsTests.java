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
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsJetStreamConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamOptionsTests extends TestBase {

    @Test
    public void testBuilder() {
        // default
        JetStreamOptions jso = JetStreamOptions.defaultOptions();
        assertNull(jso.getRequestTimeout());
        assertEquals(DEFAULT_API_PREFIX, jso.getPrefix());
        assertTrue(jso.isDefaultPrefix());
        assertFalse(jso.isPublishNoAck());
        assertFalse(jso.isOptOut290ConsumerCreate());

        // default copy
        jso = JetStreamOptions.builder(jso).build();
        assertNull(jso.getRequestTimeout());
        assertEquals(DEFAULT_API_PREFIX, jso.getPrefix());
        assertTrue(jso.isDefaultPrefix());
        assertFalse(jso.isPublishNoAck());
        assertFalse(jso.isOptOut290ConsumerCreate());

        // affirmative
        jso = JetStreamOptions.builder()
            .prefix("pre")
            .requestTimeout(Duration.ofSeconds(42))
            .publishNoAck(true)
            .optOut290ConsumerCreate(true)
            .build();
        assertEquals(Duration.ofSeconds(42), jso.getRequestTimeout());
        assertEquals("pre.", jso.getPrefix());
        assertFalse(jso.isDefaultPrefix());
        assertTrue(jso.isPublishNoAck());
        assertTrue(jso.isOptOut290ConsumerCreate());

        // affirmative copy
        jso = JetStreamOptions.builder(jso).build();
        assertEquals(Duration.ofSeconds(42), jso.getRequestTimeout());
        assertEquals("pre.", jso.getPrefix());
        assertFalse(jso.isDefaultPrefix());
        assertTrue(jso.isPublishNoAck());
        assertTrue(jso.isOptOut290ConsumerCreate());

        // variations / coverage
        jso = JetStreamOptions.builder()
            .prefix("pre.")
            .publishNoAck(false)
            .optOut290ConsumerCreate(false)
            .build();
        assertNull(jso.getRequestTimeout());
        assertEquals("pre.", jso.getPrefix());
        assertFalse(jso.isDefaultPrefix());
        assertFalse(jso.isPublishNoAck());
        assertFalse(jso.isOptOut290ConsumerCreate());

        // variations / coverage copy
        jso = JetStreamOptions.builder(jso).build();
        assertNull(jso.getRequestTimeout());
        assertEquals("pre.", jso.getPrefix());
        assertFalse(jso.isDefaultPrefix());
        assertFalse(jso.isPublishNoAck());
        assertFalse(jso.isOptOut290ConsumerCreate());
    }

    @Test
    public void testPrefixValidation() {

        assertDefaultPrefix(null);
        assertDefaultPrefix("");
        assertDefaultPrefix(" ");

        assertValidPrefix(PLAIN);
        assertValidPrefix(PLAIN + DOT);
        assertValidPrefix(HAS_PRINTABLE);
        assertValidPrefix(HAS_DOT);
        assertValidPrefix(HAS_DASH);
        assertValidPrefix(HAS_UNDER);
        assertValidPrefix(HAS_DOLLAR);
        assertValidPrefix(HAS_FWD_SLASH);
        assertValidPrefix(HAS_EQUALS);
        assertValidPrefix(HAS_TIC);

        assertInvalidPrefix(HAS_SPACE);
        assertInvalidPrefix(HAS_STAR);
        assertInvalidPrefix(HAS_GT);
        assertInvalidPrefix(HAS_LOW);
        assertInvalidPrefix(HAS_127);

        assertInvalidPrefix(DOT);
        assertInvalidPrefix(DOT + PLAIN);
    }

    private void assertValidPrefix(String prefix) {
        JetStreamOptions jso = JetStreamOptions.builder().prefix(prefix).build();
        String expected = prefix.endsWith(DOT) ? prefix : prefix + DOT;
        assertEquals(expected, jso.getPrefix());
    }

    private void assertDefaultPrefix(String prefix) {
        JetStreamOptions jso = JetStreamOptions.builder().prefix(prefix).build();
        assertEquals(DEFAULT_API_PREFIX, jso.getPrefix());
    }

    private void assertInvalidPrefix(String prefix) {
        assertThrows(IllegalArgumentException.class, () -> JetStreamOptions.builder().prefix(prefix).build());
    }

    @Test
    public void testDomainValidation() {
        assertDefaultDomain(null);
        assertDefaultDomain("");
        assertDefaultDomain(" ");

        assertValidDomain(PLAIN);
        assertValidDomain(PLAIN + DOT);
        assertValidDomain(HAS_PRINTABLE);
        assertValidDomain(HAS_DOT);
        assertValidDomain(HAS_DASH);
        assertValidDomain(HAS_UNDER);
        assertValidDomain(HAS_DOLLAR);
        assertValidDomain(HAS_FWD_SLASH);
        assertValidDomain(HAS_EQUALS);
        assertValidDomain(HAS_TIC);

        assertInvalidDomain(HAS_SPACE);
        assertInvalidDomain(HAS_STAR);
        assertInvalidDomain(HAS_GT);
        assertInvalidDomain(HAS_LOW);
        assertInvalidDomain(HAS_127);

        assertInvalidDomain(DOT);
        assertInvalidDomain(DOT + PLAIN);
    }

    private void assertValidDomain(String domain) {
        JetStreamOptions jso = JetStreamOptions.builder().domain(domain).build();
        String prefixWithDot = domain.endsWith(DOT) ? domain : domain + DOT;
        String expected = PREFIX_DOLLAR_JS_DOT + prefixWithDot + PREFIX_API + DOT;
        assertEquals(expected, jso.getPrefix());
    }

    private void assertDefaultDomain(String domain) {
        JetStreamOptions jso = JetStreamOptions.builder().domain(domain).build();
        assertEquals(DEFAULT_API_PREFIX, jso.getPrefix());
    }

    private void assertInvalidDomain(String domain) {
        assertThrows(IllegalArgumentException.class, () -> JetStreamOptions.builder().domain(domain).build());
    }
}
