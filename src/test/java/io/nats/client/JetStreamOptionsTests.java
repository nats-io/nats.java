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
    public void testAffirmative() {
        JetStreamOptions jso = JetStreamOptions.defaultOptions();
        assertEquals(DEFAULT_API_PREFIX, jso.getPrefix());
        assertEquals(Options.DEFAULT_CONNECTION_TIMEOUT, jso.getRequestTimeout());

        jso = JetStreamOptions.builder()
                .prefix("pre")
                .requestTimeout(Duration.ofSeconds(42))
                .build();
        assertEquals("pre.", jso.getPrefix());
        assertEquals(Duration.ofSeconds(42), jso.getRequestTimeout());
        assertFalse(jso.isPublishNoAck());

        jso = JetStreamOptions.builder()
                .prefix("pre.")
                .publishNoAck(true)
                .build();
        assertEquals("pre.", jso.getPrefix());
        assertTrue(jso.isPublishNoAck());
    }

    @Test
    public void testPrefixValidation() {

        assertDefaultPrefix(null);
        assertDefaultPrefix("");
        assertDefaultPrefix(" ");

        assertValidPrefix(PLAIN);
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
        String prefixWithDot = prefix.endsWith(DOT) ? prefix : prefix + DOT;
        assertEquals(prefixWithDot, jso.getPrefix());
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
        if (domain.startsWith(DOT)) {
            domain = domain.substring(1);
        }
        String prefixWithDot = domain.endsWith(DOT) ? domain : domain + DOT;
        String expected = PREFIX_DOLLAR_JS_DOT + prefixWithDot + PREFIX_API_DOT;
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
