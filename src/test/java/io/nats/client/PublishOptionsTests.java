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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Base64;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.Test;

import io.nats.client.ConnectionListener.Events;
import io.nats.client.impl.DataPort;
import io.nats.client.utils.CloseOnUpgradeAttempt;

public class PublishOptionsTests {
    @Test
    public void testDefaultOptions() {
        PublishOptions o = new PublishOptions.Builder().build();

        assertEquals(PublishOptions.unspecifiedStream, o.getStream(), "default stream");
        assertEquals(PublishOptions.defaultTimeout, o.getTimeout(), "default timeout");
    }

    @Test
    public void testChainedOptions() {
        PublishOptions o = new PublishOptions.Builder().stream("foo").timeout(Duration.ofSeconds(99)).build();

        assertEquals("foo", o.getStream(), "default stream");
        assertEquals(Duration.ofSeconds(99), o.getTimeout(), "default timeout");
    }

    @Test
    public void testProperties() {
        Properties p = new Properties();
        p.setProperty(PublishOptions.PROP_PUBLISH_TIMEOUT, "PT20M");
        p.setProperty(PublishOptions.PROP_STREAM_NAME, "foo");
        PublishOptions o = new PublishOptions.Builder(p).build();
        assertEquals("foo", o.getStream(), "stream foo");
        assertEquals(Duration.ofMinutes(20), o.getTimeout(), "20M timeout");

    }
}