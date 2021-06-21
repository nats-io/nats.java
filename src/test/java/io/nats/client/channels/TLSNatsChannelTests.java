// Copyright 2021 The NATS Authors
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

package io.nats.client.channels;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;

import javax.net.ssl.SSLContext;

import org.junit.jupiter.api.Test;

public class TLSNatsChannelTests {
    static class WrappedException extends RuntimeException {
    }

    @Test
    public void testTimeoutDuringConnect() throws Exception {
        NatsChannel wrapped = new BaseNatsChannel() {
            @Override
            public int read(ByteBuffer dst) throws IOException {
                try {
                    Thread.sleep(10_000);
                } catch (InterruptedException ex) {
                    throw new IOException(ex);
                }
                return 0;
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                try {
                    Thread.sleep(10_000);
                } catch (InterruptedException ex) {
                    throw new IOException(ex);
                }
                return 0;
            }
        };
        NatsChannel channel = TLSNatsChannel.factory(uri -> SSLContext.getDefault().createSSLEngine())
        .connect(
            new URI("tls://example.com"),
            Duration.ofSeconds(1),
            (u,t) -> wrapped);
        assertThrows(
            ConnectTimeoutException.class,
            () -> channel.upgradeToSecure(Duration.ofSeconds(1)));
    }

    @Test
    public void testExceptionDuringConnect() throws Exception {
        NatsChannel wrapped = new BaseNatsChannel() {
            @Override
            public int read(ByteBuffer dst) throws IOException {
                throw new WrappedException();
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                throw new WrappedException();
            }
        };
        NatsChannel channel = TLSNatsChannel.factory(uri -> SSLContext.getDefault().createSSLEngine())
            .connect(
                new URI("tls://example.com"),
                Duration.ofSeconds(1),
                (u,t) -> wrapped);
        assertThrows(
            WrappedException.class,
            () -> channel.upgradeToSecure(Duration.ofSeconds(5)));
    }
}