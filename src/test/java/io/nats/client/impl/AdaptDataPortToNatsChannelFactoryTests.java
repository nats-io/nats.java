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

package io.nats.client.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import io.nats.client.Options;
import io.nats.client.channels.NatsChannel;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AdaptDataPortToNatsChannelFactoryTests {
    enum Method {
        READ,
        WRITE,
        SHUTDOWN_INPUT,
        CLOSE,
        FLUSH;
    }

    @Test
    public void test() throws IOException, URISyntaxException {
        Method[] method = new Method[1];
        byte[][] buff = new byte[1][];
        NatsChannel natsChannel = new AdaptDataPortToNatsChannelFactory(
            () ->
            new DataPort() {
                boolean connected;

                @Override
                public void connect(String serverURI, NatsConnection conn, long timeoutNanos) throws IOException {
                    try {
                        assertEquals("nats://server:42", serverURI);
                        assertEquals(new URI("nats://options:42"), conn.getOptions().getServers().iterator().next());
                        connected = true;
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }

                @Override
                public void upgradeToSecure() throws IOException {
                    // Can't be called.
                }

                @Override
                public int read(byte[] dst, int off, int len) throws IOException {
                    method[0] = Method.READ;
                    int maxLen = buff[0].length;
                    if (len > maxLen) {
                        len = maxLen;
                    }
                    System.arraycopy(buff[0], 0, dst, off, len);
                    return len;
                }

                @Override
                public void write(byte[] src, int toWrite) throws IOException {
                    method[0] = Method.WRITE;
                    buff[0] = Arrays.copyOfRange(src, 0, toWrite);
                }

                @Override
                public void shutdownInput() throws IOException {
                    assertTrue(connected);
                    method[0] = Method.SHUTDOWN_INPUT;
                }

                @Override
                public void close() throws IOException {
                    method[0] = Method.CLOSE;
                }

                @Override
                public void flush() throws IOException {
                    // Can't be called.
                }

            },
            new Options.Builder().server("nats://options:42").build()).connect(new URI("nats://server:42"), Duration.ofNanos(42));
        natsChannel.shutdownInput();
        assertEquals(Method.SHUTDOWN_INPUT, method[0]);
        natsChannel.close();
        assertEquals(Method.CLOSE, method[0]);


        ByteBuffer bb = ByteBuffer.allocateDirect(10);
        bb.position(3);
        assertTrue(!bb.hasArray());
        buff[0] = "hello".getBytes(UTF_8);
        natsChannel.read(bb);
        assertEquals(Method.READ, method[0]);
        bb.flip();
        bb.position(3);
        assertEquals(ByteBuffer.wrap(buff[0]), bb);

        buff[0] = new byte[10];
        natsChannel.write(bb);
        assertEquals(Method.WRITE, method[0]);
        bb.flip();
        bb.position(3);
        assertEquals(ByteBuffer.wrap(buff[0]), bb);
    }

    @Test
    public void socketDataPortTestForCoverage() throws IOException {
        SocketDataPort dataPort = new SocketDataPort();
        dataPort.flush(); // would never be called.

        Options options = new Options.Builder().server("nats://example.com:42").build();
        assertThrows(IOException.class, () -> dataPort.connect("nats://example.com:42", new NatsConnection(options), Duration.ofMillis(10).toNanos()));
    }
}
