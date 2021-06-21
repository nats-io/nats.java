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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.GatheringByteChannel;
import java.security.GeneralSecurityException;
import java.time.Duration;

/**
 * Low-level API for establishing a connection. This allows us to support the
 * "decorator" design pattern to support TLS, Websockets, and HTTP Proxy support.
 */
public interface NatsChannel extends ByteChannel, GatheringByteChannel {
    /**
     * When performing the NATS INFO/CONNECT handshake, we may need to
     * upgrade to a secure connection, but if this connection is already
     * secured, it should be a no-op.
     * 
     * @return true if the connection is already secured.
     */
    boolean isSecure();

    /**
     * Starts the TLS handshake.
     * 
     * When NATS handles the INFO message, it contains a tls_required
     * field, which when set to true triggers the connection to be
     * upgraded to secure protocol.
     * 
     * @param timeout is the max time allowed for performing the TLS handshake.
     * @throws IOException if there is an input/output error
     * @throws GeneralSecurityException if there is a problem configuring the SSLContext
     */
    void upgradeToSecure(Duration timeout) throws IOException, GeneralSecurityException;

    /**
     * Shutdown the reader side of the channel.
     * 
     * @throws IOException if an IO error occurs.
     */
    void shutdownInput() throws IOException;

    /**
     * When NATS handles the INFO message, it contains connect_urls which
     * may not be fully qualified with a protocol, and thus when using
     * these urls during reconnect attempts, the NatsChannelFactory may
     * not create the correct implementation of NatsChannel.
     * 
     * @param connectUrl to transform
     * @return the transformed connect_url
     */
    String transformConnectUrl(String connectUrl);

    @Override
    default int write(ByteBuffer src) throws IOException {
        return Math.toIntExact(write(new ByteBuffer[]{src}, 0, 1));
    }

    @Override
    default long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }
}
