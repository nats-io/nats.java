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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import io.nats.client.channels.ConnectTimeoutException;
import io.nats.client.channels.NatsChannel;
import io.nats.client.channels.NatsChannelFactory;
import io.nats.client.support.SSLUtils;
import io.nats.client.Options;

import static io.nats.client.support.NatsConstants.TLS_PROTOCOL;
import static io.nats.client.support.NatsConstants.OPENTLS_PROTOCOL;
import static io.nats.client.support.BufferUtils.remaining;
import static io.nats.client.support.WithTimeout.withTimeout;

/**
 * Adapter for legacy implementations of DataPort.
 * 
 * <p><b>NOTES:</b>
 * <ul>
 *   <li>The NatsConnection passed into connect() is a completely disconnected instance, and thus only the
 *       {@link NatsConnection#getOptions() getOptions()} method may be relied upon.
 *   <li>{@link DataPort#upgradeToSecure() upgradeToSecure()} will never be called, but if there is a
 *       need to upgrade to a secure connection, this adapted DataPort instance will be wrapped automatically,
 *       and thus the TLS upgrade should happen transparently.
 *   <li>{@link DataPort#flush() flush()} will never be called.
 * </ul>
 */
@Deprecated
public class AdaptDataPortToNatsChannelFactory implements NatsChannelFactory.Chain {
    private Supplier<DataPort> dataPortSupplier;
    private Options options;

    public AdaptDataPortToNatsChannelFactory(Supplier<DataPort> dataPortSupplier, Options options) {
        this.dataPortSupplier = dataPortSupplier;
        this.options = options;
    }

    @Override
    public NatsChannel connect(
        URI serverURI,
        Duration timeout)
        throws IOException
    {
        DataPort dataPort = dataPortSupplier.get();
        dataPort.connect(serverURI.toString(), new NatsConnection(options), timeout.toNanos());
        return new Adapter(dataPort);
    }

    private static class Adapter implements NatsChannel {
        private DataPort dataPort;
        private boolean isOpen = true;

        private Adapter(DataPort dataPort) {
            this.dataPort = dataPort;
        }

        @Override
        public int read(ByteBuffer original) throws IOException {
            ByteBuffer dst = original;
            if (!original.hasArray()) {
                dst = ByteBuffer.allocate(original.remaining());
            }
            int offset = dst.arrayOffset();
            int length = dataPort.read(dst.array(), offset + dst.position(), dst.remaining());
            if (length > 0) {
                dst.position(dst.position() + length);
            }
            if (original != dst) {
                dst.flip();
                original.put(dst);
            }
            return length;
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public void close() throws IOException {
            dataPort.close();
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            int size = Math.toIntExact(remaining(srcs, offset, length));
            // NOTE: We are allocating a new buffer on every write, not very efficient!
            ByteBuffer buff = ByteBuffer.allocate(size);
            int endOffset = offset + length;
            while (offset < endOffset) {
                buff.put(srcs[offset++]);
            }
            dataPort.write(buff.array(), size);

            return size;
        }

        @Override
        public boolean isSecure() {
            return false;
        }

        @Override
        public void shutdownInput() throws IOException {
            dataPort.shutdownInput();
        }

        @Override
        public String transformConnectUrl(String connectUrl) {
            return connectUrl;
        }

		@Override
		public void upgradeToSecure(Duration timeout) throws IOException {
            withTimeout(() -> {
                dataPort.upgradeToSecure();
                return null;
            }, timeout, ex -> new ConnectTimeoutException("Failed to upgrade to secure using a legacy DataPort", ex));
		}
    }

    @Override
    public SSLContext createSSLContext(URI serverURI) throws GeneralSecurityException {
        // Original data port implementation only supported these uris:
        if (TLS_PROTOCOL.equals(serverURI.getScheme())) {
            return SSLContext.getDefault();
        }
        else if (OPENTLS_PROTOCOL.equals(serverURI.getScheme())) {
            // Previous code would return null if Exception is thrown... but if that
            // happens then the exception will only be deferred until the SSLContext
            // is required, therefore I believe it is better to NOT catch the
            // exception.
            return SSLUtils.createOpenTLSContext();
        }
        return null;
    }
}
