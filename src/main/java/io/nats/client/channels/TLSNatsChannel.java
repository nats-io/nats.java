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
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Arrays;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import io.nats.client.support.SSLUtils;

import static io.nats.client.Options.DEFAULT_PORT;
import static io.nats.client.support.NatsConstants.TLS_PROTOCOL;
import static io.nats.client.support.NatsConstants.OPENTLS_PROTOCOL;
import static io.nats.client.support.WithTimeout.withTimeout;
import static io.nats.client.support.URIUtils.withDefaultPort;

public class TLSNatsChannel extends AbstractNatsChannel {
    @FunctionalInterface
    public interface CreateSSLEngine {
        SSLEngine create(URI serverURI) throws GeneralSecurityException;
    }
    private static class Factory implements NatsChannelFactory {
        private CreateSSLEngine createSSLEngine;

        // SslContext.createSSLEngine(uri.getHost(), uri.getPort())
        private Factory(CreateSSLEngine createSSLEngine) {
            this.createSSLEngine = createSSLEngine;
        }

        @Override
        public NatsChannel connect(
            URI serverURI,
            Duration timeout,
            Chain next) throws IOException, GeneralSecurityException
        {
            if (null != createSSLEngine) {
                SSLEngine sslEngine = createSSLEngine.create(withDefaultPort(serverURI, DEFAULT_PORT));
                if (null != sslEngine) {
                    return new TLSNatsChannel(
                        next.connect(serverURI, timeout),
                        sslEngine);
                }
            }
            return next.connect(serverURI, timeout);
        }

        @Override
        public SSLContext createSSLContext(URI serverURI, Chain next) throws GeneralSecurityException {
            if (TLS_PROTOCOL.equals(serverURI.getScheme())) {
                return SSLContext.getDefault();
            }
            else if (OPENTLS_PROTOCOL.equals(serverURI.getScheme())) {
                return SSLUtils.createOpenTLSContext();
            }
            return next.createSSLContext(serverURI);
        }
    }

    /**
     * @param createSSLEngine is called if {@link NatsChannel#upgradeToSecure(Duration)}
     *     is called.
     * @return a nats channel factory that will connect using TLS if the server URI
     *     is tls or opentls. Note that the TLS handshake is delayed until {@link #upgradeToSecure(Duration)}
     *     is called.
     */
    public static NatsChannelFactory factory(CreateSSLEngine createSSLEngine) {
        return new Factory(createSSLEngine);
    }

    private TLSByteChannel byteChannel;
    private SSLEngine sslEngine;

    private TLSNatsChannel(NatsChannel wrap, SSLEngine sslEngine) {
        super(wrap);
        this.sslEngine = sslEngine;
    }

    @Override
    public boolean isSecure() {
        return null != byteChannel;
    }

    @Override
    public void shutdownInput() throws IOException {
        if (null == byteChannel) {
            wrap.shutdownInput();
        }
        // cannot call shutdownInput on sslSocket
    }

    @Override
    public void close() throws IOException {
        if (null == byteChannel) {
            wrap.close();
        } else {
            // Ensures proper close sequence of TLS:
            byteChannel.close();
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (null == byteChannel) {
            return wrap.read(dst);
        } else {
            return byteChannel.read(dst);
        }
    }

    @Override
    public boolean isOpen() {
        if (null == byteChannel) {
            return wrap.isOpen();
        } else {
            return byteChannel.isOpen();
        }
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return byteChannel.write(srcs, offset, length);
    }

    @Override
    public String transformConnectUrl(String connectUrl) {
        return wrap.transformConnectUrl(connectUrl);
    }

	@Override
	public void upgradeToSecure(Duration timeout) throws IOException {
        if (null != this.byteChannel) {
            // Already upgraded, no-op.
            return;
        }
        withTimeout(() -> {
            sslEngine.setUseClientMode(true);
            this.byteChannel = new TLSByteChannel(wrap, sslEngine);
            this.byteChannel.handshake();
            // Dispose of unnecessary resources:
            sslEngine = null;
            return null;
        }, timeout, cause -> new ConnectTimeoutException("Timeout performing TLS handshake", cause));
    }
}
