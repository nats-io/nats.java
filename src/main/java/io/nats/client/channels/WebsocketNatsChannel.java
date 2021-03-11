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
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import javax.net.ssl.SSLContext;

import io.nats.client.channels.WebsocketByteChannel.Role;
import io.nats.client.http.HttpInterceptor;
import io.nats.client.support.WebsocketFrameHeader.OpCode;

import static io.nats.client.support.WithTimeout.withTimeout;
import static io.nats.client.support.URIUtils.withScheme;
import static io.nats.client.support.URIUtils.withDefaultPort;

/**
 * Wrap a NatsChannel with websocket protocol.
 */
public class WebsocketNatsChannel extends AbstractNatsChannel {
    // returns null if it was not websockets.
    private static URI stripWebsocketScheme(URI uri) {
        if ("ws".equalsIgnoreCase(uri.getScheme())) {
            return withDefaultPort(withScheme(uri, "nats"), 80);
        } else if ("wss".equalsIgnoreCase(uri.getScheme())) {
            return withDefaultPort(withScheme(uri, "tls"), 443);
        }
        return null;
    }
    private static class Factory implements NatsChannelFactory {
        private List<HttpInterceptor> interceptors;
        private Factory(List<HttpInterceptor> interceptors) {
            this.interceptors = interceptors;
        }

        @Override
        public NatsChannel connect(URI serverURI, Duration timeout, Chain next) throws IOException, GeneralSecurityException {
            URI subTransportURI = stripWebsocketScheme(serverURI);
            if (null == subTransportURI) {
                return next.connect(serverURI, timeout);
            }

            // Socket connect:
            long t0 = System.nanoTime();
            NatsChannel channel = next.connect(subTransportURI, timeout);
            timeout = timeout.minusNanos(System.nanoTime() - t0);

            // Force immediate upgradToSecure if needed:
            if ("wss".equals(serverURI.getScheme())) {
                t0 = System.nanoTime();
                channel.upgradeToSecure(timeout);
                timeout = timeout.minusNanos(System.nanoTime() - t0);
            }

            WebsocketByteChannel websocketChannel = new WebsocketByteChannel(
                channel,
                channel,
                OpCode.BINARY,
                new SecureRandom(),
                interceptors,
                serverURI,
                Role.CLIENT);
            withTimeout(() -> {
                websocketChannel.handshake();
                return null;
            }, timeout, cause -> new ConnectTimeoutException("Timeout attempting to connect using websockets", cause));
            return new WebsocketNatsChannel(channel, websocketChannel);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SSLContext createSSLContext(URI serverURI, Chain next) throws GeneralSecurityException {
            return next.createSSLContext(
                Optional.ofNullable(stripWebsocketScheme(serverURI))
                .orElse(serverURI));
        }
    }

    /**
     * @param interceptors is a list of HTTP interceptors which can be used to augment
     *     the websocket request (for example, add HTTP headers or perform oauth).
     * @return the NatsChannelFactory for this implementation.
     */
    public static NatsChannelFactory factory(List<HttpInterceptor> interceptors) {
        return new Factory(interceptors);
    }

    private WebsocketByteChannel channel;

    private WebsocketNatsChannel(NatsChannel wrap, WebsocketByteChannel channel) throws IOException {
        super(wrap);
        this.channel = channel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        channel.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return channel.write(srcs, offset, length);
    }

    /**
     * If no scheme is specified, a URI is returned with either wss or ws scheme
     * depending on if the wrapped channel is secure.
     */
    @Override
    public String transformConnectUrl(String connectUrl) {
        URI uri;
        try {
            uri = new URI(connectUrl);
        } catch (URISyntaxException e) {
            return connectUrl;
        }
        if (null != uri.getScheme()) {
            return connectUrl;
        }
        // Otherwise default it to websockets:
        try {
            return new URI(
                isSecure() ? "wss" : "ws",
                uri.getSchemeSpecificPart(),
                uri.getFragment()).toString();
        } catch (URISyntaxException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
