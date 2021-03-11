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
import java.util.List;

import javax.net.ssl.SSLContext;

import io.nats.client.http.HttpInterceptor;
import io.nats.client.http.HttpRequest;
import io.nats.client.http.HttpResponse;
import io.nats.client.impl.Headers;

import static io.nats.client.support.WithTimeout.withTimeout;
import static io.nats.client.support.URIUtils.withScheme;
import static io.nats.client.support.URIUtils.withDefaultPort;

/**
 * Wrap a NatsChannel with HTTP CONNECT proxy.
 */
public class HttpProxyNatsChannel extends AbstractNatsChannel {
    /**
     * Factory used for creating instances of HttpProxyNatsChannel.
     */
    private static class Factory implements NatsChannelFactory {
        private final URI proxyServer;

        private Factory(URI proxyServer, List<HttpInterceptor> proxyInterceptors) {
            if (null != proxyServer) {
                String scheme = proxyServer.getScheme();
                if ("https".equalsIgnoreCase(scheme)) {
                    this.proxyServer = withDefaultPort(withScheme(proxyServer, "tls"), 443);
                } else if (null == scheme || "http".equalsIgnoreCase(scheme)) {
                    this.proxyServer = withDefaultPort(withScheme(proxyServer, "nats"), 80);
                } else {
                    throw new IllegalArgumentException("Unsupported scheme: " + proxyServer);
                }
            } else {
                this.proxyServer = null;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public NatsChannel connect(URI serverURI, Duration timeout, Chain next) throws IOException, GeneralSecurityException {
            if (null == proxyServer) {
                return next.connect(serverURI, timeout);
            }
            long t0 = System.nanoTime();
            NatsChannel channel = next.connect(proxyServer, timeout);
            timeout = timeout.minusNanos(System.nanoTime() - t0);

            HttpProxyNatsChannel proxy = new HttpProxyNatsChannel(channel, serverURI);

            proxy.connect(proxyServer, timeout);
            return proxy;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SSLContext createSSLContext(URI serverURI, Chain next) throws GeneralSecurityException {
            SSLContext context = next.createSSLContext(serverURI);
            if (null == context && null != proxyServer) {
                return next.createSSLContext(proxyServer);
            }
            return context;
        }
    }

    /**
     * @param proxyServer is the fully qualified proxy server, for example http://proxy:8888
     * @param proxyInterceptors is a list of http interceptors to use when connecting with
     *     the proxy server. Define an interceptor to do things like adding an HTTP header
     *     to the connect request or performing oauth.
     * @return a nats channel factory which will tunnel through the specified proxy server.
     */
    public static NatsChannelFactory factory(URI proxyServer, List<HttpInterceptor> proxyInterceptors) {
        return new Factory(proxyServer, proxyInterceptors);
    }

    private URI serverURI;

    private HttpProxyNatsChannel(NatsChannel wrap, URI serverURI) throws IOException {
        super(wrap);
        this.serverURI = serverURI;
        this.wrap = wrap;
    }

    public void connect(URI proxyServer, Duration timeout) throws IOException, GeneralSecurityException {
        // Force upgradeToSecure if necessary:
        if ("tls".equals(proxyServer.getScheme())) {
            long t0 = System.nanoTime();
            wrap.upgradeToSecure(timeout);
            timeout = timeout.minusNanos(System.nanoTime() - t0);
        }
        withTimeout(() -> {
            ByteBuffer readBuffer = ByteBuffer.allocate(10 * 1024);
            HttpRequest request = HttpRequest.builder()
                .method("CONNECT")
                .uri(serverURI.getAuthority())
                .headers(new Headers()
                    .add("Host", proxyServer.getHost()))
                .build();
            HttpResponse response;
            try {
                request.write(wrap);
                response = HttpResponse.read(wrap, readBuffer);
            } catch (IOException ex) {
                throw new InvalidProxyProtocolException("Failed to read/write HTTP request/response from " + proxyServer, ex);
            }
            if (200 != response.getStatusCode()) {
                throw new InvalidProxyProtocolException("Received non-200 status code from " + proxyServer + " got response=" + response);
            }
            return null;
        }, timeout, ex -> new ConnectTimeoutException("Timeout trying to connect to the proxy", ex));
    }    
}
