// Copyright 2026 The NATS Authors
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

package io.nats.client.support.ssl;

import org.bouncycastle.asn1.x500.X500Name;

import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Minimal loopback NATS server that selects its certificate by the TLS server name (SNI) the client sends,
 * like a TLS terminating proxy or ingress in front of nats-server does.
 * Only SNI for the certificates' host selects the valid, CA-signed certificate. Any other name, or no name,
 * gets an expired default certificate. No DNS, external server, certificate files or clock changes are needed.
 * Adapted from the test server in nats.java PR #1637.
 */
public class SniTestServer implements AutoCloseable {
    public static final String HOST = "nats.test";

    public static class Certificates {
        public final String host;
        public final X509Certificate valid;
        public final X509Certificate expired;
        private final X509Certificate ca;
        private final KeyPair serverKey;

        public Certificates() throws Exception {
            this(HOST);
        }

        public Certificates(String host) throws Exception {
            this.host = host;
            long now = System.currentTimeMillis();
            KeyPair caKey = ExpiringClientCertUtil.generateKeyPair();
            X500Name issuer = new X500Name("CN=SNI Test CA");
            ca = ExpiringClientCertUtil.generateCertificate(issuer, issuer,
                caKey.getPublic(), caKey.getPrivate(), new Date(now - 7_200_000),
                new Date(now + 86_400_000), true);
            serverKey = ExpiringClientCertUtil.generateKeyPair();
            valid = ExpiringClientCertUtil.generateCertificate(new X500Name("CN=" + host), issuer,
                serverKey.getPublic(), caKey.getPrivate(), new Date(now - 3_600_000),
                new Date(now + 86_400_000), false);
            expired = ExpiringClientCertUtil.generateCertificate(new X500Name("CN=expired.default"), issuer,
                serverKey.getPublic(), caKey.getPrivate(), new Date(now - 7_200_000),
                new Date(now - 3_600_000), false);
        }

        public DiagnosticSslContext clientContext() throws Exception {
            KeyStore trust = KeyStore.getInstance("JKS");
            trust.load(null, null);
            // Trust only the CA: trusting the expired leaf directly would bypass
            // the expiration check on some providers.
            trust.setCertificateEntry("ca", ca);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trust);
            DiagnosticSslContext context = DiagnosticSslContext.getInstance("TLSv1.2");
            context.init(null, tmf.getTrustManagers(), null);
            return context;
        }
    }

    private final ServerSocket listener;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();
    public final CompletableFuture<String> requestedName = new CompletableFuture<>();
    public final CompletableFuture<X509Certificate> selectedCertificate = new CompletableFuture<>();
    public final CompletableFuture<Void> completed = new CompletableFuture<>();
    private volatile boolean closed;

    public SniTestServer(Certificates certificates, boolean tlsFirst, int discoveredPort) throws Exception {
        listener = new ServerSocket(0, 10, InetAddress.getByName("127.0.0.1"));
        SSLContext context = SSLContext.getInstance("TLSv1.2");
        context.init(new KeyManager[]{new X509ExtendedKeyManager() {
            @Override
            public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
                if (!"RSA".equals(keyType)) {
                    return null;
                }
                ExtendedSSLSession session = (ExtendedSSLSession) ((SSLSocket) socket).getHandshakeSession();
                String name = "";
                for (SNIServerName serverName : session.getRequestedServerNames()) {
                    if (serverName instanceof SNIHostName) {
                        name = ((SNIHostName) serverName).getAsciiName();
                    }
                }
                requestedName.complete(name);
                boolean matches = certificates.host.equals(name);
                selectedCertificate.complete(matches ? certificates.valid : certificates.expired);
                return matches ? "valid" : "expired";
            }

            @Override
            public X509Certificate[] getCertificateChain(String alias) {
                return new X509Certificate[]{"valid".equals(alias) ? certificates.valid : certificates.expired, certificates.ca};
            }

            @Override
            public PrivateKey getPrivateKey(String alias) { return certificates.serverKey.getPrivate(); }
            @Override
            public String[] getServerAliases(String keyType, Principal[] issuers) { return new String[]{"valid", "expired"}; }
            @Override
            public String[] getClientAliases(String keyType, Principal[] issuers) { return null; }
            @Override
            public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) { return null; }
        }}, null, null);
        executor.submit(() -> {
            try {
                while (!closed) {
                    Socket raw = listener.accept();
                    sockets.add(raw);
                    executor.submit(() -> serve(raw, context, tlsFirst, discoveredPort));
                }
            }
            catch (IOException e) {
                if (!closed) {
                    completed.completeExceptionally(e);
                }
            }
        });
    }

    public int port() { return listener.getLocalPort(); }

    private void serve(Socket raw, SSLContext context, boolean tlsFirst, int discoveredPort) {
        String urls = discoveredPort == 0 ? "" : ",\"connect_urls\":[\"127.0.0.1:" + discoveredPort + "\"]";
        String info = "INFO {\"server_id\":\"sni-test\",\"version\":\"2.11.0\",\"proto\":1,"
            + "\"max_payload\":1048576,\"tls_required\":true" + urls + "}\r\n";
        try (Socket ignored = raw) {
            raw.setSoTimeout(10_000);
            if (!tlsFirst) {
                raw.getOutputStream().write(info.getBytes(StandardCharsets.UTF_8));
                raw.getOutputStream().flush();
            }
            try (SSLSocket tls = (SSLSocket) context.getSocketFactory().createSocket(raw, "127.0.0.1", port(), true)) {
                tls.setUseClientMode(false);
                tls.startHandshake();
                Writer out = new OutputStreamWriter(tls.getOutputStream(), StandardCharsets.UTF_8);
                if (tlsFirst) {
                    out.write(info);
                    out.flush();
                }
                BufferedReader in = new BufferedReader(new InputStreamReader(tls.getInputStream(), StandardCharsets.UTF_8));
                String line;
                while ((line = in.readLine()) != null) {
                    if ("PING".equals(line)) {
                        out.write("PONG\r\n");
                        out.flush();
                    }
                }
                completed.complete(null);
            }
        }
        catch (IOException e) {
            if (!closed) {
                completed.completeExceptionally(e);
            }
        }
        finally {
            sockets.remove(raw);
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        listener.close();
        for (Socket socket : sockets) {
            socket.close();
        }
        executor.shutdownNow();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            throw new IOException("SNI test server did not stop");
        }
    }
}
