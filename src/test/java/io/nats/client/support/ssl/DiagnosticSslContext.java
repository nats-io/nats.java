package io.nats.client.support.ssl;

import javax.net.ssl.*;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;

/**
 * An SSLContext subclass that captures SSL/TLS diagnostic information
 * into queryable POJOs instead of logging.
 * <p>
 * When {@code init} is called, key and trust managers are automatically wrapped
 * with recording versions that store events. The socket factory is also wrapped
 * to record handshake completions.
 * <p>
 * Usage:
 * <pre>
 *   DiagnosticSslContext ctx = DiagnosticSslContext.create("TLSv1.2");
 *   ctx.init(km, tm, random);
 *
 *   // After SSL operations...
 *   for (TrustCheckEvent e : ctx.getTrustCheckEvents()) {
 *       if (!e.trusted) {
 *           System.out.println("Trust check failed: " + e.failure.getMessage());
 *       }
 *   }
 *   Date expiry = ctx.getClientCertificateExpiry();
 * </pre>
 */
public class DiagnosticSslContext extends SSLContext {

    private final DiagnosticSpi spi;

    private DiagnosticSslContext(DiagnosticSpi spi, Provider provider, String protocol) {
        super(spi, provider, protocol);
        this.spi = spi;
    }

    /**
     * Creates a new DiagnosticSslContext for the given protocol.
     * Call {@code init} to initialize with key/trust managers.
     */
    public static DiagnosticSslContext getInstance(String protocol) throws NoSuchAlgorithmException {
        SSLContext delegate = SSLContext.getInstance(protocol);
        DiagnosticSpi spi = new DiagnosticSpi(delegate);
        return new DiagnosticSslContext(spi, delegate.getProvider(), protocol);
    }

    // ========================================================================
    // Event query methods
    // ========================================================================

    /**
     * Returns the context configuration snapshot captured after {@code init},
     * or {@code null} if init has not been called.
     */
    public ContextInfo getContextInfo() {
        return spi.contextInfo;
    }

    /**
     * Returns all trust check events (both successful and failed).
     */
    public List<TrustCheckEvent> getTrustCheckEvents() {
        return Collections.unmodifiableList(spi.trustCheckEvents);
    }

    /**
     * Returns all alias selection events.
     */
    public List<AliasSelectionEvent> getAliasSelectionEvents() {
        return Collections.unmodifiableList(spi.aliasSelectionEvents);
    }

    /**
     * Returns all handshake completion events.
     */
    public List<HandshakeEvent> getHandshakeEvents() {
        return Collections.unmodifiableList(spi.handshakeEvents);
    }

    /**
     * Clears all recorded events. Useful between reconnect attempts
     * to isolate diagnostics for a single attempt.
     */
    public void clearEvents() {
        spi.trustCheckEvents.clear();
        spi.aliasSelectionEvents.clear();
        spi.handshakeEvents.clear();
    }

    // ========================================================================
    // Certificate accessors
    // ========================================================================

    /**
     * Returns the X509KeyManagers that were passed to {@code init} (wrapped for recording),
     * or an empty array if init has not been called or no X509KeyManagers were provided.
     */
    public X509KeyManager[] getX509KeyManagers() {
        return spi.x509KeyManagers;
    }

    /**
     * Returns the X509TrustManagers that were passed to {@code init} (wrapped for recording),
     * or an empty array if init has not been called or no X509TrustManagers were provided.
     */
    public X509TrustManager[] getX509TrustManagers() {
        return spi.x509TrustManagers;
    }

    /**
     * Returns the client certificate chain for the given alias from the first X509KeyManager,
     * or {@code null} if not available.
     */
    public X509Certificate[] getClientCertificateChain(String alias) {
        for (X509KeyManager km : spi.x509KeyManagers) {
            X509Certificate[] chain = km.getCertificateChain(alias);
            if (chain != null) {
                return chain;
            }
        }
        return null;
    }

    /**
     * Returns the client certificate chain using the default alias "client",
     * falling back to trying all available client aliases.
     * Returns {@code null} if no client certificates are found.
     */
    public X509Certificate[] getClientCertificateChain() {
        X509Certificate[] chain = getClientCertificateChain("client");
        if (chain != null) {
            return chain;
        }
        for (X509KeyManager km : spi.x509KeyManagers) {
            String[] aliases = km.getClientAliases("RSA", null);
            if (aliases != null) {
                for (String alias : aliases) {
                    chain = km.getCertificateChain(alias);
                    if (chain != null) {
                        return chain;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Returns the expiration date of the leaf client certificate,
     * or {@code null} if no client certificate is available.
     */
    public Date getClientCertificateExpiry() {
        X509Certificate[] chain = getClientCertificateChain();
        return chain != null && chain.length > 0 ? chain[0].getNotAfter() : null;
    }

    /**
     * Returns the trusted CA certificates from all X509TrustManagers.
     */
    public X509Certificate[] getTrustedCertificates() {
        int total = 0;
        for (X509TrustManager tm : spi.x509TrustManagers) {
            total += tm.getAcceptedIssuers().length;
        }
        X509Certificate[] result = new X509Certificate[total];
        int pos = 0;
        for (X509TrustManager tm : spi.x509TrustManagers) {
            X509Certificate[] issuers = tm.getAcceptedIssuers();
            System.arraycopy(issuers, 0, result, pos, issuers.length);
            pos += issuers.length;
        }
        return result;
    }

    // ========================================================================
    // Session inspection (static utilities)
    // ========================================================================

    /**
     * Returns the most recent client {@link SSLSession} from this context's session cache,
     * or {@code null} if the cache is empty.
     */
    public static SSLSession getMostRecentClientSession(SSLContext ctx) {
        SSLSessionContext sessionCtx = ctx.getClientSessionContext();
        if (sessionCtx == null) {
            return null;
        }
        Enumeration<byte[]> ids = sessionCtx.getIds();
        SSLSession newest = null;
        while (ids.hasMoreElements()) {
            byte[] id = ids.nextElement();
            SSLSession session = sessionCtx.getSession(id);
            if (session != null && (newest == null || session.getCreationTime() > newest.getCreationTime())) {
                newest = session;
            }
        }
        return newest;
    }

    /**
     * Formats diagnostic information from an {@link SSLSession} into a readable string.
     * Includes protocol, cipher suite, peer info, and certificate details with expiry status.
     */
    public static String formatSessionDiagnostics(SSLSession session) {
        StringBuilder sb = new StringBuilder();
        sb.append("SSLSession diagnostics:\n");
        sb.append("  Protocol     : ").append(session.getProtocol()).append('\n');
        sb.append("  Cipher suite : ").append(session.getCipherSuite()).append('\n');
        sb.append("  Peer host    : ").append(session.getPeerHost()).append('\n');
        sb.append("  Peer port    : ").append(session.getPeerPort()).append('\n');
        sb.append("  Created      : ").append(new Date(session.getCreationTime())).append('\n');
        sb.append("  Last accessed: ").append(new Date(session.getLastAccessedTime())).append('\n');

        java.security.cert.Certificate[] localCerts = session.getLocalCertificates();
        if (localCerts != null) {
            sb.append("  Local certs  : ").append(localCerts.length).append('\n');
            for (int i = 0; i < localCerts.length; i++) {
                if (localCerts[i] instanceof X509Certificate) {
                    X509Certificate x = (X509Certificate) localCerts[i];
                    sb.append(String.format("    [%d] Subject : %s%n", i, x.getSubjectX500Principal()));
                    sb.append(String.format("         Valid   : %s -> %s%n", x.getNotBefore(), x.getNotAfter()));
                    try {
                        x.checkValidity();
                        sb.append("         Status  : VALID\n");
                    } catch (CertificateException e) {
                        sb.append("         Status  : *** ").append(e.getMessage()).append(" ***\n");
                    }
                }
            }
        } else {
            sb.append("  Local certs  : none\n");
        }

        try {
            java.security.cert.Certificate[] peerCerts = session.getPeerCertificates();
            sb.append("  Peer certs   : ").append(peerCerts.length).append('\n');
            for (int i = 0; i < peerCerts.length; i++) {
                if (peerCerts[i] instanceof X509Certificate) {
                    X509Certificate x = (X509Certificate) peerCerts[i];
                    sb.append(String.format("    [%d] Subject : %s%n", i, x.getSubjectX500Principal()));
                    sb.append(String.format("         Valid   : %s -> %s%n", x.getNotBefore(), x.getNotAfter()));
                    try {
                        x.checkValidity();
                        sb.append("         Status  : VALID\n");
                    } catch (CertificateException e) {
                        sb.append("         Status  : *** ").append(e.getMessage()).append(" ***\n");
                    }
                }
            }
        } catch (SSLPeerUnverifiedException e) {
            sb.append("  Peer certs   : UNVERIFIED (").append(e.getMessage()).append(")\n");
        }

        return sb.toString();
    }
}
