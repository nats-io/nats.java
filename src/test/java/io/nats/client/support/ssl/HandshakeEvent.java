package io.nats.client.support.ssl;

import javax.net.ssl.HandshakeCompletedEvent;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.security.cert.X509Certificate;
import java.util.Date;

/**
 * Records a successful TLS handshake completion.
 */
public class HandshakeEvent {
    public final Date timestamp;
    public final String protocol;
    public final String cipherSuite;
    public final String peerHost;
    public final int peerPort;
    /** The leaf peer certificate, or {@code null} if unverified */
    public final X509Certificate peerCertificate;

    HandshakeEvent(HandshakeCompletedEvent event) {
        this.timestamp = new Date();
        SSLSession session = event.getSession();
        this.protocol = session.getProtocol();
        this.cipherSuite = event.getCipherSuite();
        this.peerHost = session.getPeerHost();
        this.peerPort = session.getPeerPort();
        X509Certificate peer = null;
        try {
            java.security.cert.Certificate[] peerCerts = event.getPeerCertificates();
            if (peerCerts.length > 0 && peerCerts[0] instanceof X509Certificate) {
                peer = (X509Certificate) peerCerts[0];
            }
        } catch (SSLPeerUnverifiedException ignored) {
        }
        this.peerCertificate = peer;
    }
}
