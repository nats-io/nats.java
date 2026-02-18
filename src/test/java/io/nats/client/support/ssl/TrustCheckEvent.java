package io.nats.client.support.ssl;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;

/**
 * Records a trust manager check — whether a client or server certificate
 * chain was accepted or rejected.
 */
public class TrustCheckEvent {
    public final Date timestamp;
    /** "client" or "server" */
    public final String side;
    public final String authType;
    public final X509Certificate[] chain;
    public final boolean trusted;
    /** The failure reason, or {@code null} if trusted */
    public final CertificateException failure;

    TrustCheckEvent(String side, String authType, X509Certificate[] chain,
                    boolean trusted, CertificateException failure) {
        this.timestamp = new Date();
        this.side = side;
        this.authType = authType;
        this.chain = chain != null ? chain.clone() : null;
        this.trusted = trusted;
        this.failure = failure;
    }
}
