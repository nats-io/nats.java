package io.nats.client.support.ssl;

import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * An X509TrustManager wrapper that records trust check results
 * as {@link TrustCheckEvent} instances.
 */
class RecordingX509TrustManager implements X509TrustManager {
    private final X509TrustManager delegate;
    private final List<TrustCheckEvent> events;

    RecordingX509TrustManager(X509TrustManager delegate, List<TrustCheckEvent> events) {
        this.delegate = delegate;
        this.events = events;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
        try {
            delegate.checkClientTrusted(chain, authType);
            events.add(new TrustCheckEvent("client", authType, chain, true, null));
        } catch (CertificateException e) {
            events.add(new TrustCheckEvent("client", authType, chain, false, e));
            throw e;
        }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType)
        throws CertificateException {
        try {
            delegate.checkServerTrusted(chain, authType);
            events.add(new TrustCheckEvent("server", authType, chain, true, null));
        } catch (CertificateException e) {
            events.add(new TrustCheckEvent("server", authType, chain, false, e));
            throw e;
        }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return delegate.getAcceptedIssuers();
    }
}
