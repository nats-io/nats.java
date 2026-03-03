package io.nats.client.support.ssl;

import javax.net.ssl.*;
import java.security.KeyManagementException;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * SSLContextSpi implementation that wraps key and trust managers with recording
 * versions on {@code init}, and returns a recording socket factory.
 */
class DiagnosticSpi extends SSLContextSpi {

    static final X509KeyManager[] EMPTY_KM = new X509KeyManager[0];
    static final X509TrustManager[] EMPTY_TM = new X509TrustManager[0];

    final SSLContext delegate;
    volatile ContextInfo contextInfo;
    volatile X509KeyManager[] x509KeyManagers = EMPTY_KM;
    volatile X509TrustManager[] x509TrustManagers = EMPTY_TM;
    final List<TrustCheckEvent> trustCheckEvents = new CopyOnWriteArrayList<TrustCheckEvent>();
    final List<AliasSelectionEvent> aliasSelectionEvents = new CopyOnWriteArrayList<AliasSelectionEvent>();
    final List<HandshakeEvent> handshakeEvents = new CopyOnWriteArrayList<HandshakeEvent>();

    DiagnosticSpi(SSLContext delegate) {
        this.delegate = delegate;
    }

    @Override
    protected void engineInit(KeyManager[] km, TrustManager[] tm, SecureRandom sr)
        throws KeyManagementException {
        KeyManager[] wrappedKm = wrapKeyManagers(km);
        TrustManager[] wrappedTm = wrapTrustManagers(tm);
        storeManagers(wrappedKm, wrappedTm);
        delegate.init(wrappedKm, wrappedTm, sr);
        contextInfo = new ContextInfo(delegate);
    }

    private KeyManager[] wrapKeyManagers(KeyManager[] originals) {
        if (originals == null) return null;
        KeyManager[] wrapped = new KeyManager[originals.length];
        for (int i = 0; i < originals.length; i++) {
            if (originals[i] instanceof X509KeyManager) {
                wrapped[i] = new RecordingX509KeyManager(
                    (X509KeyManager) originals[i], aliasSelectionEvents);
            } else {
                wrapped[i] = originals[i];
            }
        }
        return wrapped;
    }

    private TrustManager[] wrapTrustManagers(TrustManager[] originals) {
        if (originals == null) return null;
        TrustManager[] wrapped = new TrustManager[originals.length];
        for (int i = 0; i < originals.length; i++) {
            if (originals[i] instanceof X509TrustManager) {
                wrapped[i] = new RecordingX509TrustManager(
                    (X509TrustManager) originals[i], trustCheckEvents);
            } else {
                wrapped[i] = originals[i];
            }
        }
        return wrapped;
    }

    private void storeManagers(KeyManager[] km, TrustManager[] tm) {
        if (km != null) {
            int count = 0;
            for (KeyManager k : km) {
                if (k instanceof X509KeyManager) count++;
            }
            X509KeyManager[] arr = new X509KeyManager[count];
            int i = 0;
            for (KeyManager k : km) {
                if (k instanceof X509KeyManager) arr[i++] = (X509KeyManager) k;
            }
            x509KeyManagers = arr;
        }
        if (tm != null) {
            int count = 0;
            for (TrustManager t : tm) {
                if (t instanceof X509TrustManager) count++;
            }
            X509TrustManager[] arr = new X509TrustManager[count];
            int i = 0;
            for (TrustManager t : tm) {
                if (t instanceof X509TrustManager) arr[i++] = (X509TrustManager) t;
            }
            x509TrustManagers = arr;
        }
    }

    @Override
    protected SSLSocketFactory engineGetSocketFactory() {
        return new RecordingSSLSocketFactory(delegate.getSocketFactory(), handshakeEvents);
    }

    @Override
    protected SSLServerSocketFactory engineGetServerSocketFactory() {
        return delegate.getServerSocketFactory();
    }

    @Override
    protected SSLEngine engineCreateSSLEngine() {
        return delegate.createSSLEngine();
    }

    @Override
    protected SSLEngine engineCreateSSLEngine(String host, int port) {
        return delegate.createSSLEngine(host, port);
    }

    @Override
    protected SSLSessionContext engineGetServerSessionContext() {
        return delegate.getServerSessionContext();
    }

    @Override
    protected SSLSessionContext engineGetClientSessionContext() {
        return delegate.getClientSessionContext();
    }
}
