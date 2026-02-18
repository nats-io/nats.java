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

package io.nats.client.support;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.security.cert.X509Certificate;
import java.security.Principal;
import java.security.cert.Certificate;

public class ReadOnlySSLSession implements SSLSession {
    final SSLSession sslSession;

    public ReadOnlySSLSession(SSLSession sslSession) {
        this.sslSession = sslSession;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getId() {
        return sslSession.getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SSLSessionContext getSessionContext() {
        return sslSession.getSessionContext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCreationTime() {
        return sslSession.getCreationTime();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLastAccessedTime() {
        return sslSession.getLastAccessedTime();
    }

    /**
     * READ-ONLY, NOT SUPPORTED
     */
    @Override
    public void invalidate() {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        return sslSession.isValid();
    }

    /**
     * READ-ONLY, NOT SUPPORTED
     */
    @Override
    public void putValue(String s, Object o) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getValue(String s) {
        return sslSession.getValue(s);
    }

    /**
     * READ-ONLY, NOT SUPPORTED
     */
    @Override
    public void removeValue(String s) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getValueNames() {
        return sslSession.getValueNames();
    }

    /**
     * READ-ONLY, NOT SUPPORTED
     */
    @Override
    public Certificate[] getPeerCertificates() throws SSLPeerUnverifiedException {
        return sslSession.getPeerCertificates();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Certificate[] getLocalCertificates() {
        return sslSession.getLocalCertificates();
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("removal")
    @Override
    public X509Certificate[] getPeerCertificateChain() throws SSLPeerUnverifiedException {
        return sslSession.getPeerCertificateChain();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Principal getPeerPrincipal() throws SSLPeerUnverifiedException {
        return sslSession.getPeerPrincipal();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Principal getLocalPrincipal() {
        return sslSession.getLocalPrincipal();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCipherSuite() {
        return sslSession.getCipherSuite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getProtocol() {
        return sslSession.getProtocol();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getPeerHost() {
        return sslSession.getPeerHost();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getPeerPort() {
        return sslSession.getPeerPort();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getPacketBufferSize() {
        return sslSession.getPacketBufferSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getApplicationBufferSize() {
        return sslSession.getApplicationBufferSize();
    }
}
