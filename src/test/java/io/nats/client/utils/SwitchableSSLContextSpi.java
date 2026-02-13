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

package io.nats.client.utils;

import javax.net.ssl.*;
import java.security.SecureRandom;
import java.util.Enumeration;

/**
 * An SSLContextSpi that delegates to one of two SSLContexts.
 * Starts in "good" mode and can be switched to "fail" mode.
 */
class SwitchableSSLContextSpi extends SSLContextSpi {
    private final SSLContext goodContext;
    private final SSLContext failContext;
    private volatile boolean failMode = false;

    SwitchableSSLContextSpi(SSLContext goodContext, SSLContext failContext) {
        this.goodContext = goodContext;
        this.failContext = failContext;
    }

    void switchToFailMode() {
        // Invalidate all cached SSL sessions from the good context so that
        // the next connection attempt cannot reuse a cached handshake and
        // must perform a full handshake against the fail context.
        invalidateSessions(goodContext.getClientSessionContext());
        failMode = true;
    }

    private static void invalidateSessions(SSLSessionContext sessionContext) {
        if (sessionContext == null) {
            return;
        }
        Enumeration<byte[]> ids = sessionContext.getIds();
        while (ids.hasMoreElements()) {
            SSLSession session = sessionContext.getSession(ids.nextElement());
            if (session != null) {
                session.invalidate();
            }
        }
    }

    private SSLContext current() {
        return failMode ? failContext : goodContext;
    }

    @Override
    protected SSLSocketFactory engineGetSocketFactory() {
        return current().getSocketFactory();
    }

    @Override
    protected SSLServerSocketFactory engineGetServerSocketFactory() {
        return current().getServerSocketFactory();
    }

    @Override
    protected SSLEngine engineCreateSSLEngine() {
        return current().createSSLEngine();
    }

    @Override
    protected SSLEngine engineCreateSSLEngine(String host, int port) {
        return current().createSSLEngine(host, port);
    }

    @Override
    protected SSLSessionContext engineGetClientSessionContext() {
        return current().getClientSessionContext();
    }

    @Override
    protected SSLSessionContext engineGetServerSessionContext() {
        return current().getServerSessionContext();
    }

    @Override
    protected void engineInit(KeyManager[] km, TrustManager[] tm, SecureRandom sr) { /* delegates are already initialized */ }
}
