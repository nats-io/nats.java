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

package io.nats.client.impl;

import javax.net.ssl.*;
import java.security.SecureRandom;

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
        failMode = true;
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
