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

import io.nats.client.SslTestingHelper;

import javax.net.ssl.SSLContext;

/**
 * An SSLContext that initially behaves like a normal TLS context (from SslTestingHelper)
 * but can be switched to "fail mode" where it rejects all server certificates,
 * causing SSL handshake failures on reconnect.
 */
public class SwitchableSSLContext extends SSLContext {
    private final SwitchableSSLContextSpi switchableSpi;

    private SwitchableSSLContext(SwitchableSSLContextSpi spi, SSLContext referenceContext) {
        super(spi, referenceContext.getProvider(), referenceContext.getProtocol());
        this.switchableSpi = spi;
    }

    public static SwitchableSSLContext create() throws Exception {
        // Create a "fail" context whose TrustManager rejects all certificates
        SSLContext goodContext = SslTestingHelper.createTestSSLContext();
        SSLContext failContext = SslTestingHelper.getFailContext(goodContext);
        SwitchableSSLContextSpi spi = new SwitchableSSLContextSpi(goodContext, failContext);
        return new SwitchableSSLContext(spi, goodContext);
    }

    public void changeToFailMode() {
        switchableSpi.switchToFailMode();
    }
}
