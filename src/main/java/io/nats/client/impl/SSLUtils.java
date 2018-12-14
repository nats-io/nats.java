// Copyright 2015-2018 The NATS Authors
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

import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Set;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import io.nats.client.Options;

public class SSLUtils {
    private static TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        public void checkClientTrusted(X509Certificate[] certs, String authType) {
        }

        public void checkServerTrusted(X509Certificate[] certs, String authType) {
        }
    } };

    public static SSLContext createOpenTLSContext() {
        SSLContext context = null;

        try {
            context = SSLContext.getInstance(Options.DEFAULT_SSL_PROTOCOL);
            context.init(null, trustAllCerts, new SecureRandom());
        } catch (Exception e) {
            context = null;
        }

        return context;
    }

    /**
     * Creates a context that allows the certs from our previous full TLS connection.
     */
    public static SSLContext createReconnectContext(Set<Certificate> allowedCerts) {
        SSLContext context = null;
        TrustManager[] tms = new TrustManager[] { new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }
    
            public void checkClientTrusted(X509Certificate[] certs, String authType)
             throws CertificateException {
            }
    
            public void checkServerTrusted(X509Certificate[] certs, String authType) 
             throws CertificateException {
                for (Certificate c : certs) {
                    if (allowedCerts.contains(c)) {
                        break;
                    }
                }
                throw new CertificateException("Reconnect from cluster server with different certs");
            }
        } };

        try {
            context = SSLContext.getInstance(Options.DEFAULT_SSL_PROTOCOL);
            context.init(null, tms, new SecureRandom()); // Default keystore
        } catch (Exception e) {
            context = null;
        }

        return context;
    }
}