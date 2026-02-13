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

package io.nats.client;

import io.nats.client.support.SSLUtils;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Properties;

public class SslTestingHelper {
    public static String KEYSTORE_PATH = "src/test/resources/keystore.jks";
    public static String TRUSTSTORE_PATH = "src/test/resources/truststore.jks";
    public static String PASSWORD = "password";
    public static char[] PASSWORD_CHARS = PASSWORD.toCharArray();

    public static KeyStore loadKeystore(String path) throws Exception {
        return SSLUtils.loadKeystore(path, PASSWORD_CHARS);
    }

    public static Properties createTestSSLProperties() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_KEYSTORE, KEYSTORE_PATH);
        props.setProperty(Options.PROP_KEYSTORE_PASSWORD, PASSWORD);
        props.setProperty(Options.PROP_TRUSTSTORE, TRUSTSTORE_PATH);
        props.setProperty(Options.PROP_TRUSTSTORE_PASSWORD, PASSWORD);
        return props;
    }

    public static void setKeystoreSystemParameters() {
        System.setProperty("javax.net.ssl.keyStore", KEYSTORE_PATH);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStore",TRUSTSTORE_PATH);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
    }

    public static KeyManager[] createTestKeyManagers() throws Exception {
        return SSLUtils.createKeyManagers(KEYSTORE_PATH, PASSWORD_CHARS);
    }

    public static TrustManager[] createTestTrustManagers() throws Exception {
        return SSLUtils.createTrustManagers(TRUSTSTORE_PATH, PASSWORD_CHARS);
    }

    public static SSLContext createTestSSLContext() throws Exception {
        return SSLUtils.createSSLContext(KEYSTORE_PATH, PASSWORD_CHARS, TRUSTSTORE_PATH, PASSWORD_CHARS);
    }

    public static SSLContext createEmptySSLContext() throws Exception {
        SSLContext ctx = SSLContext.getInstance(Options.DEFAULT_SSL_PROTOCOL);
        ctx.init(new KeyManager[0], new TrustManager[0], new SecureRandom());
        return ctx;
    }

    public static SSLContext getFailContext() throws Exception {
        return getFailContext(createTestSSLContext());
    }

    public static SSLContext getFailContext(SSLContext goodContext) throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext failContext = SSLContext.getInstance(goodContext.getProtocol());
        failContext.init(null, new TrustManager[]{new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                throw new CertificateException("Fail mode: all certificates rejected");
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                throw new CertificateException("Fail mode: all certificates rejected");
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        }}, new SecureRandom());
        return failContext;
    }
}