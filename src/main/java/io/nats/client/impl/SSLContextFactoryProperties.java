// Copyright 2023 The NATS Authors
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

import io.nats.client.support.NatsUri;

import java.util.List;

public class SSLContextFactoryProperties {
    public final boolean useDefaultTls;
    public final boolean useTrustAllTls;
    public final String keystore;
    public final char[] keystorePassword;
    public final String truststore;
    public final char[] truststorePassword;
    public final String tlsAlgorithm;

    private SSLContextFactoryProperties(Builder b) {
        this.useDefaultTls = b.useDefaultTls;
        this.useTrustAllTls = b.useTrustAllTls;
        this.keystore = b.keystore;
        this.keystorePassword = b.keystorePassword;
        this.truststore = b.truststore;
        this.truststorePassword = b.truststorePassword;
        this.tlsAlgorithm = b.tlsAlgorithm;
    }

    public boolean isUseDefaultTls() {
        return useDefaultTls;
    }

    public boolean isUseTrustAllTls() {
        return useTrustAllTls;
    }

    public String getKeystore() {
        return keystore;
    }

    public char[] getKeystorePassword() {
        return keystorePassword;
    }

    public String getTruststore() {
        return truststore;
    }

    public char[] getTruststorePassword() {
        return truststorePassword;
    }

    public String getTlsAlgorithm() {
        return tlsAlgorithm;
    }

    public static class Builder {
        boolean useDefaultTls;
        boolean useTrustAllTls;
        String keystore;
        char[] keystorePassword;
        String truststore;
        char[] truststorePassword;
        String tlsAlgorithm;
        List<NatsUri> natsServerUris;
        boolean checkUrisForSecure;

        public Builder useDefaultTls(boolean useDefaultTls) {
            this.useDefaultTls = useDefaultTls;
            return this;
        }

        public Builder useTrustAllTls(boolean useTrustAllTls) {
            this.useTrustAllTls = useTrustAllTls;
            return this;
        }

        public Builder keystore(String keystore) {
            this.keystore = keystore;
            return this;
        }

        public Builder keystorePassword(char[] keystorePassword) {
            this.keystorePassword = keystorePassword;
            return this;
        }

        public Builder truststore(String truststore) {
            this.truststore = truststore;
            return this;
        }

        public Builder truststorePassword(char[] truststorePassword) {
            this.truststorePassword = truststorePassword;
            return this;
        }

        public Builder tlsAlgorithm(String tlsAlgorithm) {
            this.tlsAlgorithm = tlsAlgorithm;
            return this;
        }

        public SSLContextFactoryProperties build() {
            return new SSLContextFactoryProperties(this);
        }
    }
}
