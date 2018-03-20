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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import java.util.Arrays;
import java.util.Objects;

class ServerInfo {
    @SerializedName("server_id")
    private String id;

    @SerializedName("version")
    private String version;

    @SerializedName("go")
    private String goVersion;

    @SerializedName("host")
    private String host;

    @SerializedName("port")
    private int port;

    @SerializedName("auth_required")
    private boolean authRequired;

    @SerializedName("ssl_required")
    private boolean sslRequired;

    @SerializedName("tls_required")
    private boolean tlsRequired;

    @SerializedName("tls_verify")
    private boolean tlsVerify;

    @SerializedName("max_payload")
    private long maxPayload; // int64 in Go

    @SerializedName("connect_urls")
    private String[] connectUrls;

    private transient String jsonString = null;
    private static final transient Gson gson = new GsonBuilder().create();

    protected ServerInfo() {}

    ServerInfo(String id, String host, int port, String version, boolean authRequired,
               boolean tlsRequired, int maxPayload, final String[] connectUrls) {

        this.id = id;
        this.host = host;
        this.port = port;
        this.version = version;
        this.authRequired = authRequired;
        this.tlsRequired = tlsRequired;
        this.maxPayload = maxPayload;
        if (connectUrls != null) {
            this.connectUrls = Arrays.copyOf(connectUrls, connectUrls.length);
        }
    }

    ServerInfo(ServerInfo input) {
        this.id = input.id;
        this.version = input.version;
        this.goVersion = input.goVersion;
        this.host = input.host;
        this.port = input.port;
        this.authRequired = input.authRequired;
        this.sslRequired = input.sslRequired;
        this.tlsRequired = input.tlsRequired;
        this.tlsVerify = input.tlsVerify;
        this.maxPayload = input.maxPayload;
        if (input.connectUrls != null) {
            this.connectUrls = Arrays.copyOf(input.connectUrls, input.connectUrls.length);
        }
    }

    static ServerInfo createFromWire(String infoString) {
        ServerInfo rv;
        String jsonString = infoString.replaceFirst("^INFO ", "").trim();
        rv = gson.fromJson(jsonString, ServerInfo.class);
        return rv;
    }

    String getId() {
        return id;
    }

    void setId(String id) {
        this.id = id;
    }

    String getGoVersion() {
        return goVersion;
    }

    void setGoVersion(String goVersion) {
        this.goVersion = goVersion;
    }

    /**
     * Returns the host.
     *
     * @return the host
     */
    String getHost() {
        return host;
    }

    void setHost(String host) {
        this.host = host;
    }

    /**
     * Returns the port.
     *
     * @return the port
     */
    int getPort() {
        return port;
    }

    void setPort(int port) {
        this.port = port;
    }

    String getVersion() {
        return version;
    }

    void setVersion(String version) {
        this.version = version;
    }

    boolean isAuthRequired() {
        return authRequired;
    }

    void setAuthRequired(boolean authRequired) {
        this.authRequired = authRequired;
    }

    boolean isTlsRequired() {
        return tlsRequired;
    }

    void setTlsRequired(boolean tlsRequired) {
        this.tlsRequired = tlsRequired;
    }

    boolean isSslRequired() {
        return sslRequired;
    }

    void setSslRequired(boolean sslRequired) {
        this.sslRequired = sslRequired;
    }

    boolean isTlsVerify() {
        return tlsVerify;
    }

    void setTlsVerify(boolean tlsVerify) {
        this.tlsVerify = tlsVerify;
    }

    long getMaxPayload() {
        return maxPayload;
    }

    void setMaxPayload(long maxPayload) {
        this.maxPayload = maxPayload;
    }

    String[] getConnectUrls() {
        return connectUrls;
    }

    void setConnectUrls(String[] connectUrls) {
        this.connectUrls = Arrays.copyOf(connectUrls, connectUrls.length);
    }

    public String toString() {
        String rv;
        if (jsonString == null) {
            jsonString = gson.toJson(this);
        }
        rv = String.format("INFO %s", jsonString);
        return rv;
    }

    public static boolean compare(String str1, String str2) {
        return (str1 == null ? str2 == null : str1.equals(str2));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof ServerInfo)) {
            return false;
        }

        ServerInfo other = (ServerInfo) obj;

        return (compare(id, other.id) && compare(version, other.version)
                && compare(goVersion, other.goVersion) && compare(host, other.host)
                && Integer.compare(port, other.port) == 0
                && Boolean.compare(authRequired, other.authRequired) == 0
                && Boolean.compare(sslRequired, other.sslRequired) == 0
                && Boolean.compare(tlsRequired, other.tlsRequired) == 0
                && Boolean.compare(tlsVerify, other.tlsVerify) == 0
                && Long.compare(maxPayload, other.maxPayload) == 0
                && Arrays.equals(connectUrls, other.connectUrls));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, goVersion, host, port, authRequired, sslRequired,
                tlsRequired, tlsVerify, maxPayload, connectUrls);
    }
}
