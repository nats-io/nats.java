/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

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

    /**
     * Returns the server_id.
     *
     * @return the id
     */
    String getId() {
        return id;
    }

    void setId(String id) {
        this.id = id;
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

    /**
     * Returns the NATS server version.
     *
     * @return the gnatsd server version
     */
    String getVersion() {
        return version;
    }

    void setVersion(String version) {
        this.version = version;
    }

    /**
     * Returns whether or not authorization is required by the NATS server.
     *
     * @return the authRequired
     */
    boolean isAuthRequired() {
        return authRequired;
    }

    void setAuthRequired(boolean authRequired) {
        this.authRequired = authRequired;
    }

    /**
     * Returns whether or not TLS is required by the NATS server.
     *
     * @return the tlsRequired
     */
    boolean isTlsRequired() {
        return tlsRequired;
    }

    void setTlsRequired(boolean tlsRequired) {
        this.tlsRequired = tlsRequired;
    }

    /**
     * Returns the max payload size enforced by the NATS server.
     *
     * @return the maxPayload
     */
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

    public byte[] toProtoBytes() {
        return (toString() + "\r\n").getBytes();
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

        return (Boolean.compare(authRequired, other.authRequired) == 0
                && Arrays.equals(connectUrls, other.connectUrls)
                && compare(goVersion, other.goVersion) && compare(host, other.host)
                && compare(id, other.id) && Long.compare(maxPayload, other.maxPayload) == 0
                && Integer.compare(port, other.port) == 0
                && Boolean.compare(sslRequired, other.sslRequired) == 0
                && Boolean.compare(tlsRequired, other.tlsRequired) == 0
                && compare(version, other.version));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, goVersion, host, port, authRequired, sslRequired,
                tlsRequired, tlsVerify, maxPayload, connectUrls);
    }
}
