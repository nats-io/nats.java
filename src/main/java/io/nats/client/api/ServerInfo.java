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

package io.nats.client.api;

import io.nats.client.support.JsonParseException;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import io.nats.client.support.ServerVersion;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.*;
import static io.nats.client.support.NatsConstants.UNDEFINED;

public class ServerInfo {

    public static final ServerInfo EMPTY_INFO = new ServerInfo("INFO {}");

    private final String serverId;
    private final String serverName;
    private final String version;
    private final String go;
    private final String host;
    private final int port;
    private final boolean headersSupported;
    private final boolean authRequired;
    private final boolean tlsRequired;
    private final boolean tlsAvailable;
    private final long maxPayload;
    private final List<String> connectURLs;
    private final int protocolVersion;
    private final byte[] nonce;
    private final boolean lameDuckMode;
    private final boolean jetStream;
    private final int clientId;
    private final String clientIp;
    private final String cluster;

    public ServerInfo(String json) {
        // INFO<sp>{ INFO<\t>{ or {
        if (json == null || json.length() < 6 || ('{' != json.charAt(0) && '{' != json.charAt(5))) {
            throw new IllegalArgumentException("Invalid Server Info");
        }

        JsonValue jv;
        try {
            jv = JsonParser.parse(json, json.indexOf("{"));
        }
        catch (JsonParseException e) {
            throw new IllegalArgumentException("Invalid Server Info Json");
        }

        serverId = readString(jv, SERVER_ID, UNDEFINED);
        serverName = readString(jv, SERVER_NAME, UNDEFINED);
        version = readString(jv, VERSION, "0.0.0");
        go = readString(jv, GO, "0.0.0");
        host = readString(jv, HOST, UNDEFINED);
        headersSupported = readBoolean(jv, HEADERS);
        authRequired = readBoolean(jv, AUTH_REQUIRED);
        nonce = readBytes(jv, NONCE);
        tlsRequired = readBoolean(jv, TLS_REQUIRED);
        tlsAvailable = readBoolean(jv, TLS_AVAILABLE);
        lameDuckMode = readBoolean(jv, LAME_DUCK_MODE);
        jetStream = readBoolean(jv, JETSTREAM);
        port = readInteger(jv, PORT, 0);
        protocolVersion = readInteger(jv, PROTO, 0);
        maxPayload = readLong(jv, MAX_PAYLOAD, 0);
        clientId = readInteger(jv, CLIENT_ID, 0);
        clientIp = readString(jv, CLIENT_IP, "0.0.0.0");
        cluster = readString(jv, CLUSTER);
        connectURLs = readStringListIgnoreEmpty(jv, CONNECT_URLS);
    }

    public boolean isLameDuckMode() {
        return lameDuckMode;
    }

    @NonNull
    public String getServerId() {
        return this.serverId;
    }

    @NonNull
    public String getServerName() {
        return serverName;
    }

    @NonNull
    public String getVersion() {
        return this.version;
    }

    @NonNull
    public String getGoVersion() {
        return this.go;
    }

    @NonNull
    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public int getProtocolVersion() {
        return this.protocolVersion;
    }

    public boolean isHeadersSupported() { return this.headersSupported; }

    public boolean isAuthRequired() {
        return this.authRequired;
    }

    public boolean isTLSRequired() {
        return this.tlsRequired;
    }

    public boolean isTLSAvailable() {
        return tlsAvailable;
    }

    public long getMaxPayload() {
        return this.maxPayload;
    }

    @NonNull
    public List<String> getConnectURLs() {
        return this.connectURLs;
    }

    public byte @Nullable [] getNonce() {
        return this.nonce;
    }

    public boolean isJetStreamAvailable() {
        return this.jetStream;
    }

    public int getClientId() {
        return clientId;
    }

    @NonNull
    public String getClientIp() {
        return clientIp;
    }

    @Nullable
    public String getCluster() {
        return cluster;
    }

    public boolean isNewerVersionThan(String vTarget) {
        return ServerVersion.isNewer(version, vTarget);
    }

    public boolean isSameVersion(String vTarget) {
        return ServerVersion.isSame(version, vTarget);
    }

    public boolean isOlderThanVersion(String vTarget) {
        return ServerVersion.isOlder(version, vTarget);
    }

    public boolean isSameOrOlderThanVersion(String vTarget) {
        return ServerVersion.isSameOrOlder(version, vTarget);
    }

    public boolean isSameOrNewerThanVersion(String vTarget) {
        return ServerVersion.isSameOrNewer(version, vTarget);
    }

    @Override
    public String toString() {
        return "ServerInfo{" +
                "serverId='" + serverId + '\'' +
                ", serverName='" + serverName + '\'' +
                ", version='" + version + '\'' +
                ", go='" + go + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", headersSupported=" + headersSupported +
                ", authRequired=" + authRequired +
                ", tlsRequired=" + tlsRequired +
                ", tlsAvailable=" + tlsAvailable +
                ", maxPayload=" + maxPayload +
                ", connectURLs=" + connectURLs +
                ", protocolVersion=" + protocolVersion +
                ", nonce=" + Arrays.toString(nonce) +
                ", lameDuckMode=" + lameDuckMode +
                ", jetStream=" + jetStream +
                ", clientId=" + clientId +
                ", clientIp='" + clientIp + '\'' +
                ", cluster='" + cluster + '\'' +
                '}';
    }
}
