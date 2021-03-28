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

import io.nats.client.support.JsonUtils;

import java.util.Arrays;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;

public class ServerInfo {

    private final String serverId;
    private final String serverName;
    private final String version;
    private final String go;
    private final String host;
    private final int port;
    private final boolean headersSupported;
    private final boolean authRequired;
    private final boolean tlsRequired;
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

        serverId = JsonUtils.readString(json, SERVER_ID_RE);
        serverName = JsonUtils.readString(json, SERVER_NAME_RE);
        version = JsonUtils.readString(json, VERSION_RE);
        go = JsonUtils.readString(json, GO_RE);
        host = JsonUtils.readString(json, HOST_RE);
        headersSupported =JsonUtils.readBoolean(json, HEADERS_RE);
        authRequired = JsonUtils.readBoolean(json, AUTH_REQUIRED_RE);
        nonce = JsonUtils.readBytes(json, NONCE_RE);
        tlsRequired = JsonUtils.readBoolean(json, TLS_RE);
        lameDuckMode = JsonUtils.readBoolean(json, LAME_DUCK_MODE_RE);
        jetStream = JsonUtils.readBoolean(json, JET_STREAM_RE);
        port = JsonUtils.readInt(json, PORT_RE, 0);
        protocolVersion = JsonUtils.readInt(json, PROTO_RE, 0);
        maxPayload = JsonUtils.readLong(json, MAX_PAYLOAD_RE, 0);
        clientId = JsonUtils.readInt(json, CLIENT_ID_RE, 0);
        clientIp = JsonUtils.readString(json, CLIENT_IP_RE);
        cluster = JsonUtils.readString(json, CLUSTER_RE);
        connectURLs = JsonUtils.getStringList(CONNECT_URLS, json);
    }

    public boolean isLameDuckMode() {
        return lameDuckMode;
    }

    public String getServerId() {
        return this.serverId;
    }

    public String getServerName() {
        return serverName;
    }

    public String getVersion() {
        return this.version;
    }

    public String getGoVersion() {
        return this.go;
    }

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

    public long getMaxPayload() {
        return this.maxPayload;
    }

    public List<String> getConnectURLs() {
        return this.connectURLs;
    }

    public byte[] getNonce() {
        return this.nonce;
    }

    public boolean isJetStreamAvailable() {
        return this.jetStream;
    }

    public int getClientId() {
        return clientId;
    }

    public String getClientIp() {
        return clientIp;
    }

    public String getCluster() {
        return cluster;
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
