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

import java.util.Arrays;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.*;

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

        JsonValue jv;
        try {
            jv = JsonParser.parse(json, json.indexOf("{"));
        }
        catch (JsonParseException e) {
            throw new IllegalArgumentException("Invalid Server Info Json");
        }

        serverId = readString(jv, SERVER_ID);
        serverName = readString(jv, SERVER_NAME);
        version = readString(jv, VERSION);
        go = readString(jv, GO);
        host = readString(jv, HOST);
        headersSupported = readBoolean(jv, HEADERS);
        authRequired = readBoolean(jv, AUTH_REQUIRED);
        nonce = readBytes(jv, NONCE);
        tlsRequired = readBoolean(jv, TLS);
        lameDuckMode = readBoolean(jv, LAME_DUCK_MODE);
        jetStream = readBoolean(jv, JETSTREAM);
        port = readInteger(jv, PORT, 0);
        protocolVersion = readInteger(jv, PROTO, 0);
        maxPayload = readLong(jv, MAX_PAYLOAD, 0);
        clientId = readInteger(jv, CLIENT_ID, 0);
        clientIp = readString(jv, CLIENT_IP);
        cluster = readString(jv, CLUSTER);
        connectURLs = readStringListIgnoreEmpty(jv, CONNECT_URLS);
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

    static class Version implements Comparable<Version> {
        Integer major;
        Integer minor;
        Integer patch;
        String extra;

        Version(String v) {
            try {
                String[] split = v.replaceAll("v", "").replaceAll("-", ".").split("\\Q.\\E");
                major = Integer.parseInt(split[0]);
                minor = Integer.parseInt(split[1]);
                patch = Integer.parseInt(split[2]);
                extra = split.length == 3 ? "zzzzz" : split[3];
            }
            catch (NumberFormatException nfe) {
                major = 0;
                minor = 0;
                patch = 0;
                extra = "";
            }
        }

        @Override
        public int compareTo(Version o) {
            int c = major.compareTo(o.major);
            if (c == 0) {
                c = minor.compareTo(o.minor);
                if (c == 0) {
                    c = patch.compareTo(o.patch);
                    if (c == 0) {
                        c = extra.compareTo(o.extra);
                    }
                }
            }
            return c;
        }
    }

    private static String normalExtra(String vString) {
        int at = vString.indexOf("-");
        return at == -1 ? "~" : vString.substring(at).toLowerCase();
    }

    public boolean isNewerVersionThan(String vTarget) {
        return new Version(version).compareTo(new Version(vTarget)) > 0;
    }

    public boolean isSameVersion(String vTarget) {
        return new Version(version).compareTo(new Version(vTarget)) == 0;
    }

    public boolean isOlderThanVersion(String vTarget) {
        return new Version(version).compareTo(new Version(vTarget)) < 0;
    }

    public boolean isSameOrOlderThanVersion(String vTarget) {
        return new Version(version).compareTo(new Version(vTarget)) <= 0;
    }

    public boolean isSameOrNewerThanVersion(String vTarget) {
        return new Version(version).compareTo(new Version(vTarget)) >= 0;
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
