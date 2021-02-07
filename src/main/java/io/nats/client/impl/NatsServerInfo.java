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

import io.nats.client.ServerInfo;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.nats.client.impl.JsonUtils.*;

class NatsServerInfo implements ServerInfo {

    private String serverId;
    private String serverName;
    private String version;
    private String go;
    private String host;
    private int port;
    private boolean headersSupported;
    private boolean authRequired;
    private boolean tlsRequired;
    private long maxPayload;
    private String[] connectURLs;
    private String rawInfoJson;
    private int protocolVersion;
    private byte[] nonce;
    private boolean lameDuckMode;
    private boolean jetStream;
    private int clientId;
    private String clientIp;
    private String cluster;

    public NatsServerInfo(String json) {
        this.rawInfoJson = json;
        parseInfo(json);
    }

    @Override
    public boolean isLameDuckMode() {
        return lameDuckMode;
    }

    @Override
    public String getServerId() {
        return this.serverId;
    }

    @Override
    public String getServerName() {
        return serverName;
    }

    @Override
    public String getVersion() {
        return this.version;
    }

    @Override
    public String getGoVersion() {
        return this.go;
    }

    @Override
    public String getHost() {
        return this.host;
    }

    @Override
    public int getPort() {
        return this.port;
    }

    @Override
    public int getProtocolVersion() {
        return this.protocolVersion;
    }

    @Override
    public boolean isHeadersSupported() { return this.headersSupported; }

    @Override
    public boolean isAuthRequired() {
        return this.authRequired;
    }

    @Override
    public boolean isTLSRequired() {
        return this.tlsRequired;
    }

    @Override
    public long getMaxPayload() {
        return this.maxPayload;
    }

    @Override
    public String[] getConnectURLs() {
        return this.connectURLs;
    }

    @Override
    public byte[] getNonce() {
        return this.nonce;
    }

    @Override
    public boolean isJetStreamAvailable() {
        return this.jetStream;
    }

    @Override
    public int getClientId() {
        return clientId;
    }

    @Override
    public String getClientIp() {
        return clientIp;
    }

    @Override
    public String getCluster() {
        return cluster;
    }

    // If parsing succeeds this is the JSON, if not this may be the full protocol line
    @Override
    public String getRawJson() {
        return rawInfoJson;
    }

    private static final Pattern lameDuckModeRE = buildBooleanPattern(LAME_DUCK_MODE);
    private static final Pattern jetStreamRE = buildBooleanPattern(JETSTREAM);
    private static final Pattern serverIdRE = buildStringPattern(SERVER_ID);
    private static final Pattern serverNameRE = buildStringPattern(SERVER_NAME);
    private static final Pattern versionRE = buildStringPattern(VERSION);
    private static final Pattern goRE = buildStringPattern(GO);
    private static final Pattern hostRE = buildStringPattern(HOST);
    private static final Pattern nonceRE = buildStringPattern(NONCE);
    private static final Pattern headersRE = buildBooleanPattern(HEADERS);
    private static final Pattern authRE = buildBooleanPattern(AUTH);
    private static final Pattern tlsRE = buildBooleanPattern(TLS);
    private static final Pattern portRE = buildNumberPattern(PORT);
    private static final Pattern maxRE = buildNumberPattern(MAX_PAYLOAD);
    private static final Pattern protoRE = buildNumberPattern(PROTOCOL_VERSION);
    private static final Pattern connectRE = buildStringArrayPattern(CONNECT_URLS);
    private static final Pattern clientIdRE = buildNumberPattern(CLIENT_ID);
    private static final Pattern clientIpRE = buildStringPattern(CLIENT_IP);
    private static final Pattern clusterRE = buildStringPattern(CLUSTER);
    private static final Pattern infoObject = buildCustomPattern("\\{(.+?)\\}");

    void parseInfo(String jsonString) {

        Matcher m = infoObject.matcher(jsonString);
        if (m.find()) {
            jsonString = m.group(0);
            this.rawInfoJson = jsonString;
        } else {
            jsonString = "";
        }

        if (jsonString.length() < 2) {
            throw new IllegalArgumentException("Server info requires at least {}.");
        } else if (jsonString.charAt(0) != '{' || jsonString.charAt(jsonString.length()-1) != '}') {
            throw new IllegalArgumentException("Server info should be JSON wrapped with { and }.");
        }

        m = serverIdRE.matcher(jsonString);
        if (m.find()) {
            this.serverId = unescapeString(m.group(1));
        }

        m = serverNameRE.matcher(jsonString);
        if (m.find()) {
            this.serverName = unescapeString(m.group(1));
        }

        m = versionRE.matcher(jsonString);
        if (m.find()) {
            this.version = unescapeString(m.group(1));
        }
        
        m = goRE.matcher(jsonString);
        if (m.find()) {
            this.go = unescapeString(m.group(1));
        }
        
        m = hostRE.matcher(jsonString);
        if (m.find()) {
            this.host = unescapeString(m.group(1));
        }

        m = headersRE.matcher(jsonString);
        if (m.find()) {
            this.headersSupported = Boolean.parseBoolean(m.group(1));
        }

        m = authRE.matcher(jsonString);
        if (m.find()) {
            this.authRequired = Boolean.parseBoolean(m.group(1));
        }

        m = nonceRE.matcher(jsonString);
        if (m.find()) {
            String encodedNonce = m.group(1);
            this.nonce = encodedNonce.getBytes(StandardCharsets.US_ASCII);
        }
        
        m = tlsRE.matcher(jsonString);
        if (m.find()) {
            this.tlsRequired = Boolean.parseBoolean(m.group(1));
        }

        m = lameDuckModeRE.matcher(jsonString);
        if (m.find()) {
            this.lameDuckMode = Boolean.parseBoolean(m.group(1));
        }

        m = jetStreamRE.matcher(jsonString);
        if (m.find()) {
            this.jetStream = Boolean.parseBoolean(m.group(1));
        }

        m = portRE.matcher(jsonString);
        if (m.find()) {
            this.port = Integer.parseInt(m.group(1));
        }
        
        m = protoRE.matcher(jsonString);
        if (m.find()) {
            this.protocolVersion = Integer.parseInt(m.group(1));
        }
        
        m = maxRE.matcher(jsonString);
        if (m.find()) {
            this.maxPayload = Long.parseLong(m.group(1));
        }

        m = clientIdRE.matcher(jsonString);
        if (m.find()) {
            this.clientId = Integer.parseInt(m.group(1));
        }

        m = clientIpRE.matcher(jsonString);
        if (m.find()) {
            this.clientIp = unescapeString(m.group(1));
        }

        m = clusterRE.matcher(jsonString);
        if (m.find()) {
            this.cluster = unescapeString(m.group(1));
        }

        m = connectRE.matcher(jsonString);
        if (m.find()) {
            String arrayString = m.group(1);
            String[] raw = arrayString.split(",");
            List<String> urls = new ArrayList<>();

            for (String s : raw) {
                String cleaned = s.trim().replace("\"", "");;
                if (cleaned.length() > 0) {
                    urls.add(cleaned);
                }
            }

            this.connectURLs = urls.toArray(new String[0]);
        }
    }

    // See https://gist.github.com/uklimaschewski/6741769, no license required
    // Removed octal support
    String unescapeString(String st) {

        StringBuilder sb = new StringBuilder(st.length());

        for (int i = 0; i < st.length(); i++) {
            char ch = st.charAt(i);
            if (ch == '\\') {
                char nextChar = (i == st.length() - 1) ? '\\' : st.charAt(i + 1);
                switch (nextChar) {
                case '\\':
                    ch = '\\';
                    break;
                case 'b':
                    ch = '\b';
                    break;
                case 'f':
                    ch = '\f';
                    break;
                case 'n':
                    ch = '\n';
                    break;
                case 'r':
                    ch = '\r';
                    break;
                case 't':
                    ch = '\t';
                    break;
                /*case '\"':
                    ch = '\"';
                    break;
                case '\'':
                    ch = '\'';
                    break;*/
                // Hex Unicode: u????
                case 'u':
                    if (i >= st.length() - 5) {
                        ch = 'u';
                        break;
                    }
                    int code = Integer.parseInt(
                            "" + st.charAt(i + 2) + st.charAt(i + 3) + st.charAt(i + 4) + st.charAt(i + 5), 16);
                    sb.append(Character.toChars(code));
                    i += 5;
                    continue;
                }
                i++;
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "NatsServerInfo{" +
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
                ", connectURLs=" + Arrays.toString(connectURLs) +
                ", rawInfoJson='" + rawInfoJson + '\'' +
                ", protocolVersion=" + protocolVersion +
                ", nonce=" + Arrays.toString(nonce) +
                ", lameDuckMode=" + lameDuckMode +
                ", jetStream=" + jetStream +
                ", clientId=" + clientId +
                ", clientIp='" + clientIp + '\'' +
                ", cluster='" + cluster + '\'' +
                '}';
    }

    public String toJsonString() {
        StringBuilder sb = JsonUtils.beginFormattedJson();
        addFld(sb, SERVER_ID, serverId);
        addFld(sb, SERVER_NAME, serverName);
        addFld(sb, VERSION, version);
        addFld(sb, GO, go);
        addFld(sb, HOST, host);
        addFld(sb, PORT,  + port);
        addFld(sb, HEADERS, headersSupported);
        addFld(sb, AUTH, authRequired);
        addFld(sb, TLS, tlsRequired);
        addFld(sb, MAX_PAYLOAD,  + maxPayload);
        addFld(sb, CONNECT_URLS, connectURLs);
        addFld(sb, PROTOCOL_VERSION, protocolVersion);
        addFld(sb, NONCE, nonce == null ? null : new String(nonce));
        addFld(sb, LAME_DUCK_MODE, lameDuckMode);
        addFld(sb, JETSTREAM, jetStream);
        addFld(sb, CLIENT_ID,  + clientId);
        addFld(sb, CLIENT_IP, clientIp);
        addFld(sb, CLUSTER, cluster);
        return endFormattedJson(sb);
    }
}