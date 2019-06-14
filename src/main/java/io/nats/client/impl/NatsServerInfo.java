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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class NatsServerInfo {

    static final String SERVER_ID = "server_id";
    static final String VERSION = "version";
    static final String GO = "go";
    static final String HOST = "host";
    static final String PORT = "port";
    static final String AUTH = "auth_required";
    static final String TLS = "tls_required";
    static final String MAX_PAYLOAD = "max_payload";
    static final String CONNECT_URLS = "connect_urls";
    static final String PROTOCOL_VERSION = "proto";
    static final String NONCE = "nonce";

    private String serverId;
    private String version;
    private String go;
    private String host;
    private int port;
    private boolean authRequired;
    private boolean tlsRequired;
    private long maxPayload;
    private String[] connectURLs;
    private String rawInfoJson;
    private int protocolVersion;
    private byte[] nonce;

    public NatsServerInfo(String json) {
        this.rawInfoJson = json;
        parseInfo(json);
    }

    public String getServerId() {
        return this.serverId;
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

    public boolean isAuthRequired() {
        return this.authRequired;
    }

    public boolean isTLSRequired() {
        return this.tlsRequired;
    }

    public long getMaxPayload() {
        return this.maxPayload;
    }

    public String[] getConnectURLs() {
        return this.connectURLs;
    }

    public byte[] getNonce() {
        return this.nonce;
    }

    // If parsing succeeds this is the JSON, if not this may be the full protocol line
    public String getRawJson() {
        return rawInfoJson;
    }

    private static final String grabString = "\\s*\"(.+?)\"";
    private static final String grabBoolean = "\\s*(true|false)";
    private static final String grabNumber = "\\s*(\\d+)";
    private static final String grabStringArray = "\\s*\\[(\".+?\")\\]";
    private static final String grabObject = "\\{(.+?)\\}";

    void parseInfo(String jsonString) {
        Pattern serverIdRE = Pattern.compile("\""+SERVER_ID+"\":" + grabString, Pattern.CASE_INSENSITIVE);
        Pattern versionRE = Pattern.compile("\""+VERSION+"\":" + grabString, Pattern.CASE_INSENSITIVE);
        Pattern goRE = Pattern.compile("\""+GO+"\":" + grabString, Pattern.CASE_INSENSITIVE);
        Pattern hostRE = Pattern.compile("\""+HOST+"\":" + grabString, Pattern.CASE_INSENSITIVE);
        Pattern nonceRE = Pattern.compile("\""+NONCE+"\":" + grabString, Pattern.CASE_INSENSITIVE);
        Pattern authRE = Pattern.compile("\""+AUTH+"\":" + grabBoolean, Pattern.CASE_INSENSITIVE);
        Pattern tlsRE = Pattern.compile("\""+TLS+"\":" + grabBoolean, Pattern.CASE_INSENSITIVE);
        Pattern portRE = Pattern.compile("\""+PORT+"\":" + grabNumber, Pattern.CASE_INSENSITIVE);
        Pattern maxRE = Pattern.compile("\""+MAX_PAYLOAD+"\":" + grabNumber, Pattern.CASE_INSENSITIVE);
        Pattern protoRE = Pattern.compile("\""+PROTOCOL_VERSION+"\":" + grabNumber, Pattern.CASE_INSENSITIVE);
        Pattern connectRE = Pattern.compile("\""+CONNECT_URLS+"\":" + grabStringArray, Pattern.CASE_INSENSITIVE);
        Pattern infoObject = Pattern.compile(grabObject, Pattern.CASE_INSENSITIVE);

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

        m = connectRE.matcher(jsonString);
        if (m.find()) {
            String arrayString = m.group(1);
            String[] raw = arrayString.split(",");
            ArrayList<String> urls = new ArrayList<>();

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
}