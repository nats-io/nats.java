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

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

class NatsServerInfo {

    static final String SERVER_ID = "server_id";
    static final String VERSION = "version";
    static final String GO = "go";
    static final String HOST = "host";
    static final String PORT = "port";
    static final String AUTH = "auth_required";
    static final String SSL = "ssl_required";
    static final String MAX_PAYLOAD = "max_payload";
    static final String CONNECT_URLS = "connect_urls";

    private String serverId;
    private String version;
    private String go;
    private String host;
    private int port;
    private boolean authRequired;
    private boolean sslRequired;
    private long maxPayload;
    private String[] connectURLs;
    private HashMap<String, Object> unexpected;

    public NatsServerInfo(String json) {
        unexpected = new HashMap<>();
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

    public boolean isAuthRequired() {
        return this.authRequired;
    }

    public boolean isSSLRequired() {
        return this.sslRequired;
    }

    public long getMaxPayload() {
        return this.maxPayload;
    }

    public String[] getConnectURLs() {
        return this.connectURLs;
    }

    public Map<String, Object> getUnknownInfo() {
        return unexpected;
    }

    void parseInfo(String jsonString) {
        char c;
        boolean skipNext = false;
        CharacterIterator json = new StringCharacterIterator(jsonString);
        ArrayList<String> currentList = null;
        String currentKey = null;

        while ((c = json.current()) != CharacterIterator.DONE) {
            skipNext = false;

            switch (c) {
            case '"':
                String str = readString(json);
                if (currentList != null) {
                    currentList.add(str);
                } else if (currentKey == null) {
                    currentKey = str;
                } else {
                    switch (currentKey) {
                    case SERVER_ID:
                        this.serverId = str;
                        break;
                    case VERSION:
                        this.version = str;
                        break;
                    case GO:
                        this.go = str;
                        break;
                    case HOST:
                        this.host = str;
                        break;
                    default:
                        unexpected.put(currentKey, str);
                        break;
                    }

                    currentKey = null;
                }
                break;
            case '[':
                currentList = new ArrayList<String>();
                break;
            case ']':
                if (CONNECT_URLS.equals(currentKey)) {
                    this.connectURLs = currentList.toArray(new String[0]);
                } else {
                    unexpected.put(currentKey, currentList.toArray(new String[0]));
                }
                currentList = null;
                break;
            case 't':
                json.next();
                json.next();
                json.next(); // assumed r-u-e

                if (currentList != null || currentKey == null) {
                    throw new IllegalArgumentException("Long value with a null key.");
                }

                switch (currentKey) {
                case AUTH:
                    this.authRequired = true;
                    break;
                case SSL:
                    this.sslRequired = true;
                    break;
                default:
                    unexpected.put(currentKey, Boolean.TRUE);
                    break;
                }

                currentKey = null;
                break;
            case 'f':
                json.next();
                json.next();
                json.next();
                json.next(); // assumed a-l-s-e

                if (currentList != null || currentKey == null) {
                    throw new IllegalArgumentException("Long value with a null key.");
                }

                switch (currentKey) {
                case AUTH:
                    this.authRequired = false;
                    break;
                case SSL:
                    this.sslRequired = false;
                    break;
                default:
                    unexpected.put(currentKey, Boolean.FALSE);
                    break;
                }

                currentKey = null;
                break;
            case 'n':
                json.next();
                json.next();
                json.next(); // assumed u-l-l
                // All the defaults are null so no value to set
                currentKey = null;
                break;
            default:
                if (Character.isDigit(c) || c == '-') {
                    long value = readLong(json);

                    if (currentList != null || currentKey == null) {
                        throw new IllegalArgumentException("Long value with a null key.");
                    }

                    switch (currentKey) {
                    case PORT:
                        this.port = (int) value;
                        break;
                    case MAX_PAYLOAD:
                        this.maxPayload = value;
                        break;
                    default:
                        unexpected.put(currentKey, Long.valueOf(value));
                        break;
                    }
                    currentKey = null;
                    skipNext = true;// we read 1 too far on number
                }
                break;
            }

            if (!skipNext)
                json.next();
        }
    }

    long readLong(CharacterIterator json) {
        StringBuilder builder = new StringBuilder();
        char c;

        while ((c = json.current()) != CharacterIterator.DONE) {
            if (!Character.isDigit(c) && c != '-') {
                break;
            }

            builder.append(c);
            json.next();
        }

        try {
            return new Long(builder.toString()).longValue();
        } catch (Exception exp) {
            throw new IllegalArgumentException("Info contained invalid long");
        }
    }

    String readString(CharacterIterator json) {
        StringBuilder builder = new StringBuilder();
        char c;

        while ((c = json.next()) != CharacterIterator.DONE) {
            if (c == '\"') {
                break;
            } else if (c == '\\')// escape
            {
                builder.append('\\');
                c = json.next(); // skip at leat one char to avoid \"
                builder.append(c);
            } else {
                builder.append(c);
            }
        }

        return unescapeString(builder.toString());
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
                case '\"':
                    ch = '\"';
                    break;
                case '\'':
                    ch = '\'';
                    break;
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