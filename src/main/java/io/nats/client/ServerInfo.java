/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import java.util.HashMap;
import java.util.Map;

class ServerInfo {
    private String id;
    private String host;
    private int port;
    private String version;
    private String goVersion;
    private boolean authRequired;
    private boolean sslRequired;
    private boolean tlsRequired;
    private boolean tlsVerify;
    private long maxPayload; // int64 in Go

    private Map<String, String> parameters = new HashMap<String, String>();

    public ServerInfo(String jsonString) {
        try {
            jsonString = jsonString.substring(jsonString.indexOf('{') + 1);
            jsonString = jsonString.substring(0, jsonString.lastIndexOf('}'));
        } catch (IndexOutOfBoundsException iobe) {
            // do nothing
        }

        String[] kvPairs = jsonString.split(",");
        for (String pair : kvPairs)
            addKVPair(pair);


        this.id = parameters.get("server_id");
        this.host = parameters.get("host");
        this.port = Integer.parseInt(parameters.get("port"));
        this.version = parameters.get("version");
        this.goVersion = parameters.get("go");
        this.authRequired = Boolean.parseBoolean(parameters.get("auth_required"));
        this.sslRequired = Boolean.parseBoolean(parameters.get("ssl_required"));
        this.tlsRequired = Boolean.parseBoolean(parameters.get("tls_required"));
        this.tlsVerify = Boolean.parseBoolean(parameters.get("tls_verify"));
        this.maxPayload = Long.parseLong(parameters.get("max_payload"));

    }

    private void addKVPair(String kvPair) {
        String key;
        String val;

        kvPair = kvPair.trim();
        String[] parts = kvPair.split(":", 2);
        key = parts[0].trim();
        val = parts[1].trim();

        // trim the quotes
        int lastQuotePos = key.lastIndexOf("\"");
        key = key.substring(1, lastQuotePos);

        // bools and numbers may not have quotes.
        if (val.startsWith("\"")) {
            lastQuotePos = val.lastIndexOf("\"");
            val = val.substring(1, lastQuotePos);
        }
        parameters.put(key, val);
    }

    /**
     * @return the id
     */
    String getId() {
        return id;
    }

    /**
     * @return the host
     */
    String getHost() {
        return host;
    }

    /**
     * @return the port
     */
    int getPort() {
        return port;
    }

    /**
     * @return the gnatsd server version
     */
    String getVersion() {
        return version;
    }

    /**
     * @return the authRequired
     */
    boolean isAuthRequired() {
        return authRequired;
    }

    /**
     * @return the tlsRequired
     */
    boolean isTlsRequired() {
        return tlsRequired;
    }

    /**
     * @return the maxPayload
     */
    long getMaxPayload() {
        return maxPayload;
    }

    /**
     * @return the parameters
     */
    Map<String, String> getParameters() {
        return parameters;
    }

    public String toString() {
        String rv = String.format(
                "INFO {\"server_id\":\"%s\",\"version\":\"%s\",\"go\":\"%s\","
                        + "\"host\":\"%s\",\"port\":%d,\"auth_required\":%b,\"ssl_required\":%b,"
                        + "\"tls_required\":%b,\"tls_verify\":%b,\"max_payload\":%d}\r\n",
                this.id, this.version, this.goVersion, this.host, this.port, this.authRequired,
                this.tlsRequired, this.tlsRequired, this.tlsVerify, this.maxPayload);
        return rv;
    }
}
