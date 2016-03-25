/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

@Category(UnitTest.class)
public class ClientConnectInfo {

    public ClientConnectInfo() {
        // TODO Auto-generated constructor stub
    }

    private Map<String, String> parameters = new HashMap<String, String>();

    private boolean verbose = false;
    private boolean pedantic = false;
    private boolean sslRequired = false;
    private String name = "";
    private String language = "java";
    private String version;

    public ClientConnectInfo(String jsonString) {
        try {
            jsonString = jsonString.substring(jsonString.indexOf('{') + 1);
            jsonString = jsonString.substring(0, jsonString.lastIndexOf('}'));
        } catch (IndexOutOfBoundsException iobe) {
            // do nothing
        }

        String[] kvPairs = jsonString.split(",");
        for (String s : kvPairs)
            addKVPair(s);


        this.verbose = Boolean.parseBoolean(parameters.get("verbose"));
        this.pedantic = Boolean.parseBoolean(parameters.get("pedantic"));
        this.sslRequired = Boolean.parseBoolean(parameters.get("ssl_required"));
        this.name = parameters.get("name");
        this.language = parameters.get("lang");
        this.version = parameters.get("version");

    }

    private void addKVPair(String kvPair) {
        String key;
        String val;

        kvPair = kvPair.trim();
        String[] parts = kvPair.split(":");
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
     * @return the verbose
     */
    public boolean isVerbose() {
        return verbose;
    }

    /**
     * @param verbose the verbose to set
     */
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * @return the pedantic
     */
    public boolean isPedantic() {
        return pedantic;
    }

    /**
     * @param pedantic the pedantic to set
     */
    public void setPedantic(boolean pedantic) {
        this.pedantic = pedantic;
    }

    /**
     * @return the sslRequired
     */
    public boolean isSslRequired() {
        return sslRequired;
    }

    /**
     * @param sslRequired the sslRequired to set
     */
    public void setSslRequired(boolean sslRequired) {
        this.sslRequired = sslRequired;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the language
     */
    public String getLanguage() {
        return language;
    }

    /**
     * @param language the language to set
     */
    public void setLanguage(String language) {
        this.language = language;
    }

    /**
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * @param version the version to set
     */
    public void setVersion(String version) {
        this.version = version;
    }

    public String toString() {
        String rv = String.format(
                "CONNECT {\"verbose\":%b,\"pedantic\":%b,\"ssl_required\":%b,"
                        + "\"name\":\"%s\",\"lang\":\"%s\",\"version\":\"%s\"}\r\n",
                this.verbose, this.pedantic, this.sslRequired, this.name, this.language,
                this.version);
        return rv;
    }

}
