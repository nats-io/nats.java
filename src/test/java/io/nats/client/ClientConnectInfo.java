/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class ClientConnectInfo {

    public ClientConnectInfo() {}

    @SerializedName("verbose")
    private boolean verbose = false;

    @SerializedName("pedantic")
    private boolean pedantic = false;

    @SerializedName("user")
    private String user = null;

    @SerializedName("pass")
    private String pass = null;

    @SerializedName("auth_token")
    private String token = null;

    @SerializedName("ssl_required")
    private boolean sslRequired = false;

    @SerializedName("tls_required")
    private boolean tlsRequired = false;

    @SerializedName("name")
    private String name = "";

    @SerializedName("lang")
    private String lang = "java";

    @SerializedName("version")
    private String version;

    @SerializedName("protocol")
    private int protocol;

    private static transient Gson gson = new GsonBuilder().create();

    protected static ClientConnectInfo createFromWire(String connectString) {
        ClientConnectInfo rv = null;
        String jsonString = connectString.replaceFirst("^CONNECT ", "").trim();
        rv = gson.fromJson(jsonString, ClientConnectInfo.class);
        return rv;
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
        return lang;
    }

    /**
     * @param language the language to set
     */
    public void setLanguage(String language) {
        this.lang = language;
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
        // String rv = String.format(
        // "CONNECT {\"verbose\":%b,\"pedantic\":%b,\"ssl_required\":%b,"
        // + "\"name\":\"%s\",\"lang\":\"%s\",\"version\":\"%s\"}\r\n",
        // this.verbose, this.pedantic, this.sslRequired, this.name, this.lang, this.version);
        String rv = String.format("CONNECT %s", gson.toJson(this));
        return rv;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public int getProtocol() {
        return protocol;
    }

    public void setProtocol(int protocol) {
        this.protocol = protocol;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}
