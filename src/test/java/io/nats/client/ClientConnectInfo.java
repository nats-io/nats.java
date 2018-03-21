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

package io.nats.client;

import org.junit.experimental.categories.Category;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

@Category(UnitTest.class)
class ClientConnectInfo {

    private ClientConnectInfo() {
    }

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

    private static final transient Gson gson = new GsonBuilder().create();

    static ClientConnectInfo createFromWire(String connectString) {
        String jsonString = connectString.replaceFirst("^CONNECT ", "").trim();
        return gson.fromJson(jsonString, ClientConnectInfo.class);
    }

    boolean isVerbose() {
        return verbose;
    }

    void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    boolean isPedantic() {
        return pedantic;
    }

    void setPedantic(boolean pedantic) {
        this.pedantic = pedantic;
    }

    boolean isSslRequired() {
        return sslRequired;
    }

    void setSslRequired(boolean sslRequired) {
        this.sslRequired = sslRequired;
    }

    String getName() {
        return name;
    }

    void setName(String name) {
        this.name = name;
    }

    String getLanguage() {
        return lang;
    }


    void setLanguage(String language) {
        this.lang = language;
    }

    String getVersion() {
        return version;
    }

    void setVersion(String version) {
        this.version = version;
    }

    public String toString() {
        return String.format("CONNECT %s", gson.toJson(this));
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
