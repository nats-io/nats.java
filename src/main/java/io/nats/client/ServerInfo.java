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

import java.net.InetAddress;

public interface ServerInfo {

    String SERVER_ID = "server_id";
    String SERVER_NAME = "server_name";
    String VERSION = "version";
    String GO = "go";
    String HOST = "host";
    String PORT = "port";
    String HEADERS = "headers";
    String AUTH = "auth_required";
    String TLS = "tls_required";
    String MAX_PAYLOAD = "max_payload";
    String CONNECT_URLS = "connect_urls";
    String PROTOCOL_VERSION = "proto";
    String NONCE = "nonce";
    String LAME_DUCK_MODE = "ldm";
    String JETSTREAM = "jetstream";
    String CLIENT_ID = "client_id";
    String CLIENT_IP = "client_ip";
    String CLUSTER = "cluster";

    boolean isLameDuckMode();
    String getServerId();
    String getServerName();
    String getVersion();
    String getGoVersion();
    String getHost();
    int getPort();
    int getProtocolVersion();
    boolean isHeadersSupported();
    boolean isAuthRequired();
    boolean isTLSRequired();
    long getMaxPayload();
    String[] getConnectURLs();
    byte[] getNonce();
    boolean isJetStreamAvailable();
    int getClientId();
    String getClientIp();
    InetAddress getClientIpInetAddress();
    String getCluster() ;
    String getRawJson();
}