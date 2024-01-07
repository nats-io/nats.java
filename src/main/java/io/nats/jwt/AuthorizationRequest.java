// Copyright 2021-2024 The NATS Authors
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

package io.nats.jwt;

import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.jwt.Utils.AUTH_REQUEST_CLAIM_TYPE;

public class AuthorizationRequest extends GenericClaimFields<AuthorizationRequest> {
    public ServerId serverId;
    public String userNkey;
    public ClientInfo clientInfo;
    public ConnectOpts connectOpts;
    public ClientTls clientTls;
    public String requestNonce;

    public AuthorizationRequest() {
        super(AUTH_REQUEST_CLAIM_TYPE, 2);
    }

    public AuthorizationRequest(JsonValue jv) {
        super(jv, AUTH_REQUEST_CLAIM_TYPE, 2);
        serverId = ServerId.optionalInstance(JsonValueUtils.readValue(jv, "server_id"));
        userNkey = JsonValueUtils.readString(jv, "user_nkey");
        clientInfo = ClientInfo.optionalInstance(JsonValueUtils.readValue(jv, "client_info"));
        connectOpts = ConnectOpts.optionalInstance(JsonValueUtils.readValue(jv, "connect_opts"));
        clientTls = ClientTls.optionalInstance(JsonValueUtils.readValue(jv, "client_tls"));
        requestNonce = JsonValueUtils.readString(jv, "request_nonce");
    }

    @Override
    protected AuthorizationRequest getThis() {
        return this;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        baseJson(sb);
        JsonUtils.addField(sb, "server_id", serverId);
        JsonUtils.addField(sb, "user_nkey", userNkey);
        JsonUtils.addField(sb, "client_info", clientInfo);
        JsonUtils.addField(sb, "connect_opts", connectOpts);
        JsonUtils.addField(sb, "client_tls", clientTls);
        JsonUtils.addField(sb, "request_nonce", requestNonce);
        return endJson(sb).toString();
    }

    public AuthorizationRequest serverId(ServerId serverId) {
        this.serverId = serverId;
        return this;
    }

    public AuthorizationRequest userNkey(String userNkey) {
        this.userNkey = userNkey;
        return this;
    }

    public AuthorizationRequest clientInformation(ClientInfo clientInfo) {
        this.clientInfo = clientInfo;
        return this;
    }

    public AuthorizationRequest connectOptions(ConnectOpts connectOpts) {
        this.connectOpts = connectOpts;
        return this;
    }

    public AuthorizationRequest clientTls(ClientTls clientTls) {
        this.clientTls = clientTls;
        return this;
    }

    public AuthorizationRequest requestNonce(String requestNonce) {
        this.requestNonce = requestNonce;
        return this;
    }
}
