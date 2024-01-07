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

import io.nats.client.support.*;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.jwt.Utils.AUTH_RESPONSE_CLAIM_TYPE;

public class AuthorizationResponse extends GenericClaimFields<AuthorizationResponse> {
    public String jwt;
    public String error;
    public String issuerAccount;

    public AuthorizationResponse() {
        super(AUTH_RESPONSE_CLAIM_TYPE, 2);
    }

    public AuthorizationResponse(String json) throws JsonParseException {
        this(JsonParser.parse(json));
    }

    public AuthorizationResponse(JsonValue jv) {
        super(jv, AUTH_RESPONSE_CLAIM_TYPE, 2);
        jwt = JsonValueUtils.readString(jv, "jwt");
        error = JsonValueUtils.readString(jv, "error");
        issuerAccount = JsonValueUtils.readString(jv, "issuer_account");
    }

    @Override
    protected AuthorizationResponse getThis() {
        return this;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, "jwt", jwt);
        JsonUtils.addField(sb, "error", error);
        JsonUtils.addField(sb, "issuer_account", issuerAccount);
        baseJson(sb);
        return endJson(sb).toString();
    }

    public AuthorizationResponse jwt(String jwt) {
        this.jwt = jwt;
        return this;
    }

    public AuthorizationResponse error(String error) {
        this.error = error;
        return this;
    }

    public AuthorizationResponse issuerAccount(String issuerAccount) {
        this.issuerAccount = issuerAccount;
        return this;
    }
}
