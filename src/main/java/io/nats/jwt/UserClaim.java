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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.jwt.Utils.NO_LIMIT;
import static io.nats.jwt.Utils.USER_CLAIM_TYPE;

public class UserClaim extends GenericClaimFields<UserClaim> {
    public String issuerAccount;            // User
    public Permission pub;                  // User/UserPermissionLimits/Permissions
    public Permission sub;                  // User/UserPermissionLimits/Permissions
    public ResponsePermission resp;         // User/UserPermissionLimits/Permissions
    public List<String> src;                // User/UserPermissionLimits/Limits/UserLimits
    public List<TimeRange> timeRanges;      // User/UserPermissionLimits/Limits/UserLimits
    public String locale;                   // User/UserPermissionLimits/Limits/UserLimits
    public long subs = NO_LIMIT;            // User/UserPermissionLimits/Limits/NatsLimits
    public long data = NO_LIMIT;            // User/UserPermissionLimits/Limits/NatsLimits
    public long payload = NO_LIMIT;         // User/UserPermissionLimits/Limits/NatsLimits
    public boolean bearerToken;             // User/UserPermissionLimits
    public List<String> allowedConnectionTypes; // User/UserPermissionLimits

    public UserClaim() {
        super(USER_CLAIM_TYPE, 2);
    }

    public UserClaim(String issuerAccount) {
        super(USER_CLAIM_TYPE, 2);
        this.issuerAccount = issuerAccount;
    }

    public UserClaim(JsonValue jv) {
        super(jv, USER_CLAIM_TYPE, 2);
        issuerAccount = JsonValueUtils.readString(jv, "issuer_account");
        pub = Permission.optionalInstance(JsonValueUtils.readValue(jv, "pub"));
        sub = Permission.optionalInstance(JsonValueUtils.readValue(jv, "sub"));
        resp = ResponsePermission.optionalInstance(JsonValueUtils.readValue(jv, "resp"));
        src = JsonValueUtils.readOptionalStringList(jv, "src");
        timeRanges = TimeRange.optionalListOf(JsonValueUtils.readValue(jv, "times"));
        locale = JsonValueUtils.readString(jv, "times_location");
        subs = JsonValueUtils.readLong(jv, "subs", NO_LIMIT);
        data = JsonValueUtils.readLong(jv, "data", NO_LIMIT);
        payload = JsonValueUtils.readLong(jv, "payload", NO_LIMIT);
        bearerToken = JsonValueUtils.readBoolean(jv, "bearer_token");
        allowedConnectionTypes = JsonValueUtils.readOptionalStringList(jv, "allowed_connection_types");
    }

    @Override
    protected UserClaim getThis() {
        return this;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, "issuer_account", issuerAccount);
        baseJson(sb);
        JsonUtils.addField(sb, "pub", pub);
        JsonUtils.addField(sb, "sub", sub);
        JsonUtils.addField(sb, "resp", resp);
        JsonUtils.addStrings(sb, "src", src);
        JsonUtils.addJsons(sb, "times", timeRanges);
        JsonUtils.addField(sb, "times_location", locale);
        JsonUtils.addFieldWhenGteMinusOne(sb, "subs", subs);
        JsonUtils.addFieldWhenGteMinusOne(sb, "data", data);
        JsonUtils.addFieldWhenGteMinusOne(sb, "payload", payload);
        JsonUtils.addFldWhenTrue(sb, "bearer_token", bearerToken);
        JsonUtils.addStrings(sb, "allowed_connection_types", allowedConnectionTypes);
        return endJson(sb).toString();
    }

    public UserClaim issuerAccount(String issuerAccount) {
        this.issuerAccount = issuerAccount;
        return this;
    }

    public UserClaim pub(Permission pub) {
        this.pub = pub;
        return this;
    }

    public UserClaim sub(Permission sub) {
        this.sub = sub;
        return this;
    }

    public UserClaim resp(ResponsePermission resp) {
        this.resp = resp;
        return this;
    }

    public UserClaim src(String... src) {
        this.src = new ArrayList<>(Arrays.asList(src));
        return this;
    }

    public UserClaim src(List<String> src) {
        this.src = src;
        return this;
    }

    public UserClaim timeRanges(List<TimeRange> times) {
        this.timeRanges = times;
        return this;
    }

    public UserClaim locale(String locale) {
        this.locale = locale;
        return this;
    }

    public UserClaim subs(long subs) {
        this.subs = subs;
        return this;
    }

    public UserClaim data(long data) {
        this.data = data;
        return this;
    }

    public UserClaim payload(long payload) {
        this.payload = payload;
        return this;
    }

    public UserClaim bearerToken(boolean bearerToken) {
        this.bearerToken = bearerToken;
        return this;
    }

    public UserClaim allowedConnectionTypes(String... allowedConnectionTypes) {
        this.allowedConnectionTypes = Arrays.asList(allowedConnectionTypes);
        return this;
    }

    public UserClaim allowedConnectionTypes(List<String> allowedConnectionTypes) {
        this.allowedConnectionTypes = allowedConnectionTypes;
        return this;
    }
}