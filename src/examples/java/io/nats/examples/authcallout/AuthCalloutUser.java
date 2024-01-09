// Copyright 2024 The NATS Authors
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

package io.nats.examples.authcallout;

import io.nats.jwt.Permission;
import io.nats.jwt.ResponsePermission;

public class AuthCalloutUser {
    public String user;
    public String pass;
    public String account;
    public Permission pub;
    public Permission sub;
    public ResponsePermission resp;

    public AuthCalloutUser userPass(String userPass) {
        this.user = userPass;
        this.pass = userPass;
        return this;
    }

    public AuthCalloutUser user(String user) {
        this.user = user;
        return this;
    }

    public AuthCalloutUser pass(String pass) {
        this.pass = pass;
        return this;
    }

    public AuthCalloutUser account(String account) {
        this.account = account;
        return this;
    }

    public AuthCalloutUser pub(Permission pub) {
        this.pub = pub;
        return this;
    }

    public AuthCalloutUser sub(Permission sub) {
        this.sub = sub;
        return this;
    }

    public AuthCalloutUser resp(ResponsePermission resp) {
        this.resp = resp;
        return this;
    }
}
