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
