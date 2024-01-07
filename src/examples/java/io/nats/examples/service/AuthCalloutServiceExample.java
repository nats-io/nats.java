// Copyright 2022 The NATS Authors
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

package io.nats.examples.service;

import io.nats.client.*;
import io.nats.client.impl.Headers;
import io.nats.jwt.*;
import io.nats.service.*;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.nats.jwt.Utils.currentTimeSeconds;
import static io.nats.jwt.Utils.getClaimBody;

/**
 * This example demonstrates basic setup and use of the Service Framework
 */
public class AuthCalloutServiceExample {
    static String ISSUER_NKEY = "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA";
    static String ISSUER_NSEED = "SAANDLKMXL6CUS3CP52WIXBEDN6YJ545GDKC65U5JZPPV6WH6ESWUA6YAI";

    static final Map<String, NatsUser> NATS_USERS;

//    static final NKey USER_KEY;
    static final NKey USER_SIGNING_KEY;
//    static final String PUB_USER_KEY;
    static final String PUB_USER_SIGNING_KEY;

    static {
        try {
//            USER_KEY = NKey.fromSeed(ISSUER_NKEY.toCharArray());
//            PUB_USER_KEY = new String(USER_KEY.getPublicKey());
            USER_SIGNING_KEY = NKey.fromSeed(ISSUER_NSEED.toCharArray());
            PUB_USER_SIGNING_KEY = new String(USER_SIGNING_KEY.getPublicKey());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        // curveNKey = NKey.fromSeed(ISSUER_XSEED.toCharArray());

        // sys/sys, SYS
        // alice/alice, APP
        // bob/bob, APP, pub allow "bob.>", sub allow "bob.>", response max 1
        NATS_USERS = new HashMap<>();
        NATS_USERS.put("sys", new NatsUser().userPass("sys").account("SYS"));
        NATS_USERS.put("alice", new NatsUser().userPass("alice").account("APP"));
        Permission p = new Permission().allow("bob.>");
        ResponsePermission r = new ResponsePermission().max(1);
        NATS_USERS.put("bob", new NatsUser().userPass("bob").account("APP").pub(p).sub(p).resp(r));
    }

    public static void main(String[] args) throws Exception {

        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .userInfo("auth", "auth")
            .build();

        try (Connection nc = Nats.connect(options)) {
            // endpoints can be created ahead of time
            // or created directly by the ServiceEndpoint builder.
            Endpoint epAuthCallout = Endpoint.builder()
                .name("AuthEndpoint")
                .subject("$SYS.REQ.USER.AUTH")
                .build();

            AuthCalloutHandler handler = new AuthCalloutHandler(nc);
            ServiceEndpoint seAuthCallout = ServiceEndpoint.builder()
                .endpoint(epAuthCallout)
                .handler(handler)
                .build();

            // Create the service from service endpoints.
            Service service1 = new ServiceBuilder()
                .connection(nc)
                .name("AuthService")
                .version("0.0.1")
                .addServiceEndpoint(seAuthCallout)
                .build();

            System.out.println("\n" + service1);

            // ----------------------------------------------------------------------------------------------------
            // Start the services
            // ----------------------------------------------------------------------------------------------------
            CompletableFuture<Boolean> serviceStoppedFuture = service1.startService();

            AuthCalloutUserExample.main("alicex", "alice");
            AuthCalloutUserExample.main("alice", "alice");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
    {
        "aud": "nats-authorization-request",
        "jti": "43CCBS2OARGHVNIMBI2IEGVXSCT2L25WNQ4D2BF2T24DWORJLE2A",
        "iat": 1704653960,
        "iss": "NC5WUN42IKPYZQI5EWREZOHGYHZA6XN7UE766VSNBV3AOPTYKAISY6XV",
        "sub": "ABJHLOVMPA4CI6R5KLNGOB4GSLNIY7IOUPAJC4YFNDLQVIOBYQGUWVLA",
        "exp": 1704653962,
        "nats": {
            "client_info": {
                "kind": "Client",
                "host": "127.0.0.1",
                "id": 28,
                "type": "nats",
                "user": "alicex"
            },
            "connect_opts": {
                "protocol": 1,
                "pass": "alice",
                "lang": "java",
                "user": "alicex",
                "version": "2.17.3.dev"
            },
            "server_id": {
                "name": "NC5WUN42IKPYZQI5EWREZOHGYHZA6XN7UE766VSNBV3AOPTYKAISY6XV",
                "host": "0.0.0.0",
                "id": "NC5WUN42IKPYZQI5EWREZOHGYHZA6XN7UE766VSNBV3AOPTYKAISY6XV",
                "version": "2.10.4"
            },
            "type": "authorization_request",
            "version": 2,
            "user_nkey": "UCPGU74LE4GOMJZFFD6Z4QJLSUGUJIQ23OH6IZUMRI3BXITCIPPFGW6D"
        }
    }
     */

    static class AuthCalloutHandler implements ServiceMessageHandler {
        Connection nc;

        public AuthCalloutHandler(Connection nc) {
            this.nc = nc;
        }

        @Override
        public void onMessage(ServiceMessage smsg) {
            System.out.println("\nSubject    : " + smsg.getSubject());
            System.out.println("Headers    : " + hToString(smsg.getHeaders()));

            try {
                // Convert the message data into a Claim
                Claim claim = new Claim(getClaimBody(smsg.getData()));
                System.out.println("Claim      : " + claim.toJson());

                // The Claim should contain an Authorization Request
                AuthorizationRequest ar = claim.authorizationRequest;
                if (ar == null) {
                    System.err.println("Invalid Authorization Request Claim");
                    return;
                }
                System.out.println("Auth Req   : " + ar.toJson());

                // Check if the user exists.
                NatsUser u = NATS_USERS.get(ar.connectOpts.user);
                if (u == null) {
                    respond(smsg, ar, null, "User Not Found: " + ar.connectOpts.user);
                    return;
                }

                UserClaim uc = new UserClaim()
                    .pub(u.pub)
                    .sub(u.sub)
                    .resp(u.resp);

                String userJwt = new ClaimIssuer()
                    .aud(u.account)
                    .name(ar.connectOpts.user)
                    .iat(currentTimeSeconds())
                    .iss(PUB_USER_SIGNING_KEY)
                    .sub(ar.userNkey)
                    .nats(uc)
                    .issueJwt(USER_SIGNING_KEY);

                respond(smsg, ar, userJwt, null);
            }
            catch (Exception e) {
                onException(e);
            }
        }

        private void respond(ServiceMessage smsg,
                             AuthorizationRequest ar,
                             String userJwt,
                             String error) throws GeneralSecurityException, IOException {
            AuthorizationResponse response = new AuthorizationResponse()
                .jwt(userJwt)
                .error(error);
            System.out.println("Auth Resp  : " + response.toJson());

            String jwt = new ClaimIssuer()
                .aud(ar.serverId.id)
                .iat(currentTimeSeconds())
                .iss(PUB_USER_SIGNING_KEY)
                .sub(ar.userNkey)
                .nats(response)
                .issueJwt(USER_SIGNING_KEY);

            System.out.println("Claim Resp : " + getClaimBody(jwt));
            smsg.respond(nc, jwt);
        }
    }

    private static void onException(Exception e) {
        //noinspection CallToPrintStackTrace
        e.printStackTrace();
    }

    static class NatsUser {
        public String user;
        public String pass;
        public String account;
        public Permission pub;
        public Permission sub;
        public ResponsePermission resp;

        public NatsUser userPass(String userPass) {
            this.user = userPass;
            this.pass = userPass;
            return this;
        }

        public NatsUser user(String user) {
            this.user = user;
            return this;
        }

        public NatsUser pass(String pass) {
            this.pass = pass;
            return this;
        }

        public NatsUser account(String account) {
            this.account = account;
            return this;
        }

        public NatsUser pub(Permission pub) {
            this.pub = pub;
            return this;
        }

        public NatsUser sub(Permission sub) {
            this.sub = sub;
            return this;
        }

        public NatsUser resp(ResponsePermission resp) {
            this.resp = resp;
            return this;
        }
    }

    public static String hToString(Headers h) {
        if (h == null || h.isEmpty()) {
            return "None";
        }

        boolean notFirst = false;
        StringBuilder sb = new StringBuilder("[");
        for (String key : h.keySet()) {
            if (notFirst) {
                sb.append(',');
            }
            else {
                notFirst = true;
            }
            sb.append(key).append("=").append(h.get(key));
        }
        return sb.append(']').toString();
    }
}
