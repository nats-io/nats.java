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

import io.nats.client.*;
import io.nats.service.Endpoint;
import io.nats.service.Service;
import io.nats.service.ServiceBuilder;
import io.nats.service.ServiceEndpoint;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class AuthCalloutServiceExample {

    static final String NATS_URL = "nats://localhost:4222";

    public static void main(String[] args) throws Exception {
        Options options = new Options.Builder()
            .server(NATS_URL)
            .errorListener(new ErrorListener() {})
            .userInfo("auth", "auth")
            .build();

        try (Connection nc = Nats.connect(options)) {
            // endpoints can be created ahead of time
            // or created directly by the ServiceEndpoint builder.
            Endpoint endpoint = Endpoint.builder()
                .name("AuthCallbackEndpoint")
                .subject("$SYS.REQ.USER.AUTH")
                .build();

            AuthCalloutHandler handler = new AuthCalloutHandler(nc);

            ServiceEndpoint serviceEndpoint = ServiceEndpoint.builder()
                .endpoint(endpoint)
                .handler(handler)
                .build();

            // Create the service from service endpoint.
            Service acService = new ServiceBuilder()
                .connection(nc)
                .name("AuthCallbackService")
                .version("0.0.1")
                .addServiceEndpoint(serviceEndpoint)
                .build();

            System.out.println("\n" + acService);

            // ----------------------------------------------------------------------------------------------------
            // Start the services
            // ----------------------------------------------------------------------------------------------------
            CompletableFuture<Boolean> serviceStoppedFuture = acService.startService();

            // \/ Real service will do something like this \/
            // serviceStoppedFuture.get();

            test("alice", "alice", "test", "should connect, publish and receive");
            test("alice", "wrong", "n/a", "should not connect");
            test("bob", "bob", "bob.test", "should connect, publish and receive");
            test("bob", "bob", "test", "should connect, publish but not receive");
        }
        catch (Exception e) {
            //noinspection CallToPrintStackTrace
            e.printStackTrace();
        }
    }

    public static void test(String u, String p, String subject, String behavior) {
        System.out.println("\n--------------------------------------------------------------------------------");
        System.out.println("[TEST] user     : " + u);
        System.out.println("[TEST] subject  : " + subject);
        System.out.println("[TEST] behavior : " + behavior);

        Options options = new Options.Builder()
            .server(NATS_URL)
            .errorListener(new ErrorListener() {})
            .userInfo(u, p)
            .maxReconnects(3)
            .build();

        boolean connected = false;
        try (Connection nc = Nats.connect(options)) {
            System.out.println("[TEST] connected " + u);
            connected = true;

            AtomicBoolean gotMessage = new AtomicBoolean(false);
            Dispatcher d = nc.createDispatcher(m -> {
                System.out.println("[TEST] received message on '" + m.getSubject() + "'");
                gotMessage.set(true);
            });
            d.subscribe(subject);

            nc.publish(subject, (u + "-publish-" + System.currentTimeMillis()).getBytes());
            System.out.println("[TEST] published to '" + subject + "'");

            Thread.sleep(1000); // just giving time for publish to work

            if (!gotMessage.get()) {
                System.out.println("[TEST] no message from '" + subject + "'");
            }
        }
        catch (Exception e) {
            if (connected) {
                System.out.println("[TEST] post connection exception, " + e);
            }
            else {
                System.out.println("[TEST] did not connect, " + e);
            }
        }
    }
}
