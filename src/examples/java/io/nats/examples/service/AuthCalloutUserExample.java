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

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.service.ServiceMessage;

import java.io.IOException;

/**
 * This example demonstrates basic setup and use of the Service Framework
 */
public class AuthCalloutUserExample {
    public static void main(String[] args) throws IOException {
        main("alice", "alice");
    }

    public static void main(String u, String p) {
        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .userInfo(u, p)
            .build();

        try (Connection nc = Nats.connect(options)) {
            System.out.println("\nAuthCalloutUserExample --> Connected");
            System.out.println(nc.getServerInfo());
            System.out.println("----------------------------------------------------------------------------------------------------");
        }
        catch (Exception e) {
            System.err.println("\nAuthCalloutUserExample --> Did Not Connect");
            e.printStackTrace();
            System.err.println("----------------------------------------------------------------------------------------------------");
        }
    }

    private static void handleAuthMessage(Connection nc, ServiceMessage smsg) {
        smsg.respond(nc, smsg.getData());
    }
}
