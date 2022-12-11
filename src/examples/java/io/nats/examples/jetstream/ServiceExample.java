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

package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.service.Service;
import io.nats.service.ServiceDescriptor;
import io.nats.service.api.*;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ServiceExample {

    public static void main(String[] args) throws IOException {

        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .build();

        try (Connection nc = Nats.connect(options)) {
            ServiceDescriptor sd = new ServiceDescriptor(
                "EchoService", "An Echo Service", "0.0.1", "EchoListener",
                "schema request text/url", "schema response text/url");
            Service service = new Service(nc, sd,
                msg -> {
                    System.out.println("SERVICE REQUEST: " + msg);
                    byte[] outBytes = ("Echo " + new String(msg.getData())).getBytes();
                    nc.publish(msg.getReplyTo(), outBytes);
            });

            rr(nc, "$SRV.PING");
            String data = rr(nc, "$SRV.PING.EchoService");
            PingResponse pr = new PingResponse(data);
            rr(nc, "$SRV.PING.EchoService." + pr.getServiceId());

            rr(nc, "$SRV.INFO");
            data = rr(nc, "$SRV.INFO.EchoService");
            InfoResponse in = new InfoResponse(data);
            rr(nc, "$SRV.INFO.EchoService." + in.getServiceId());

            rr(nc, "$SRV.SCHEMA");
            data = rr(nc, "$SRV.SCHEMA.EchoService");
            SchemaResponse sr = new SchemaResponse(data);
            rr(nc, "$SRV.SCHEMA.EchoService." + sr.getServiceId());

            rr(nc, "$SRV.STATS");
            data = rr(nc, "$SRV.STATS.EchoService", new StatsRequest(false).serialize());
            StatsResponse tr = new StatsResponse(data);

            rr(nc, "EchoListener", "Hello".getBytes());

            rr(nc, "$SRV.STATS.EchoService." + tr.getServiceId(), new StatsRequest(true).serialize());

            service.stop();

            System.out.println(service.done().get(1, TimeUnit.SECONDS));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String rr(Connection nc, String subject) throws InterruptedException, ExecutionException {
        return rr(nc, subject, null);
    }

    private static String rr(Connection nc, String subject, byte[] body) throws InterruptedException, ExecutionException {
        CompletableFuture<Message> reply = nc.request(subject, body);
        String data = new String(reply.get().getData());
        System.out.println("RECEIVED REPLY: " + subject + " -> " + data);
        return data;
    }
}
