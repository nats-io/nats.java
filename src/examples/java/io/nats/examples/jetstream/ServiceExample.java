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
import io.nats.service.Discovery;
import io.nats.service.Service;
import io.nats.service.ServiceDescriptor;
import io.nats.service.api.InfoResponse;
import io.nats.service.api.PingResponse;
import io.nats.service.api.SchemaResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.nats.client.support.JsonUtils.getFormatted;
import static io.nats.client.support.JsonUtils.printFormatted;

public class ServiceExample {

    public static final String ECHO_SERVICE = "EchoService";
    public static final String TIME_SERVICE = "TimeService";

    public static void main(String[] args) throws IOException {

        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .build();

        try (Connection nc = Nats.connect(options)) {
            ServiceDescriptor sd = new ServiceDescriptor(
                ECHO_SERVICE, "An Echo Service", "0.0.1", ECHO_SERVICE,
                "echo schema request string/url", "echo schema response string/url");
            Service service1 = new Service(nc, sd,
                msg -> {
                    byte[] outBytes = ("Echo " + new String(msg.getData())).getBytes();
                    nc.publish(msg.getReplyTo(), outBytes);
                });
            System.out.println(getFormatted(service1));

            ServiceDescriptor sd2 = new ServiceDescriptor(
                TIME_SERVICE, "A Time Service", "0.0.2", TIME_SERVICE,
                "time schema request string/url", "time schema response string/url");
            Service service2 = new Service(nc, sd2,
                msg -> {
                    byte[] outBytes = ("" + System.currentTimeMillis()).getBytes();
                    nc.publish(msg.getReplyTo(), outBytes);
                });
            System.out.println("\n" + getFormatted(service2));

//            rr(nc, "$SRV.PING");
//            String data = rr(nc, "$SRV.PING.EchoService");
//            PingResponse pr = new PingResponse(data);
//            rr(nc, "$SRV.PING.EchoService." + pr.getServiceId());
//
//            rr(nc, "$SRV.INFO");
//            data = rr(nc, "$SRV.INFO.EchoService");
//            InfoResponse in = new InfoResponse(data);
//            rr(nc, "$SRV.INFO.EchoService." + in.getServiceId());
//
//            rr(nc, "$SRV.SCHEMA");
//            data = rr(nc, "$SRV.SCHEMA.EchoService");
//            SchemaResponse sr = new SchemaResponse(data);
//            rr(nc, "$SRV.SCHEMA.EchoService." + sr.getServiceId());
//
//            rr(nc, "$SRV.STATS");
//            data = rr(nc, "$SRV.STATS.EchoService", new StatsRequest(false).serialize());
//            StatsResponse tr = new StatsResponse(data);
//
//            rr(nc, "EchoListener", "Hello".getBytes());
//
//            rr(nc, "$SRV.STATS.EchoService." + tr.getServiceId(), new StatsRequest(true).serialize());

            // ----------------------------------------------------------------------------------------------------
            // Call the services
            // ----------------------------------------------------------------------------------------------------
            callService(nc, ECHO_SERVICE);
            callService(nc, TIME_SERVICE);

            Discovery discovery = new Discovery(nc, 1000, 3);

            // ----------------------------------------------------------------------------------------------------
            // ping discover variations
            // ----------------------------------------------------------------------------------------------------
            report("Ping", "All", discovery.ping());

            List<PingResponse> pings = discovery.ping(ECHO_SERVICE);
            report("Ping", ECHO_SERVICE, pings);

            String echoId = pings.get(0).getServiceId();
            PingResponse ping = discovery.ping(ECHO_SERVICE, echoId);
            report("Ping", ECHO_SERVICE, echoId, ping);

            pings = discovery.ping(TIME_SERVICE);
            report("Ping", TIME_SERVICE, pings);

            String timeId = pings.get(0).getServiceId();
            ping = discovery.ping(TIME_SERVICE, timeId);
            report("Ping", TIME_SERVICE, timeId, ping);

            // ----------------------------------------------------------------------------------------------------
            // info discover variations
            // ----------------------------------------------------------------------------------------------------
            report("Info", "All", discovery.info());

            List<InfoResponse> infos = discovery.info(ECHO_SERVICE);
            report("Info", ECHO_SERVICE, infos);

            InfoResponse info = discovery.info(ECHO_SERVICE, echoId);
            report("Info", ECHO_SERVICE, echoId, info);

            infos = discovery.info(TIME_SERVICE);
            report("Info", TIME_SERVICE, infos);

            info = discovery.info(TIME_SERVICE, timeId);
            report("Info", TIME_SERVICE, timeId, info);

            // ----------------------------------------------------------------------------------------------------
            // schema discover variations
            // ----------------------------------------------------------------------------------------------------
            report("Schema", "All", discovery.schema());

            List<SchemaResponse> schemas = discovery.schema(ECHO_SERVICE);
            report("Schema", ECHO_SERVICE, schemas);

            SchemaResponse schema = discovery.schema(ECHO_SERVICE, echoId);
            report("Schema", ECHO_SERVICE, echoId, schema);

            schemas = discovery.schema(TIME_SERVICE);
            report("Schema", TIME_SERVICE, schemas);

            schema = discovery.schema(TIME_SERVICE, timeId);
            report("Schema", TIME_SERVICE, timeId, schema);

            // ----------------------------------------------------------------------------------------------------
            // schema discover variations
            // ----------------------------------------------------------------------------------------------------
            report("Stats", "All", discovery.stats());
            report("Stats", "All, Internal", discovery.stats(true));

            // ----------------------------------------------------------------------------------------------------
            // stop the service
            // ----------------------------------------------------------------------------------------------------
            service1.stop();
            service2.stop();
            System.out.println();
            System.out.println("service1 done ? " + service1.done().get(1, TimeUnit.SECONDS));
            System.out.println("service2 done ? " + service2.done().get(1, TimeUnit.SECONDS));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void report(String action, String serviceName, String serviceId, Object o) {
        System.out.println("\n" + action  + " " + serviceName + " " + serviceId);
        printFormatted(o);
    }

    @SuppressWarnings("rawtypes")
    private static void report(String action, String label, List objects) {
        System.out.println("\n" + action + " " + label + " [" + objects.size() + "]");
        for (Object o : objects) {
            System.out.println(getFormatted(o));
        }
    }

    private static void callService(Connection nc, String serviceName) throws InterruptedException, ExecutionException {
        CompletableFuture<Message> reply = nc.request(serviceName, null);
        String data = new String(reply.get().getData());
        System.out.println("\nReply from " + serviceName + " -> " + data);
    }
}
