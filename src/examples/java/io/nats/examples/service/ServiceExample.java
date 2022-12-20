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
import io.nats.client.support.JsonUtils;
import io.nats.service.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.Validator.nullOrEmpty;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class ServiceExample {
    public static final String ECHO_SERVICE_NAME = "EchoService";
    public static final String SORT_SERVICE_NAME = "SortService";
    public static final String ECHO_SERVICE_SUBJECT = "echo";
    public static final String SORT_SERVICE_SUBJECT = "sort";

    public static void main(String[] args) throws IOException {

        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .build();

        Supplier<StatsData> sds = new ExampleStatsDataSupplier();
        Function<String, StatsData> sdd = new ExampleStatsDataDecoder();

        try (Connection nc = Nats.connect(options)) {
            // create the services
            Service serviceEcho = new ServiceBuilder()
                .connection(nc)
                .name(ECHO_SERVICE_NAME)
                .subject(ECHO_SERVICE_SUBJECT)
                .description("An Echo Service")
                .version("0.0.1")
                .schemaRequest("echo schema request string/url")
                .schemaResponse("echo schema response string/url")
                .statsDataHandlers(sds, sdd)
                .serviceMessageHandler(msg -> ServiceMessage.reply(nc, msg, "Echo " + new String(msg.getData())))
                .build();

            System.out.println(getFormatted(serviceEcho));

            ServiceBuilder serviceBuilderSort = new ServiceBuilder()
                .connection(nc)
                .name(SORT_SERVICE_NAME)
                .subject(SORT_SERVICE_SUBJECT)
                .description("A Sort Service")
                .version("0.0.2")
                .schemaRequest("sort schema request string/url")
                .schemaResponse("sort schema response string/url")
                .serviceMessageHandler(msg -> {
                    byte[] data = msg.getData();
                    Arrays.sort(data);
                    ServiceMessage.reply(nc, msg, "Sort " + new String(data));
                });

            Service serviceSort = serviceBuilderSort.build();
            Service serviceAnotherSort = serviceBuilderSort.build();
            System.out.println("\n" + getFormatted(serviceSort));
            System.out.println("\n" + getFormatted(serviceAnotherSort));

            // ----------------------------------------------------------------------------------------------------
            // Start the services
            // ----------------------------------------------------------------------------------------------------
            CompletableFuture<Boolean> doneEcho = serviceEcho.startService();
            CompletableFuture<Boolean> doneSort = serviceSort.startService();
            CompletableFuture<Boolean> doneAnotherSort = serviceAnotherSort.startService();

            // ----------------------------------------------------------------------------------------------------
            // Call the services
            // ----------------------------------------------------------------------------------------------------
            String request = randomText();
            CompletableFuture<Message> reply = nc.request(ECHO_SERVICE_SUBJECT, request.getBytes());
            String response = new String(reply.get().getData());
            System.out.println("\nCalled " + ECHO_SERVICE_SUBJECT + " with [" + request + "] Received [" + response + "]");

            request = randomText();
            reply = nc.request(SORT_SERVICE_SUBJECT, request.getBytes());
            response = new String(reply.get().getData());
            System.out.println("Called " + SORT_SERVICE_SUBJECT + " with [" + request + "] Received [" + response + "]");

            // ----------------------------------------------------------------------------------------------------
            // discovery
            // ----------------------------------------------------------------------------------------------------
            Discovery discovery = new Discovery(nc, 1000, 3);

            // ----------------------------------------------------------------------------------------------------
            // ping discover variations
            // ----------------------------------------------------------------------------------------------------
            List<Ping> pings = discovery.ping();
            printDiscovery("Ping", "[All]", pings);

            pings = discovery.ping(ECHO_SERVICE_NAME);
            printDiscovery("Ping", ECHO_SERVICE_NAME, pings);

            String echoId = pings.get(0).getServiceId();
            Ping ping = discovery.ping(ECHO_SERVICE_NAME, echoId);
            printDiscovery("Ping", ECHO_SERVICE_NAME, echoId, ping);

            pings = discovery.ping(SORT_SERVICE_NAME);
            printDiscovery("Ping", SORT_SERVICE_NAME, pings);

            String sortId = pings.get(0).getServiceId();
            ping = discovery.ping(SORT_SERVICE_NAME, sortId);
            printDiscovery("Ping", SORT_SERVICE_NAME, sortId, ping);

            // ----------------------------------------------------------------------------------------------------
            // info discover variations
            // ----------------------------------------------------------------------------------------------------
            List<Info> infos = discovery.info();
            printDiscovery("Info", "[All]", infos);

            infos = discovery.info(ECHO_SERVICE_NAME);
            printDiscovery("Info", ECHO_SERVICE_NAME, infos);

            Info info = discovery.info(ECHO_SERVICE_NAME, echoId);
            printDiscovery("Info", ECHO_SERVICE_NAME, echoId, info);

            infos = discovery.info(SORT_SERVICE_NAME);
            printDiscovery("Info", SORT_SERVICE_NAME, infos);

            info = discovery.info(SORT_SERVICE_NAME, sortId);
            printDiscovery("Info", SORT_SERVICE_NAME, sortId, info);

            // ----------------------------------------------------------------------------------------------------
            // schema discover variations
            // ----------------------------------------------------------------------------------------------------
            List<SchemaInfo> schemaInfos = discovery.schema();
            printDiscovery("Schema", "[All]", schemaInfos);

            schemaInfos = discovery.schema(ECHO_SERVICE_NAME);
            printDiscovery("Schema", ECHO_SERVICE_NAME, schemaInfos);

            SchemaInfo schemaInfo = discovery.schema(ECHO_SERVICE_NAME, echoId);
            printDiscovery("Schema", ECHO_SERVICE_NAME, echoId, schemaInfo);

            schemaInfos = discovery.schema(SORT_SERVICE_NAME);
            printDiscovery("Schema", SORT_SERVICE_NAME, schemaInfos);

            schemaInfo = discovery.schema(SORT_SERVICE_NAME, sortId);
            printDiscovery("Schema", SORT_SERVICE_NAME, sortId, schemaInfo);

            // ----------------------------------------------------------------------------------------------------
            // stats discover variations
            // ----------------------------------------------------------------------------------------------------
            List<Stats> statsList = discovery.stats(sdd);
            printDiscovery("Stats", "[All]", statsList);

            statsList = discovery.stats(ECHO_SERVICE_NAME);
            printDiscovery("Stats", ECHO_SERVICE_NAME, statsList); // will show echo without data decoder

            statsList = discovery.stats(SORT_SERVICE_NAME);
            printDiscovery("Stats", SORT_SERVICE_NAME, statsList);

            // ----------------------------------------------------------------------------------------------------
            // stop the service
            // ----------------------------------------------------------------------------------------------------
            serviceEcho.stop();
            serviceSort.stop();
            serviceAnotherSort.stop();
            System.out.println("\nEcho service done ? " + doneEcho.get(1, TimeUnit.SECONDS));
            System.out.println("Sort Service done ? " + doneSort.get(1, TimeUnit.SECONDS));
            System.out.println("Another Sort Service done ? " + doneAnotherSort.get(1, TimeUnit.SECONDS));

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printDiscovery(String action, String serviceName, String serviceId, Object o) {
        System.out.println("\n" + action  + " " + serviceName + " " + serviceId);
        System.out.println("  " + o);
    }

    @SuppressWarnings("rawtypes")
    private static void printDiscovery(String action, String label, List objects) {
        System.out.println("\n" + action + " " + label);
        for (Object o : objects) {
            System.out.println("  " + o);
        }
    }

    public static class ExampleStatsData implements StatsData {
        public String sData;
        public int iData;

        public ExampleStatsData(String sData, int iData) {
            this.sData = sData;
            this.iData = iData;
        }

        public ExampleStatsData(String json) {
            this.sData = JsonUtils.readString(json, string_pattern("sdata"));
            this.iData = JsonUtils.readInt(json, integer_pattern("idata"), -1);
        }

        @Override
        public String toJson() {
            StringBuilder sb = beginJson();
            JsonUtils.addField(sb, "sdata", sData);
            JsonUtils.addField(sb, "idata", iData);
            return endJson(sb).toString();
        }

        @Override
        public String toString() {
            return "ExampleStatsData" + toJson();
        }
    }

    static class ExampleStatsDataSupplier implements Supplier<StatsData> {
        int x = 0;
        @Override
        public StatsData get() {
            ++x;
            return new ExampleStatsData("s-" + hashCode(), x);
        }
    }

    static class ExampleStatsDataDecoder implements Function<String, StatsData> {
        @Override
        public StatsData apply(String json) {
            ExampleStatsData esd = new ExampleStatsData(json);
            return nullOrEmpty(esd.sData) ? null : esd;
        }
    }

    static String randomText() {
        return Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime());
    }
}
