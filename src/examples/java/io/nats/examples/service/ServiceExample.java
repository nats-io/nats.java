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
    public static final String SORT_SERVICE_ROOT_SUBJECT = "sort";
    public static final String SORT_SERVICE_ASCENDING_SUBJECT = "ascending";
    public static final String SORT_SERVICE_DESCENDING_SUBJECT = "descending";

    public static void main(String[] args) throws IOException {

        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .build();

        Supplier<StatsData> sds = new ExampleStatsDataSupplier();
        Function<String, StatsData> sdd = new ExampleStatsDataDecoder();

        try (Connection nc = Nats.connect(options)) {
            // create the services
            ServiceBuilder serviceBuilderEcho = new ServiceBuilder()
                .connection(nc)
                .name(ECHO_SERVICE_NAME)
                .rootSubject(ECHO_SERVICE_SUBJECT)
                .description("An Echo Service")
                .version("0.0.1")
                .schemaRequest("echo schema request string/url")
                .schemaResponse("echo schema response string/url")
                .statsDataHandlers(sds, sdd)
                .rootMessageHandler(msg -> ServiceMessage.reply(nc, msg, "Echo " + new String(msg.getData())));

            Service serviceEcho = serviceBuilderEcho.build();;
            Service serviceAnotherEcho = serviceBuilderEcho.build();

            System.out.println(getFormatted(serviceEcho));
            System.out.println("\n" + getFormatted(serviceAnotherEcho));

            Service serviceSort = new ServiceBuilder()
                .connection(nc)
                .name(SORT_SERVICE_NAME)
                .rootSubject(SORT_SERVICE_ROOT_SUBJECT)
                .description("A Sort Service")
                .version("0.0.2")
                .schemaRequest("sort schema request string/url")
                .schemaResponse("sort schema response string/url")
                .endpoint(SORT_SERVICE_ASCENDING_SUBJECT, msg -> {
                    byte[] data = msg.getData();
                    Arrays.sort(data);
                    ServiceMessage.reply(nc, msg, "Sort Ascending " + new String(data));
                })
                .endpoint(SORT_SERVICE_DESCENDING_SUBJECT, msg -> {
                    byte[] data = msg.getData();
                    Arrays.sort(data);
                    int len = data.length;
                    byte[] reverse = new byte[len];
                    for (int x = 0; x < len; x++) {
                        reverse[x] = data[len - x - 1];
                    }
                    ServiceMessage.reply(nc, msg, "Sort Descending " + new String(reverse));
                })
                .build();

            System.out.println("\n" + getFormatted(serviceSort));

            // ----------------------------------------------------------------------------------------------------
            // Start the services
            // ----------------------------------------------------------------------------------------------------
            CompletableFuture<Boolean> doneEcho = serviceEcho.startService();
            CompletableFuture<Boolean> doneAnotherEcho = serviceAnotherEcho.startService();
            CompletableFuture<Boolean> doneSort = serviceSort.startService();

            // ----------------------------------------------------------------------------------------------------
            // Call the services
            // ----------------------------------------------------------------------------------------------------
            String request = randomText();
            CompletableFuture<Message> reply = nc.request(ECHO_SERVICE_SUBJECT, request.getBytes());
            String response = new String(reply.get().getData());
            System.out.println("\nCalled " + ECHO_SERVICE_SUBJECT + " with [" + request + "] Received [" + response + "]");

            String subject = SORT_SERVICE_ROOT_SUBJECT + "." + SORT_SERVICE_ASCENDING_SUBJECT;
            reply = nc.request(subject, request.getBytes());
            response = new String(reply.get().getData());
            System.out.println("Called " + subject + " with [" + request + "] Received [" + response + "]");

            subject = SORT_SERVICE_ROOT_SUBJECT + "." + SORT_SERVICE_DESCENDING_SUBJECT;
            reply = nc.request(subject, request.getBytes());
            response = new String(reply.get().getData());
            System.out.println("Called " + subject + " with [" + request + "] Received [" + response + "]");

            // ----------------------------------------------------------------------------------------------------
            // discovery
            // ----------------------------------------------------------------------------------------------------
            Discovery discovery = new Discovery(nc, 1000, 3);

            // ----------------------------------------------------------------------------------------------------
            // ping discover variations
            // ----------------------------------------------------------------------------------------------------
            List<PingResponse> pingResponses = discovery.ping();
            printDiscovery("Ping", "[All]", pingResponses);

            pingResponses = discovery.ping(ECHO_SERVICE_NAME);
            printDiscovery("Ping", ECHO_SERVICE_NAME, pingResponses);

            String echoId = pingResponses.get(0).getServiceId();
            PingResponse pingResponse = discovery.ping(ECHO_SERVICE_NAME, echoId);
            printDiscovery("Ping", ECHO_SERVICE_NAME, echoId, pingResponse);

            pingResponses = discovery.ping(SORT_SERVICE_NAME);
            printDiscovery("Ping", SORT_SERVICE_NAME, pingResponses);

            String sortId = pingResponses.get(0).getServiceId();
            pingResponse = discovery.ping(SORT_SERVICE_NAME, sortId);
            printDiscovery("Ping", SORT_SERVICE_NAME, sortId, pingResponse);

            // ----------------------------------------------------------------------------------------------------
            // info discover variations
            // ----------------------------------------------------------------------------------------------------
            List<InfoResponse> infoResponses = discovery.info();
            printDiscovery("Info", "[All]", infoResponses);

            infoResponses = discovery.info(ECHO_SERVICE_NAME);
            printDiscovery("Info", ECHO_SERVICE_NAME, infoResponses);

            InfoResponse infoResponse = discovery.info(ECHO_SERVICE_NAME, echoId);
            printDiscovery("Info", ECHO_SERVICE_NAME, echoId, infoResponse);

            infoResponses = discovery.info(SORT_SERVICE_NAME);
            printDiscovery("Info", SORT_SERVICE_NAME, infoResponses);

            infoResponse = discovery.info(SORT_SERVICE_NAME, sortId);
            printDiscovery("Info", SORT_SERVICE_NAME, sortId, infoResponse);

            // ----------------------------------------------------------------------------------------------------
            // schema discover variations
            // ----------------------------------------------------------------------------------------------------
            List<SchemaResponse> schemaResponses = discovery.schema();
            printDiscovery("Schema", "[All]", schemaResponses);

            schemaResponses = discovery.schema(ECHO_SERVICE_NAME);
            printDiscovery("Schema", ECHO_SERVICE_NAME, schemaResponses);

            SchemaResponse schemaResponse = discovery.schema(ECHO_SERVICE_NAME, echoId);
            printDiscovery("Schema", ECHO_SERVICE_NAME, echoId, schemaResponse);

            schemaResponses = discovery.schema(SORT_SERVICE_NAME);
            printDiscovery("Schema", SORT_SERVICE_NAME, schemaResponses);

            schemaResponse = discovery.schema(SORT_SERVICE_NAME, sortId);
            printDiscovery("Schema", SORT_SERVICE_NAME, sortId, schemaResponse);

            // ----------------------------------------------------------------------------------------------------
            // stats discover variations
            // ----------------------------------------------------------------------------------------------------
            List<StatsResponse> statsResponseList = discovery.stats(sdd);
            printDiscovery("Stats", "[All]", statsResponseList);

            statsResponseList = discovery.stats(ECHO_SERVICE_NAME);
            printDiscovery("Stats", ECHO_SERVICE_NAME, statsResponseList); // will show echo without data decoder

            statsResponseList = discovery.stats(SORT_SERVICE_NAME);
            printDiscovery("Stats", SORT_SERVICE_NAME, statsResponseList);

            // ----------------------------------------------------------------------------------------------------
            // stop the service
            // ----------------------------------------------------------------------------------------------------
            serviceEcho.stop();
            serviceSort.stop();
            serviceAnotherEcho.stop();
            System.out.println("\nEcho service done ? " + doneEcho.get(1, TimeUnit.SECONDS));
            System.out.println("Another Echo Service done ? " + doneAnotherEcho.get(1, TimeUnit.SECONDS));
            System.out.println("Sort Service done ? " + doneSort.get(1, TimeUnit.SECONDS));

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
