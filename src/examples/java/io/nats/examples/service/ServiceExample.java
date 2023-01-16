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
import io.nats.client.support.JsonValue;
import io.nats.service.*;
import io.nats.service.api.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class ServiceExample {
    public static final String SERVICE_NAME = "ExampleService";
    public static final String ECHO_ENDPOINT_NAME = "EchoEndpoint";
    public static final String SORT_ENDPOINT_NAME = "SortEndpoint";
    public static final String ECHO_ENDPOINT_SUBJECT = "echo";
    public static final String SORT_GROUP = "sort";
    public static final String SORT_ENDPOINT_ASCENDING_SUBJECT = "ascending";
    public static final String SORT_ENDPOINT_DESCENDING_SUBJECT = "descending";

    public static void main(String[] args) throws IOException {

        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .build();

        Supplier<JsonValue> sds = new ExampleStatsDataSupplier();

        try (Connection nc = Nats.connect(options)) {
            // create the services
            ServiceEndpoint seEcho = ServiceEndpoint.builder()
                .endpoint(Endpoint.builder()
                    .name(ECHO_ENDPOINT_NAME)
                    .subject(ECHO_ENDPOINT_SUBJECT)
                    .schemaRequest("echo schema request string/url")
                    .schemaResponse("echo schema response string/url")
                    .build())
                .handler(msg ->
                    ServiceReplyUtils.reply(nc, msg, "Echo " + new String(msg.getData())))
                .statsDataSupplier(sds)
                .build();

            Group sortGroup = new Group(SORT_GROUP);

            ServiceEndpoint seSortA = ServiceEndpoint.builder()
                .group(sortGroup)
                .endpoint(Endpoint.builder()
                    .name(SORT_ENDPOINT_NAME + SORT_ENDPOINT_ASCENDING_SUBJECT)
                    .subject(SORT_ENDPOINT_ASCENDING_SUBJECT)
                    .schemaRequest("sort ascending schema request string/url")
                    .schemaResponse("sort ascending schema response string/url")
                    .build())
                .handler(msg -> {
                    byte[] data = msg.getData();
                    Arrays.sort(data);
                    ServiceReplyUtils.reply(nc, msg, "Sort Ascending " + new String(data));})
                .build();

            ServiceEndpoint seSortD = ServiceEndpoint.builder()
                .group(sortGroup)
                .endpoint(Endpoint.builder()
                    .name(SORT_ENDPOINT_NAME + SORT_ENDPOINT_DESCENDING_SUBJECT)
                    .subject(SORT_ENDPOINT_DESCENDING_SUBJECT)
                    .schemaRequest("sort descending schema request string/url")
                    .schemaResponse("sort descending schema response string/url")
                    .build())
                .handler(msg -> {
                    byte[] data = msg.getData();
                    Arrays.sort(data);
                    int len = data.length;
                    byte[] reverse = new byte[len];
                    for (int x = 0; x < len; x++) {
                        reverse[x] = data[len - x - 1];
                    }
                    ServiceReplyUtils.reply(nc, msg, "Sort Descending " + new String(reverse));
                })
                .build();

            Service service = new ServiceBuilder()
                .connection(nc)
                .name(SERVICE_NAME)
                .apiUrl(SERVICE_NAME + " Api Url")
                .description(SERVICE_NAME + " Description")
                .version("0.0.1")
                .addServiceEndpoint(seEcho)
                .addServiceEndpoint(seSortA)
                .addServiceEndpoint(seSortD)
                .build();

            System.out.println("\n" + service);

            // ----------------------------------------------------------------------------------------------------
            // Start the services
            // ----------------------------------------------------------------------------------------------------
            CompletableFuture<Boolean> done = service.startService();

            // ----------------------------------------------------------------------------------------------------
            // Call the services
            // ----------------------------------------------------------------------------------------------------
            String request = randomText();
            CompletableFuture<Message> reply = nc.request(ECHO_ENDPOINT_SUBJECT, request.getBytes());
            String response = new String(reply.get().getData());
            System.out.println("\nCalled " + ECHO_ENDPOINT_SUBJECT + " with [" + request + "] Received [" + response + "]");

            String subject = SORT_GROUP + "." + SORT_ENDPOINT_ASCENDING_SUBJECT;
            reply = nc.request(subject, request.getBytes());
            response = new String(reply.get().getData());
            System.out.println("Called " + subject + " with [" + request + "] Received [" + response + "]");

            subject = SORT_GROUP + "." + SORT_ENDPOINT_DESCENDING_SUBJECT;
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

            pingResponses = discovery.ping(ECHO_ENDPOINT_NAME);
            printDiscovery("Ping", ECHO_ENDPOINT_NAME, pingResponses);

            String echoId = pingResponses.get(0).getId();
            PingResponse pingResponse = discovery.ping(ECHO_ENDPOINT_NAME, echoId);
            printDiscovery("Ping", ECHO_ENDPOINT_NAME, echoId, pingResponse);

            pingResponses = discovery.ping(SORT_ENDPOINT_NAME);
            printDiscovery("Ping", SORT_ENDPOINT_NAME, pingResponses);

            String sortId = pingResponses.get(0).getId();
            pingResponse = discovery.ping(SORT_ENDPOINT_NAME, sortId);
            printDiscovery("Ping", SORT_ENDPOINT_NAME, sortId, pingResponse);

            // ----------------------------------------------------------------------------------------------------
            // info discover variations
            // ----------------------------------------------------------------------------------------------------
            List<InfoResponse> infoResponses = discovery.info();
            printDiscovery("Info", "[All]", infoResponses);

            infoResponses = discovery.info(ECHO_ENDPOINT_NAME);
            printDiscovery("Info", ECHO_ENDPOINT_NAME, infoResponses);

            InfoResponse infoResponse = discovery.info(ECHO_ENDPOINT_NAME, echoId);
            printDiscovery("Info", ECHO_ENDPOINT_NAME, echoId, infoResponse);

            infoResponses = discovery.info(SORT_ENDPOINT_NAME);
            printDiscovery("Info", SORT_ENDPOINT_NAME, infoResponses);

            infoResponse = discovery.info(SORT_ENDPOINT_NAME, sortId);
            printDiscovery("Info", SORT_ENDPOINT_NAME, sortId, infoResponse);

            // ----------------------------------------------------------------------------------------------------
            // schema discover variations
            // ----------------------------------------------------------------------------------------------------
            List<SchemaResponse> schemaResponses = discovery.schema();
            printDiscovery("Schema", "[All]", schemaResponses);

            schemaResponses = discovery.schema(ECHO_ENDPOINT_NAME);
            printDiscovery("Schema", ECHO_ENDPOINT_NAME, schemaResponses);

            SchemaResponse schemaResponse = discovery.schema(ECHO_ENDPOINT_NAME, echoId);
            printDiscovery("Schema", ECHO_ENDPOINT_NAME, echoId, schemaResponse);

            schemaResponses = discovery.schema(SORT_ENDPOINT_NAME);
            printDiscovery("Schema", SORT_ENDPOINT_NAME, schemaResponses);

            schemaResponse = discovery.schema(SORT_ENDPOINT_NAME, sortId);
            printDiscovery("Schema", SORT_ENDPOINT_NAME, sortId, schemaResponse);

            // ----------------------------------------------------------------------------------------------------
            // stats discover variations
            // ----------------------------------------------------------------------------------------------------
            List<StatsResponse> statsResponseList = discovery.stats();
            printDiscovery("Stats", "[All]", statsResponseList);

            statsResponseList = discovery.stats(ECHO_ENDPOINT_NAME);
            printDiscovery("Stats", ECHO_ENDPOINT_NAME, statsResponseList); // will show echo without data decoder

            statsResponseList = discovery.stats(SORT_ENDPOINT_NAME);
            printDiscovery("Stats", SORT_ENDPOINT_NAME, statsResponseList);

            // ----------------------------------------------------------------------------------------------------
            // stop the service
            // ----------------------------------------------------------------------------------------------------
            service.stop();
            System.out.println("\nService done ? " + done.get(1, TimeUnit.SECONDS));
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

    public static class ExampleStatsData {
        public String sData;
        public int iData;

        public ExampleStatsData(String sData, int iData) {
            this.sData = sData;
            this.iData = iData;
        }

        public JsonValue toJsonValue() {
            Map<String, JsonValue> map = new HashMap<>();
            map.put("sdata", new JsonValue(sData));
            map.put("idata", new JsonValue(iData));
            return new JsonValue(map);
        }

        @Override
        public String toString() {
            return toJsonValue().toString(getClass());
        }
    }

    static class ExampleStatsDataSupplier implements Supplier<JsonValue> {
        int x = 0;
        @Override
        public JsonValue get() {
            ++x;
            return new ExampleStatsData("s-" + hashCode(), x).toJsonValue();
        }
    }

    static String randomText() {
        return Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime());
    }
}
