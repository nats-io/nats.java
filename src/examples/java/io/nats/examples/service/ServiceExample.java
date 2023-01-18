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
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import io.nats.service.*;

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
    public static final String SERVICE_NAME_1 = "Service1";
    public static final String SERVICE_NAME_2 = "Service2";
    public static final String ECHO_ENDPOINT_NAME = "EchoEndpoint";
    public static final String ECHO_ENDPOINT_SUBJECT = "echo";
    public static final String SORT_GROUP = "sort";
    public static final String SORT_ENDPOINT_ASCENDING_NAME = "SortEndpointAscending";
    public static final String SORT_ENDPOINT_DESCENDING_NAME = "SortEndpointDescending";
    public static final String SORT_ENDPOINT_ASCENDING_SUBJECT = "ascending";
    public static final String SORT_ENDPOINT_DESCENDING_SUBJECT = "descending";

    public static void main(String[] args) throws IOException {

        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .build();

        try (Connection nc = Nats.connect(options)) {
            // endpoints can be created ahead of time
            // or created directly by the ServiceEndpoint builder.
            Endpoint epEcho = Endpoint.builder()
                .name(ECHO_ENDPOINT_NAME)
                .subject(ECHO_ENDPOINT_SUBJECT)
                .schemaRequest("echo schema request info")   // optional
                .schemaResponse("echo schema response info") // optional
                .build();

            // sort is going to be grouped
            Group sortGroup = new Group(SORT_GROUP);

            // 4 service endpoints. 3 in service 1, 1 in service 2
            // - We will reuse an endpoint definition, so we make it ahead of time
            // - For echo, we could have reused a handler as well, if we wanted to.
            ServiceEndpoint seEcho1 = ServiceEndpoint.builder()
                .endpoint(epEcho)
                .handler(msg -> handleEchoMessage(nc, msg, "S1E")) // see below: handleEchoMessage below
                .statsDataSupplier(new ExampleStatsDataSupplier()) // see below: ExampleStatsDataSupplier
                .build();

            ServiceEndpoint seEcho2 = ServiceEndpoint.builder()
                .endpoint(epEcho)
                .handler(msg -> handleEchoMessage(nc, msg, "S2E"))
                .build();

            // you can make the Endpoint directly on the Service Endpoint Builder
            ServiceEndpoint seSort1A = ServiceEndpoint.builder()
                .group(sortGroup)
                .endpointName(SORT_ENDPOINT_ASCENDING_NAME)
                .endpointSubject(SORT_ENDPOINT_ASCENDING_SUBJECT)
                .endpointSchemaRequest("sort ascending schema request info")   // optional
                .endpointSchemaResponse("sort ascending schema response info") // optional
                .handler(msg -> handleSortAscending(nc, msg, "S1A"))
                .build();

            // you can also make an endpoint with a constructor instead of a builder.
            Endpoint endSortD = new Endpoint(SORT_ENDPOINT_DESCENDING_NAME, SORT_ENDPOINT_DESCENDING_SUBJECT);
            ServiceEndpoint seSort1D = ServiceEndpoint.builder()
                .group(sortGroup)
                .endpoint(endSortD)
                .handler(msg -> handlerSortDescending(nc, msg, "S1D"))
                .build();

            // Create the service from service endpoints.
            Service service1 = new ServiceBuilder()
                .connection(nc)
                .name(SERVICE_NAME_1)
                .apiUrl(SERVICE_NAME_1 + " Api Url")          // optional
                .description(SERVICE_NAME_1 + " Description") // optional
                .version("0.0.1")
                .addServiceEndpoint(seEcho1)
                .addServiceEndpoint(seSort1A)
                .addServiceEndpoint(seSort1D)
                .build();

            Service service2 = new ServiceBuilder()
                .connection(nc)
                .name(SERVICE_NAME_2)
                .version("0.0.1")
                .addServiceEndpoint(seEcho2) // another of the echo type
                .build();

            System.out.println("\n" + service1);
            System.out.println("\n" + service2);

            // ----------------------------------------------------------------------------------------------------
            // Start the services
            // ----------------------------------------------------------------------------------------------------
            CompletableFuture<Boolean> done1 = service1.startService();
            CompletableFuture<Boolean> done2 = service2.startService();

            // ----------------------------------------------------------------------------------------------------
            // Call the services
            // ----------------------------------------------------------------------------------------------------
            System.out.println();
            String request = null;
            for (int x = 1; x <= 9; x++) { // run ping a few times to see it hit different services
                request = randomText();
                String subject = ECHO_ENDPOINT_SUBJECT;
                CompletableFuture<Message> reply = nc.request(subject, request.getBytes());
                String response = new String(reply.get().getData());
                System.out.println("" + x + ". Called " + subject + " with [" + request + "] Received " + response);
            }

            String subject = SORT_GROUP + "." + SORT_ENDPOINT_ASCENDING_SUBJECT;
            CompletableFuture<Message> reply = nc.request(subject, request.getBytes());
            String response = new String(reply.get().getData());
            System.out.println("1. Called " + subject + " with [" + request + "] Received " + response);

            subject = SORT_GROUP + "." + SORT_ENDPOINT_DESCENDING_SUBJECT;
            reply = nc.request(subject, request.getBytes());
            response = new String(reply.get().getData());
            System.out.println("1. Called " + subject + " with [" + request + "] Received " + response);

            // ----------------------------------------------------------------------------------------------------
            // discovery
            // ----------------------------------------------------------------------------------------------------
            Discovery discovery = new Discovery(nc, 1000, 3);

            // ----------------------------------------------------------------------------------------------------
            // ping discover variations
            // ----------------------------------------------------------------------------------------------------
            List<PingResponse> pingResponses = discovery.ping();
            printDiscovery("Ping", "[All]", pingResponses);

            pingResponses = discovery.ping(SERVICE_NAME_1);
            printDiscovery("Ping", SERVICE_NAME_1, pingResponses);

            pingResponses = discovery.ping(SERVICE_NAME_2);
            printDiscovery("Ping", SERVICE_NAME_2, pingResponses);

            // ----------------------------------------------------------------------------------------------------
            // info discover variations
            // ----------------------------------------------------------------------------------------------------
            List<InfoResponse> infoResponses = discovery.info();
            printDiscovery("Info", "[All]", infoResponses);

            infoResponses = discovery.info(SERVICE_NAME_1);
            printDiscovery("Info", SERVICE_NAME_1, infoResponses);

            infoResponses = discovery.info(SERVICE_NAME_2);
            printDiscovery("Info", SERVICE_NAME_2, infoResponses);

            // ----------------------------------------------------------------------------------------------------
            // schema discover variations
            // ----------------------------------------------------------------------------------------------------
            List<SchemaResponse> schemaResponsList = discovery.schema();
            printDiscovery("Schema", "[All]", schemaResponsList);

            schemaResponsList = discovery.schema(SERVICE_NAME_1);
            printDiscovery("Schema", SERVICE_NAME_1, schemaResponsList);

            schemaResponsList = discovery.schema(SERVICE_NAME_2);
            printDiscovery("Schema", SERVICE_NAME_2, schemaResponsList);

            // ----------------------------------------------------------------------------------------------------
            // stats discover variations
            // ----------------------------------------------------------------------------------------------------
            List<StatsResponse> statsResponseList = discovery.stats();
            printDiscovery("Stats", "[All]", statsResponseList);

            statsResponseList = discovery.stats(SERVICE_NAME_1);
            printDiscovery("Stats", SERVICE_NAME_1, statsResponseList); // will show echo without data decoder

            statsResponseList = discovery.stats(SERVICE_NAME_2);
            printDiscovery("Stats", SERVICE_NAME_2, statsResponseList);

            // ----------------------------------------------------------------------------------------------------
            // stop the service
            // ----------------------------------------------------------------------------------------------------
            service1.stop();
            service2.stop();
            System.out.println("\nService 1 done ? " + done1.get(1, TimeUnit.SECONDS));
            System.out.println("Service 2 done ? " + done2.get(2, TimeUnit.SECONDS));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static JsonValue replyBody(String label, byte[] data, String handlerId) {
        return JsonValueUtils.mapBuilder()
            .put(label, new String(data))
            .put("hid", handlerId)
            .getJsonValue();
    }

    private static void handlerSortDescending(Connection nc, ServiceMessage smsg, String handlerId) {
        byte[] data = smsg.getData();
        Arrays.sort(data);
        int len = data.length;
        byte[] descending = new byte[len];
        for (int x = 0; x < len; x++) {
            descending[x] = data[len - x - 1];
        }
        smsg.reply(nc, replyBody("sort_descending", descending, handlerId));
    }

    private static void handleSortAscending(Connection nc, ServiceMessage smsg, String handlerId) {
        byte[] ascending = smsg.getData();
        Arrays.sort(ascending);
        smsg.reply(nc, replyBody("sort_ascending", ascending, handlerId));
    }

    private static void handleEchoMessage(Connection nc, ServiceMessage smsg, String handlerId) {
        smsg.reply(nc, replyBody("echo", smsg.getData(), handlerId));
    }

    @SuppressWarnings("rawtypes")
    private static void printDiscovery(String action, String label, List objects) {
        System.out.println("\n" + action + " " + label);
        for (Object o : objects) {
            System.out.println("  " + o);
        }
    }

    public static class ExampleStatsData implements JsonSerializable {
        public String sData;
        public int iData;

        public ExampleStatsData(String sData, int iData) {
            this.sData = sData;
            this.iData = iData;
        }

        @Override
        public String toJson() {
            return toJsonValue().toJson();
        }

        @Override
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
