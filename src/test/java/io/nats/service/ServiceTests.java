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

package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.NatsTestServer;
import io.nats.client.impl.Headers;
import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static io.nats.client.support.JsonUtils.toKey;
import static io.nats.client.support.JsonValueUtils.readInteger;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.service.ServiceMessage.NATS_SERVICE_ERROR;
import static io.nats.service.ServiceMessage.NATS_SERVICE_ERROR_CODE;
import static io.nats.service.ServiceUtil.SRV_PING;
import static org.junit.jupiter.api.Assertions.*;

public class ServiceTests extends JetStreamTestBase {
    public static final String SERVICE_NAME_1 = "Service1";
    public static final String SERVICE_NAME_2 = "Service2";
    public static final String ECHO_ENDPOINT_NAME = "EchoEndpoint";
    public static final String ECHO_ENDPOINT_SUBJECT = "echo";
    public static final String SORT_GROUP = "sort";
    public static final String SORT_ENDPOINT_ASCENDING_NAME = "SortEndpointAscending";
    public static final String SORT_ENDPOINT_DESCENDING_NAME = "SortEndpointDescending";
    public static final String SORT_ENDPOINT_ASCENDING_SUBJECT = "ascending";
    public static final String SORT_ENDPOINT_DESCENDING_SUBJECT = "descending";

    @Test
    public void testService() throws Exception {
        try (NatsTestServer ts = new NatsTestServer())
        {
            try (Connection serviceNc1 = standardConnection(ts.getURI());
                 Connection serviceNc2 = standardConnection(ts.getURI());
                 Connection clientNc = standardConnection(ts.getURI())) {

                Supplier<JsonValue> sds = new TestStatsDataSupplier();

                Endpoint endEcho = Endpoint.builder()
                    .name(ECHO_ENDPOINT_NAME)
                    .subject(ECHO_ENDPOINT_SUBJECT)
                    .schemaRequest("echo schema request info")   // optional
                    .schemaResponse("echo schema response info") // optional
                    .build();

                Endpoint endSortA = Endpoint.builder()
                    .name(SORT_ENDPOINT_ASCENDING_NAME)
                    .subject(SORT_ENDPOINT_ASCENDING_SUBJECT)
                    .schemaRequest("sort ascending schema request info")   // optional
                    .schemaResponse("sort ascending schema response info") // optional
                    .build();

                // constructor coverage
                Endpoint endSortD = new Endpoint(SORT_ENDPOINT_DESCENDING_NAME, SORT_ENDPOINT_DESCENDING_SUBJECT);

                // sort is going to be grouped
                Group sortGroup = new Group(SORT_GROUP);

                ServiceEndpoint seEcho1 = ServiceEndpoint.builder()
                    .endpoint(endEcho)
                    .handler(new EchoHandler(serviceNc1))
                    .statsDataSupplier(sds)
                    .build();

                ServiceEndpoint seSortA1 = ServiceEndpoint.builder()
                    .endpoint(endSortA)
                    .handler(new SortHandlerA(serviceNc1))
                    .build();

                ServiceEndpoint seSortD1 = ServiceEndpoint.builder()
                    .endpoint(endSortD)
                    .handler(new SortHandlerA(serviceNc1))
                    .build();

                ServiceEndpoint seEcho2 = ServiceEndpoint.builder()
                    .endpoint(endEcho)
                    .handler(new EchoHandler(serviceNc2))
                    .statsDataSupplier(sds)
                    .build();

                // build variations
                ServiceEndpoint seSortA2 = ServiceEndpoint.builder()
                    .endpointName(endSortA.getName())
                    .endpointSubject(endSortA.getSubject())
                    .endpointSchemaRequest(endSortA.getSchema().getRequest())
                    .endpointSchemaResponse(endSortA.getSchema().getResponse())
                    .handler(new SortHandlerA(serviceNc2))
                    .build();

                ServiceEndpoint seSortD2 = ServiceEndpoint.builder()
                    .endpointName(endSortD.getName())
                    .endpointSubject(endSortD.getSubject())
                    .handler(new SortHandlerA(serviceNc2))
                    .build();

                Service service1 = new ServiceBuilder()
                    .name(SERVICE_NAME_1)
                    .version("1.0.0")
                    .connection(serviceNc1)
                    .addServiceEndpoint(seEcho1)
                    .addServiceEndpoint(seSortA1)
                    .addServiceEndpoint(seSortD1)
                    .build();
                String serviceId1 = service1.getId();
                CompletableFuture<Boolean> serverDone1 = service1.startService();

                Service service2 = new ServiceBuilder()
                    .name(SERVICE_NAME_2)
                    .version("1.0.0")
                    .connection(serviceNc1)
                    .addServiceEndpoint(seEcho2)
                    .addServiceEndpoint(seSortA2)
                    .addServiceEndpoint(seSortD2)
                    .build();
                String serviceId2 = service2.getId();
                CompletableFuture<Boolean> serverDone2 = service1.startService();

                assertNotEquals(serviceId1, serviceId2);

                // service request execution
                int requestCount = 10;
                for (int x = 0; x < requestCount; x++) {
                    verifyServiceExecution(clientNc, ECHO_ENDPOINT_NAME, ECHO_ENDPOINT_SUBJECT);
                    verifyServiceExecution(clientNc, SORT_ENDPOINT_ASCENDING_NAME, SORT_ENDPOINT_ASCENDING_SUBJECT);
                    verifyServiceExecution(clientNc, SORT_ENDPOINT_DESCENDING_NAME, SORT_ENDPOINT_DESCENDING_SUBJECT);
                }

                PingResponse pingResponse1 = service1.getPingResponse();
                PingResponse pingResponse2 = service2.getPingResponse();
                InfoResponse infoResponse1 = service1.getInfoResponse();
                InfoResponse infoResponse2 = service2.getInfoResponse();
                SchemaResponse schemaResponse1 = service1.getSchemaResponse();
                SchemaResponse schemaResponse2 = service2.getSchemaResponse();

                System.out.println(pingResponse1);
                System.out.println(pingResponse2);
                System.out.println(infoResponse1);
                System.out.println(infoResponse2);
                System.out.println(schemaResponse1);
                System.out.println(schemaResponse2);
                System.out.println(service1.getStatsResponse());
                System.out.println(service2.getStatsResponse());
//
//                // discovery - wait at most 500 millis for responses, 5 total responses max
//                Discovery discovery = new Discovery(clientNc, 500, 5);
//
//                // ping discovery
//                InfoVerifier pingValidator = (expectedInfoResponse, o) -> {
//                    assertTrue(o instanceof PingResponse);
//                    PingResponse p = (PingResponse)o;
//                    if (expectedInfoResponse != null) {
//                        assertEquals(expectedInfoResponse.getName(), p.getName());
//                        assertEquals(PingResponse.TYPE, p.getType());
//                        assertEquals(expectedInfoResponse.getVersion(), p.getVersion());
//                    }
//                    return p.getServiceId();
//                };
//                verifyDiscovery(null, discovery.ping(), pingValidator, echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
//                verifyDiscovery(echoInfoResponse, discovery.ping(ECHO_ENDPOINT_NAME), pingValidator, echoServiceId1, echoServiceId2);
//                verifyDiscovery(sortInfoResponse, discovery.ping(SORT_SERVICE_NAME), pingValidator, sortServiceId1, sortServiceId2);
//                verifyDiscovery(echoInfoResponse, discovery.ping(ECHO_ENDPOINT_NAME, echoServiceId1), pingValidator, echoServiceId1);
//                verifyDiscovery(sortInfoResponse, discovery.ping(SORT_SERVICE_NAME, sortServiceId1), pingValidator, sortServiceId1);
//                verifyDiscovery(echoInfoResponse, discovery.ping(ECHO_ENDPOINT_NAME, echoServiceId2), pingValidator, echoServiceId2);
//                verifyDiscovery(sortInfoResponse, discovery.ping(SORT_SERVICE_NAME, sortServiceId2), pingValidator, sortServiceId2);
//
//                // info discovery
//                InfoVerifier infoValidator = (expectedInfoResponse, o) -> {
//                    assertTrue(o instanceof InfoResponse);
//                    InfoResponse i = (InfoResponse)o;
//                    if (expectedInfoResponse != null) {
//                        assertEquals(expectedInfoResponse.getName(), i.getName());
//                        assertEquals(InfoResponse.TYPE, i.getType());
//                        assertEquals(expectedInfoResponse.getDescription(), i.getDescription());
//                        assertEquals(expectedInfoResponse.getVersion(), i.getVersion());
//                        assertEquals(expectedInfoResponse.getSubject(), i.getSubject());
//                    }
//                    return i.getServiceId();
//                };
//                verifyDiscovery(null, discovery.info(), infoValidator, echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
//                verifyDiscovery(echoInfoResponse, discovery.info(ECHO_ENDPOINT_NAME), infoValidator, echoServiceId1, echoServiceId2);
//                verifyDiscovery(sortInfoResponse, discovery.info(SORT_SERVICE_NAME), infoValidator, sortServiceId1, sortServiceId2);
//                verifyDiscovery(echoInfoResponse, discovery.info(ECHO_ENDPOINT_NAME, echoServiceId1), infoValidator, echoServiceId1);
//                verifyDiscovery(sortInfoResponse, discovery.info(SORT_SERVICE_NAME, sortServiceId1), infoValidator, sortServiceId1);
//                verifyDiscovery(echoInfoResponse, discovery.info(ECHO_ENDPOINT_NAME, echoServiceId2), infoValidator, echoServiceId2);
//                verifyDiscovery(sortInfoResponse, discovery.info(SORT_SERVICE_NAME, sortServiceId2), infoValidator, sortServiceId2);
//
//                // schema discovery
//                SchemaInfoVerifier schemaValidator = (expectedSchemaResponse, o) -> {
//                    assertTrue(o instanceof SchemaResponse);
//                    SchemaResponse sr = (SchemaResponse)o;
//                    if (expectedSchemaResponse != null) {
//                        assertEquals(SchemaResponse.TYPE, sr.getType());
//                        assertEquals(expectedSchemaResponse.getName(), sr.getName());
//                        assertEquals(expectedSchemaResponse.getVersion(), sr.getVersion());
//                        assertEquals(expectedSchemaResponse.getSchema().getRequest(), sr.getSchema().getRequest());
//                        assertEquals(expectedSchemaResponse.getSchema().getResponse(), sr.getSchema().getResponse());
//                    }
//                    return sr.getServiceId();
//                };
//                verifyDiscovery(null, discovery.schema(), schemaValidator, echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
//                verifyDiscovery(echoSchemaResponse, discovery.schema(ECHO_ENDPOINT_NAME), schemaValidator, echoServiceId1, echoServiceId2);
//                verifyDiscovery(sortSchemaResponse, discovery.schema(SORT_SERVICE_NAME), schemaValidator, sortServiceId1, sortServiceId2);
//                verifyDiscovery(echoSchemaResponse, discovery.schema(ECHO_ENDPOINT_NAME, echoServiceId1), schemaValidator, echoServiceId1);
//                verifyDiscovery(sortSchemaResponse, discovery.schema(SORT_SERVICE_NAME, sortServiceId1), schemaValidator, sortServiceId1);
//                verifyDiscovery(echoSchemaResponse, discovery.schema(ECHO_ENDPOINT_NAME, echoServiceId2), schemaValidator, echoServiceId2);
//                verifyDiscovery(sortSchemaResponse, discovery.schema(SORT_SERVICE_NAME, sortServiceId2), schemaValidator, sortServiceId2);
//
//                // stats discovery
//                discovery = new Discovery(clientNc); // coverage for the simple constructor
//                List<StatsResponse> srList = discovery.stats(sdd);
//                assertEquals(4, srList.size());
//                int responseEcho = 0;
//                int responseSort = 0;
//                long requestsEcho = 0;
//                long requestsSort = 0;
//                for (StatsResponse statsResponse : srList) {
//                    if (statsResponse.getName().equals(ECHO_ENDPOINT_NAME)) {
//                        responseEcho++;
//                        requestsEcho += statsResponse.getNumRequests();
//                        assertNotNull(statsResponse.getData());
//                        assertTrue(statsResponse.getData() instanceof TestStatsData);
//                    }
//                    else {
//                        responseSort++;
//                        requestsSort += statsResponse.getNumRequests();
//                    }
//                    assertEquals(StatsResponse.TYPE, statsResponse.getType());
//                }
//                assertEquals(2, responseEcho);
//                assertEquals(2, responseSort);
//                assertEquals(requestCount, requestsEcho);
//                assertEquals(requestCount, requestsSort);
//
//                // stats one specific instance so I can also test reset
//                StatsResponse sr = discovery.stats(ECHO_ENDPOINT_NAME, echoServiceId1);
//                assertEquals(echoServiceId1, sr.getServiceId());
//                assertEquals(echoInfoResponse.getVersion(), sr.getVersion());
//
//                // reset stats
//                echoService1.reset();
//                sr = echoService1.getStats();
//                assertEquals(0, sr.getNumRequests());
//                assertEquals(0, sr.getNumErrors());
//                assertEquals(0, sr.getProcessingTime());
//                assertEquals(0, sr.getAverageProcessingTime());
//                assertNull(sr.getData());
//
//                sr = discovery.stats(ECHO_ENDPOINT_NAME, echoServiceId1);
//                assertEquals(0, sr.getNumRequests());
//                assertEquals(0, sr.getNumErrors());
//                assertEquals(0, sr.getProcessingTime());
//                assertEquals(0, sr.getAverageProcessingTime());
//
//                // shutdown
//                Map<String, Dispatcher> dispatchers = getDispatchers(serviceNc1);
//                assertEquals(3, dispatchers.size()); // user supplied plus echo discovery plus sort discovery
//                dispatchers = getDispatchers(serviceNc2);
//                assertEquals(4, dispatchers.size()); // echo service, echo discovery, sort service, sort discovery
//
//                sortService1.stop();
//                sortDone1.get();
//                dispatchers = getDispatchers(serviceNc1);
//                assertEquals(2, dispatchers.size()); // user supplied plus echo discovery
//                dispatchers = getDispatchers(serviceNc2);
//                assertEquals(4, dispatchers.size()); // echo service, echo discovery, sort service, sort discovery
//
//                echoService1.stop(null); // coverage of public void stop(Throwable t)
//                serverDone1.get();
//                dispatchers = getDispatchers(serviceNc1);
//                assertEquals(1, dispatchers.size()); // user supplied is not managed by the service since it was supplied by the user
//                dispatchers = getDispatchers(serviceNc2);
//                assertEquals(4, dispatchers.size());  // echo service, echo discovery, sort service, sort discovery
//
//                sortService2.stop(true); // coverage of public void stop(boolean drain)
//                sortDone2.get();
//                dispatchers = getDispatchers(serviceNc1);
//                assertEquals(1, dispatchers.size()); // no change so just user supplied
//                dispatchers = getDispatchers(serviceNc2);
//                assertEquals(2, dispatchers.size());  // echo service, echo discovery
//
//                echoService2.stop(new Exception()); // coverage
//                assertThrows(ExecutionException.class, echoDone2::get);
//                dispatchers = getDispatchers(serviceNc1);
//                assertEquals(1, dispatchers.size()); // no change so user supplied
//                dispatchers = getDispatchers(serviceNc2);
//                assertEquals(0, dispatchers.size());  // no user supplied
            }
        }
    }

    interface InfoVerifier {
        String verify(InfoResponse expectedInfoResponse, Object o);
    }

    interface SchemaInfoVerifier {
        String verify(SchemaResponse expectedSchemaResponse, Object o);
    }

    private static void verifyDiscovery(InfoResponse expectedInfoResponse, Object object, InfoVerifier iv, String... expectedIds) {
        verifyDiscovery(expectedInfoResponse, Collections.singletonList(object), iv, expectedIds);
    }

    private static void verifyDiscovery(SchemaResponse expectedSchemaResponse, Object object, SchemaInfoVerifier siv, String... expectedIds) {
        verifyDiscovery(expectedSchemaResponse, Collections.singletonList(object), siv, expectedIds);
    }

    @SuppressWarnings("rawtypes")
    private static void verifyDiscovery(InfoResponse expectedInfoResponse, List objects, InfoVerifier iv, String... expectedIds) {
        List<String> expectedList = Arrays.asList(expectedIds);
        assertEquals(expectedList.size(), objects.size());
        for (Object o : objects) {
            String id = iv.verify(expectedInfoResponse, o);
            assertTrue(expectedList.contains(id));
        }
    }

    @SuppressWarnings("rawtypes")
    private static void verifyDiscovery(SchemaResponse expectedSchemaResponse, List objects, SchemaInfoVerifier siv, String... expectedIds) {
        List<String> expectedList = Arrays.asList(expectedIds);
        assertEquals(expectedList.size(), objects.size());
        for (Object o : objects) {
            String id = siv.verify(expectedSchemaResponse, o);
            assertTrue(expectedList.contains(id));
        }
    }

    private static void verifyServiceExecution(Connection nc, String endpointName, String serviceSubject) {
        try {
            String request = Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()); // just some random text
            CompletableFuture<Message> future = nc.request(serviceSubject, request.getBytes());
            Message m = future.get();
            String response = new String(m.getData());
            switch (endpointName) {
                case ECHO_ENDPOINT_NAME:
                    assertEquals(echo(request), response);
                    break;
                case SORT_ENDPOINT_ASCENDING_NAME:
                    assertEquals(sortA(request), response);
                    break;
                case SORT_ENDPOINT_DESCENDING_NAME:
                    assertEquals(sortD(request), response);
                    break;
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class EchoHandler implements ServiceMessageHandler {
        Connection conn;

        public EchoHandler(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void onMessage(ServiceMessage smsg) {
            smsg.reply(conn, echo(smsg.getData()), new Headers().put("handlerId", Integer.toString(hashCode())));
        }
    }

    static class SortHandlerA implements ServiceMessageHandler {
        Connection conn;

        public SortHandlerA(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void onMessage(ServiceMessage smsg) {
            smsg.reply(conn, sortA(smsg.getData()), new Headers().put("handlerId", Integer.toString(hashCode())));
        }
    }

    static class SortHandlerD implements ServiceMessageHandler {
        Connection conn;

        public SortHandlerD(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void onMessage(ServiceMessage smsg) {
            smsg.reply(conn, sortD(smsg.getData()), new Headers().put("handlerId", Integer.toString(hashCode())));
        }
    }

    private static String echo(String data) {
        return "Echo " + data;
    }

    private static String echo(byte[] data) {
        return echo(new String(data));
    }

    private static String sortA(byte[] data) {
        Arrays.sort(data);
        return "Sort Ascending " + new String(data);
    }

    private static String sortA(String data) {
        return sortA(data.getBytes(StandardCharsets.UTF_8));
    }

    private static String sortD(byte[] data) {
        Arrays.sort(data);
        int len = data.length;
        byte[] descending = new byte[len];
        for (int x = 0; x < len; x++) {
            descending[x] = data[len - x - 1];
        }
        return "Sort Descending " + new String(descending);
    }

    private static String sortD(String data) {
        return sortA(data.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testHandlerException() throws Exception {
        runInServer(nc -> {
            ServiceEndpoint exServiceEndpoint = ServiceEndpoint.builder()
                .endpointName("exEndpoint")
                .endpointSubject("exSubject")
                .handler(m-> { throw new RuntimeException("handler-problem"); })
                .build();

            Service exService = new ServiceBuilder()
                .connection(nc)
                .name("ExceptionService")
                .version("0.0.1")
                .addServiceEndpoint(exServiceEndpoint)
                .build();
            exService.startService();

            CompletableFuture<Message> future = nc.request("exSubject", null);
            Message m = future.get();
            assertEquals("handler-problem", m.getHeaders().getFirst(NATS_SERVICE_ERROR));
            assertEquals("500", m.getHeaders().getFirst(NATS_SERVICE_ERROR_CODE));
            StatsResponse sr = exService.getStatsResponse();
            EndpointStats es = sr.getEndpointStats().get(0);
            assertEquals(1, es.getNumRequests());
            assertEquals(1, es.getNumErrors());
            assertEquals("java.lang.RuntimeException: handler-problem", es.getLastError());
        });
    }

    @Test
    public void testEndpoint() {
        EqualsVerifier.simple().forClass(Endpoint.class).verify();

        Endpoint e = new Endpoint(PLAIN);
        assertEquals(PLAIN, e.getName());
        assertEquals(PLAIN, e.getSubject());
        assertNull(e.getSchema());
        assertEquals(e, Endpoint.builder().endpoint(e).build());

        e = new Endpoint(PLAIN, SUBJECT);
        assertEquals(PLAIN, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertNull(e.getSchema());
        assertEquals(e, Endpoint.builder().endpoint(e).build());

        e = new Endpoint(PLAIN, SUBJECT, "schema-request", null);
        assertEquals(PLAIN, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertEquals("schema-request", e.getSchema().getRequest());
        assertNull(e.getSchema().getResponse());
        assertEquals(e, Endpoint.builder().endpoint(e).build());

        e = new Endpoint(PLAIN, SUBJECT, null, "schema-response");
        assertEquals(PLAIN, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertNull(e.getSchema().getRequest());
        assertEquals("schema-response", e.getSchema().getResponse());
        assertEquals(e, Endpoint.builder().endpoint(e).build());

        e = Endpoint.builder()
            .name(PLAIN).subject(SUBJECT)
            .schemaRequest("schema-request").schemaResponse("schema-response")
            .build();
        assertEquals(PLAIN, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertEquals("schema-request", e.getSchema().getRequest());
        assertEquals("schema-response", e.getSchema().getResponse());
        assertEquals(e, Endpoint.builder().endpoint(e).build());

        e = Endpoint.builder()
            .name(PLAIN).subject(SUBJECT)
            .schema(e.getSchema())
            .build();
        assertEquals(PLAIN, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertEquals("schema-request", e.getSchema().getRequest());
        assertEquals("schema-response", e.getSchema().getResponse());

        String j = e.toJson();
        assertTrue(j.startsWith("{"));
        assertTrue(j.contains("\"name\":\"plain\""));
        assertTrue(j.contains("\"subject\":\"subject\""));
        assertTrue(j.contains("\"schema\":{"));
        assertTrue(j.contains("\"request\":\"schema-request\""));
        assertTrue(j.contains("\"response\":\"schema-response\""));
        assertEquals(toKey(Endpoint.class) + j, e.toString());

        e = Endpoint.builder()
            .name(PLAIN).subject(SUBJECT)
            .schema(null)
            .build();
        assertEquals(PLAIN, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertNull(e.getSchema());

        // some subject testing
        e = new Endpoint(PLAIN, "foo.>");
        assertEquals("foo.>", e.getSubject());
        e = new Endpoint(PLAIN, "foo.*");
        assertEquals("foo.*", e.getSubject());

        // many names are bad
        assertThrows(IllegalArgumentException.class, () -> new Endpoint((String)null));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(EMPTY));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_PRINTABLE));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_DOT));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_STAR)); // invalid in the middle
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_GT)); // invalid in the middle
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_DOLLAR));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_127));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_FWD_SLASH));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_BACK_SLASH));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_EQUALS));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(HAS_TIC));

        // fewer subjects are bad
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(PLAIN, HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(PLAIN, HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(PLAIN, HAS_127));

        assertThrows(IllegalArgumentException.class, () -> new Endpoint(PLAIN, "foo.>.bar")); // gt is not last segment
    }

    @Test
    public void testSchema() {
        EqualsVerifier.simple().forClass(Schema.class).verify();
        Schema s1 = new Schema("request", "response");
        assertEquals("request", s1.getRequest());
        assertEquals("response", s1.getResponse());

        assertNull(Schema.optionalInstance(null, ""));
        assertNull(Schema.optionalInstance("", null));
        assertNull(Schema.optionalInstance(null));

        Schema s2 = new Schema("request", null);
        assertEquals("request", s2.getRequest());
        assertNull(s2.getResponse());

        s2 = new Schema(null, "response");
        assertNull(s2.getRequest());
        assertEquals("response", s2.getResponse());

        s2 = new Schema(s1.toJsonValue());
        assertEquals(s1, s2);

        s2 = Schema.optionalInstance(s1.toJsonValue());
        assertEquals(s1, s2);

        String j = s1.toJson();
        assertTrue(j.startsWith("{"));
        assertTrue(j.contains("\"request\":\"request\""));
        assertTrue(j.contains("\"response\":\"response\""));
        String s = s1.toString();
        assertTrue(s.startsWith(toKey(Schema.class)));
        assertTrue(s.contains("\"request\":\"request\""));
        assertTrue(s.contains("\"response\":\"response\""));
    }

    @Test
    public void testEndpointStats() {
        JsonValue data = new JsonValue("data");
        EqualsVerifier.simple().forClass(EndpointStats.class)
            .withPrefabValues(JsonValue.class, data, JsonValue.NULL)
            .verify();
        ZonedDateTime zdt = DateTimeUtils.gmtNow();

        EndpointStats es = new EndpointStats("name", "subject", 0, 0, 0, null, null, zdt);
        assertEquals("name", es.getName());
        assertEquals("subject", es.getSubject());
        assertNull(es.getLastError());
        assertNull(es.getData());
        assertEquals(0, es.getNumRequests());
        assertEquals(0, es.getNumErrors());
        assertEquals(0, es.getProcessingTime());
        assertEquals(0, es.getAverageProcessingTime());
        assertEquals(zdt, es.getStarted());

        es = new EndpointStats("name", "subject", 2, 4, 10, "lastError", data, zdt);
        assertEquals("name", es.getName());
        assertEquals("subject", es.getSubject());
        assertEquals("lastError", es.getLastError());
        assertEquals("data", es.getData().string);
        assertEquals(2, es.getNumRequests());
        assertEquals(4, es.getNumErrors());
        assertEquals(10, es.getProcessingTime());
        assertEquals(5, es.getAverageProcessingTime());
        assertEquals(zdt, es.getStarted());

        String j = es.toJson();
        assertTrue(j.startsWith("{"));
        assertTrue(j.contains("\"name\":\"name\""));
        assertTrue(j.contains("\"subject\":\"subject\""));
        assertTrue(j.contains("\"last_error\":\"lastError\""));
        assertTrue(j.contains("\"data\":\"data\""));
        assertTrue(j.contains("\"num_requests\":2"));
        assertTrue(j.contains("\"num_errors\":4"));
        assertTrue(j.contains("\"processing_time\":10"));
        assertTrue(j.contains("\"average_processing_time\":5"));
        assertEquals(toKey(EndpointStats.class) + j, es.toString());
    }

    @Test
    public void testGroup() {
        Group g1 = new Group(subject(1));
        Group g2 = new Group(subject(2));
        assertEquals(subject(1), g1.getName());
        assertEquals(subject(1), g1.getSubject());
        assertEquals(subject(2), g2.getName());
        assertEquals(subject(2), g2.getSubject());
        assertNull(g1.getNext());
        assertNull(g2.getNext());

        g1.appendGroup(g2);
        assertEquals(subject(2), g1.getNext().getName());
        assertNull(g2.getNext());
        assertEquals(subject(1), g1.getName());
        assertEquals(subject(1) + DOT + subject(2), g1.getSubject());
        assertEquals(subject(2), g2.getName());
        assertEquals(subject(2), g2.getSubject());

        g1 = new Group("foo.*");
        assertEquals("foo.*", g1.getName());

        assertThrows(IllegalArgumentException.class, () -> new Group(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> new Group(HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> new Group(HAS_127));
        assertThrows(IllegalArgumentException.class, () -> new Group("foo.>")); // gt is last segment
        assertThrows(IllegalArgumentException.class, () -> new Group("foo.>.bar")); // gt is not last segment

        EqualsVerifier.simple().forClass(Group.class).withPrefabValues(Group.class, g1, g2).verify();
    }

    @Test
    public void testUtilToDiscoverySubject() {
        assertEquals("$SRV.PING", ServiceUtil.toDiscoverySubject(SRV_PING, null, null));
        assertEquals("$SRV.PING.myservice", ServiceUtil.toDiscoverySubject(SRV_PING, "myservice", null));
        assertEquals("$SRV.PING.myservice.123", ServiceUtil.toDiscoverySubject(SRV_PING, "myservice", "123"));
    }

    static class TestServiceResponses extends ServiceResponse {
        static String TYPE = "io.nats.micro.v1.test_response";

        public TestServiceResponses(byte[] jsonBytes) {
            this(parseMessage(jsonBytes));
        }

        public TestServiceResponses(JsonValue jv) {
            super(TYPE, jv);
        }
    }

    @Test
    public void testServiceResponses() {
        PingResponse pr1 = new PingResponse("id", "name", "0.0.0");
        PingResponse pr2 = new PingResponse(pr1.toJson().getBytes());
        validateApiInOutPingResponse(pr1);
        validateApiInOutPingResponse(pr2);
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> new TestServiceResponses(pr1.toJsonValue()));
        assertTrue(iae.getMessage().contains("Invalid type"));

        iae = assertThrows(IllegalArgumentException.class, () -> new TestServiceResponses("{[bad json".getBytes()));
        assertTrue(iae.getMessage().contains("Type cannot be null"));

        String json1 = "{\"id\":\"id\",\"name\":\"name\",\"version\":\"0.0.0\"}";
        iae = assertThrows(IllegalArgumentException.class, () -> new TestServiceResponses(json1.getBytes()));
        assertTrue(iae.getMessage().contains("Type cannot be null"));

        String json2 = "{\"name\":\"name\",\"version\":\"0.0.0\",\"type\":\"io.nats.micro.v1.test_response\"}";
        iae = assertThrows(IllegalArgumentException.class, () -> new TestServiceResponses(json2.getBytes()));
        assertTrue(iae.getMessage().contains("Id cannot be null"));

        String json3 = "{\"id\":\"id\",\"version\":\"0.0.0\",\"type\":\"io.nats.micro.v1.test_response\"}";
        iae = assertThrows(IllegalArgumentException.class, () -> new TestServiceResponses(json3.getBytes()));
        assertTrue(iae.getMessage().contains("Name cannot be null"));

        String json4 = "{\"id\":\"id\",\"name\":\"name\",\"type\":\"io.nats.micro.v1.test_response\"}";
        iae = assertThrows(IllegalArgumentException.class, () -> new TestServiceResponses(json4.getBytes()));
        assertTrue(iae.getMessage().contains("Version cannot be null"));

        InfoResponse ir1 = new InfoResponse("id", "name", "0.0.0", "desc", Arrays.asList("subject1", "subject2"));
        InfoResponse ir2 = new InfoResponse(ir1.toJson().getBytes());
        validateApiInOutInfoResponse(ir1);
        validateApiInOutInfoResponse(ir2);

        List<Endpoint> endpoints = new ArrayList<>();
        endpoints.add(new Endpoint("endName0", "endSubject0", "endSchemaRequest0",  "endSchemaResponse0"));
        endpoints.add(new Endpoint("endName1", "endSubject1", "endSchemaRequest1",  "endSchemaResponse1"));
        SchemaResponse sch1 = new SchemaResponse("id", "name", "0.0.0", "apiUrl", endpoints);
        SchemaResponse sch2 = new SchemaResponse(sch1.toJson().getBytes());
        validateApiInOutSchemaResponse(sch1);
        validateApiInOutSchemaResponse(sch2);

        ZonedDateTime serviceStarted = DateTimeUtils.gmtNow();
        ZonedDateTime[] endStarteds = new ZonedDateTime[2];
        sleep(100); endStarteds[0] = DateTimeUtils.gmtNow();
        sleep(100); endStarteds[1] = DateTimeUtils.gmtNow();

        List<EndpointStats> statsList = new ArrayList<>();
        TestStatsDataSupplier tsds = new TestStatsDataSupplier();
        JsonValue[] data = new JsonValue[]{tsds.get(), tsds.get()};
        statsList.add(new EndpointStats("endName0", "endSubject0", 1000, 0, 10000, "lastError0", data[0], endStarteds[0]));
        statsList.add(new EndpointStats("endName1", "endSubject1", 2000, 10, 10000, "lastError1", data[1], endStarteds[1]));

        StatsResponse stat1 = new StatsResponse(pr1, serviceStarted, statsList);
        StatsResponse stat2 = new StatsResponse(stat1.toJson().getBytes());
        validateApiInOutStatsResponse(stat1, serviceStarted, endStarteds, data);
        validateApiInOutStatsResponse(stat2, serviceStarted, endStarteds, data);

        EqualsVerifier.simple().forClass(PingResponse.class).verify();
        EqualsVerifier.simple().forClass(InfoResponse.class).verify();
        EqualsVerifier.simple().forClass(SchemaResponse.class).verify();
        EqualsVerifier.simple().forClass(StatsResponse.class)
            .withPrefabValues(EndpointStats.class, statsList.get(0), statsList.get(1))
            .verify();
    }

    private static void validateApiInOutStatsResponse(StatsResponse stat, ZonedDateTime serviceStarted, ZonedDateTime[] endStarteds, JsonValue[] data) {
        validateApiInOutServiceResponse(stat, StatsResponse.TYPE);
        assertEquals(serviceStarted, stat.getStarted());
        assertEquals(2, stat.getEndpointStats().size());
        for (int x = 0; x < 2; x++) {
            EndpointStats e = stat.getEndpointStats().get(x);
            assertEquals("endName" + x, e.getName());
            assertEquals("endSubject" + x, e.getSubject());
            long nr = x * 1000 + 1000;
            long errs = x * 10;
            long avg = 10000 / nr;
            assertEquals(nr, e.getNumRequests());
            assertEquals(errs, e.getNumErrors());
            assertEquals(10000, e.getProcessingTime());
            assertEquals(avg, e.getAverageProcessingTime());
            assertEquals("lastError" + x, e.getLastError());
            assertEquals(new TestStatsData(data[x]), new TestStatsData(e.getData()));
            assertEquals(endStarteds[x], e.getStarted());
        }
    }

    private static void validateApiInOutSchemaResponse(SchemaResponse r) {
        validateApiInOutServiceResponse(r, SchemaResponse.TYPE);
        assertEquals("apiUrl", r.getApiUrl());
        assertEquals(2, r.getEndpoints().size());
        for (int x = 0; x < 2; x++) {
            Endpoint e = r.getEndpoints().get(x);
            assertEquals("endName" + x, e.getName());
            assertEquals("endSubject" + x, e.getSubject());
            assertEquals("endSchemaRequest" + x, e.getSchema().getRequest());
            assertEquals("endSchemaResponse" + x, e.getSchema().getResponse());
        }
    }

    private static void validateApiInOutInfoResponse(InfoResponse r) {
        validateApiInOutServiceResponse(r, InfoResponse.TYPE);
        assertEquals("desc", r.getDescription());
        assertEquals(2, r.getSubjects().size());
        assertTrue(r.getSubjects().contains("subject1"));
        assertTrue(r.getSubjects().contains("subject2"));
    }

    private static void validateApiInOutPingResponse(PingResponse r) {
        validateApiInOutServiceResponse(r, PingResponse.TYPE);
    }

    private static void validateApiInOutServiceResponse(ServiceResponse r, String type) {
        assertEquals(type, r.getType());
        assertEquals("id", r.getId());
        assertEquals("name", r.getName());
        assertEquals("0.0.0", r.getVersion());
        String j = r.toJson();
        assertTrue(j.startsWith("{"));
        assertTrue(j.contains("\"type\":\"" + type + "\""));
        assertTrue(j.contains("\"name\":\"name\""));
        assertTrue(j.contains("\"id\":\"id\""));
        assertTrue(j.contains("\"version\":\"0.0.0\""));
        assertEquals(toKey(r.getClass()) + j, r.toString());
    }

    static class TestStatsData implements JsonSerializable {
        public String sData;
        public int iData;

        public TestStatsData(String sData, int iData) {
            this.sData = sData;
            this.iData = iData;
        }

        public TestStatsData(JsonValue jv) {
            sData = readString(jv, "sdata");
            iData = readInteger(jv, "idata", -1);
        }

        @Override
        public String toJson() {
            return JsonUtils.toKey(getClass()) + toJsonValue().toJson();
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestStatsData that = (TestStatsData) o;

            if (iData != that.iData) return false;
            return Objects.equals(sData, that.sData);
        }

        @Override
        public int hashCode() {
            int result = sData != null ? sData.hashCode() : 0;
            result = 31 * result + iData;
            return result;
        }
    }

    static class TestStatsDataSupplier implements Supplier<JsonValue> {
        int x = -1;
        @Override
        public JsonValue get() {
            ++x;
            return new TestStatsData("s-" + x, x).toJsonValue();
        }
    }
}
