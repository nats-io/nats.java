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
                CompletableFuture<Boolean> serviceDone1 = service1.startService();

                Service service2 = new ServiceBuilder()
                    .name(SERVICE_NAME_2)
                    .version("1.0.0")
                    .connection(serviceNc2)
                    .addServiceEndpoint(seEcho2)
                    .addServiceEndpoint(seSortA2)
                    .addServiceEndpoint(seSortD2)
                    .build();
                String serviceId2 = service2.getId();
                CompletableFuture<Boolean> serviceDone2 = service2.startService();

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
                StatsResponse statsResponse1 = service1.getStatsResponse();
                StatsResponse statsResponse2 = service2.getStatsResponse();

                assertEquals(serviceId1, pingResponse1.getId());
                assertEquals(serviceId2, pingResponse2.getId());
                assertEquals(serviceId1, infoResponse1.getId());
                assertEquals(serviceId2, infoResponse2.getId());
                assertEquals(serviceId1, schemaResponse1.getId());
                assertEquals(serviceId2, schemaResponse2.getId());
                assertEquals(serviceId1, statsResponse1.getId());
                assertEquals(serviceId2, statsResponse2.getId());

                // this relies on the fact that I load the endpoints up in the service
                // in the same order and the json list comes back ordered
                // expecting 10 responses across each endpoint between 2 services
                for (int x = 0; x < 3; x++) {
                    assertEquals(requestCount,
                        statsResponse1.getEndpointStats().get(x).getNumRequests()
                            + statsResponse2.getEndpointStats().get(x).getNumRequests());
                }

                // discovery - wait at most 500 millis for responses, 5 total responses max
                Discovery discovery = new Discovery(clientNc, 500, 5);

                // ping discovery
                Verifier pingVerifier = (expected, response) -> {
                    assertTrue(response instanceof PingResponse);
                    PingResponse exp = (PingResponse)expected;
                    PingResponse p = (PingResponse)response;
                    assertEquals(PingResponse.TYPE, p.getType());
                    assertEquals(exp.getName(), p.getName());
                    assertEquals(exp.getVersion(), p.getVersion());
                };
                verifyDiscovery(discovery.ping(), pingVerifier, pingResponse1, pingResponse2);
                verifyDiscovery(discovery.ping(SERVICE_NAME_1), pingVerifier, pingResponse1);
                verifyDiscovery(discovery.ping(SERVICE_NAME_2), pingVerifier, pingResponse2);

                // info discovery
                Verifier infoVerifier = (expected, response) -> {
                    assertTrue(response instanceof InfoResponse);
                    InfoResponse exp = (InfoResponse)expected;
                    InfoResponse i = (InfoResponse)response;
                    assertEquals(InfoResponse.TYPE, i.getType());
                    assertEquals(exp.getName(), i.getName());
                    assertEquals(exp.getVersion(), i.getVersion());
                    assertEquals(exp.getDescription(), i.getDescription());
                    assertEquals(exp.getSubjects(), i.getSubjects());
                };
                verifyDiscovery(discovery.info(), infoVerifier, infoResponse1, infoResponse2);
                verifyDiscovery(discovery.info(SERVICE_NAME_1), infoVerifier, infoResponse1);
                verifyDiscovery(discovery.info(SERVICE_NAME_2), infoVerifier, infoResponse2);

                // schema discovery
                Verifier schemaVerifier = (expected, response) -> {
                    SchemaResponse exp = (SchemaResponse)expected;
                    SchemaResponse sr = (SchemaResponse)response;
                    assertEquals(SchemaResponse.TYPE, sr.getType());
                    assertEquals(exp.getName(), sr.getName());
                    assertEquals(exp.getVersion(), sr.getVersion());
                    assertEquals(exp.getApiUrl(), sr.getApiUrl());
                    assertEquals(exp.getEndpoints(), sr.getEndpoints());
                };
                verifyDiscovery(discovery.schema(), schemaVerifier, schemaResponse1, schemaResponse2);
                verifyDiscovery(discovery.schema(SERVICE_NAME_1), schemaVerifier, schemaResponse1);
                verifyDiscovery(discovery.schema(SERVICE_NAME_2), schemaVerifier, schemaResponse2);

                // stats discovery
                Verifier statsVerifier = (expected, response) -> {
                    assertTrue(response instanceof StatsResponse);
                    StatsResponse exp = (StatsResponse)expected;
                    StatsResponse sr = (StatsResponse)response;
                    assertEquals(StatsResponse.TYPE, sr.getType());
                    assertEquals(exp.getName(), sr.getName());
                    assertEquals(exp.getVersion(), sr.getVersion());
                    assertEquals(exp.getStarted(), sr.getStarted());
                    for (int x = 0; x < 3; x++) {
                        EndpointStats es = exp.getEndpointStats().get(x);
                        if (!es.getName().equals(ECHO_ENDPOINT_NAME)) {
                            // echo endpoint has data that will vary
                            assertEquals(es, sr.getEndpointStats().get(x));
                        }
                    }
                };
                discovery = new Discovery(clientNc); // coverage for the simple constructor
                verifyDiscovery(discovery.stats(), statsVerifier, statsResponse1, statsResponse2);
                verifyDiscovery(discovery.stats(SERVICE_NAME_1), statsVerifier, statsResponse1);
                verifyDiscovery(discovery.stats(SERVICE_NAME_2), statsVerifier, statsResponse2);

                // test reset
                ZonedDateTime zdt = DateTimeUtils.gmtNow();
                sleep(1);
                service1.reset();
                StatsResponse sr = service1.getStatsResponse();
                assertTrue(zdt.isBefore(sr.getStarted()));
                for (int x = 0; x < 3; x++) {
                    EndpointStats es = sr.getEndpointStats().get(x);
                    assertEquals(0, es.getNumRequests());
                    assertEquals(0, es.getNumErrors());
                    assertEquals(0, es.getProcessingTime());
                    assertEquals(0, es.getAverageProcessingTime());
                    assertNull(es.getLastError());
                    if (es.getName().equals(ECHO_ENDPOINT_NAME)) {
                        assertNotNull(es.getData());
                    }
                    else {
                        assertNull(es.getData());
                    }
                    assertTrue(zdt.isBefore(es.getStarted()));
                }

                // shutdown
                service1.stop();
                serviceDone1.get();
                service2.stop();
                serviceDone2.get();
            }
        }
    }

    interface Verifier {
        void verify(ServiceResponse expected, Object response);
    }

    @SuppressWarnings("unchecked")
    private static void verifyDiscovery(Object oResponse, Verifier v, ServiceResponse... expectedResponses) {
        List<Object> responses = oResponse instanceof List ? (List<Object>)oResponse : Collections.singletonList(oResponse);
        assertEquals(expectedResponses.length, responses.size());
        for (Object response : responses) {
            ServiceResponse expected = find(expectedResponses, (ServiceResponse)response);
            assertNotNull(expected);
            v.verify(expected, response);
        }
    }

    private static ServiceResponse find(ServiceResponse[] expectedResponses, ServiceResponse response) {
        for (ServiceResponse sr : expectedResponses) {
            if (response.id.equals(sr.id)) {
                return sr;
            }
        }
        return null;
    }

    @SuppressWarnings("rawtypes")
//    private static void verifyDiscovery(SchemaResponse expectedSchemaResponse, List objects, SchemaInfoVerifier siv, String... expectedIds) {
//        List<String> expectedList = Arrays.asList(expectedIds);
//        assertEquals(expectedList.size(), objects.size());
//        for (Object o : objects) {
//            String id = siv.verify(expectedSchemaResponse, o);
//            assertTrue(expectedList.contains(id));
//        }
//    }

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
