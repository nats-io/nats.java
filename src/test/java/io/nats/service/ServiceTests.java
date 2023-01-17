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
import io.nats.client.MessageHandler;
import io.nats.client.impl.Headers;
import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonValue;
import io.nats.service.api.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Supplier;

import static io.nats.client.support.JsonValueUtils.readInteger;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.service.ServiceUtil.PING;
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

//    @Test
//    public void testService() throws Exception {
//        try (NatsTestServer ts = new NatsTestServer())
//        {
//            try (Connection serviceNc1 = standardConnection(ts.getURI());
//                 Connection serviceNc2 = standardConnection(ts.getURI());
//                 Connection clientNc = standardConnection(ts.getURI())) {
//
//                // construction
//                Dispatcher dShared = serviceNc1.createDispatcher(); // services can share dispatchers if the user wants to
//
//                Supplier<JsonValue> sds = new TestStatsDataSupplier();
//
//                Service echoService1 = echoServiceCreator(serviceNc1, new EchoHandler(serviceNc1))
//                    .userServiceDispatcher(dShared)
//                    .statsDataHandlers(sds, sdd)
//                    .build();
//                String echoServiceId1 = echoService1.getId();
//                CompletableFuture<Boolean> echoDone1 = echoService1.startService();
//
//                Service sortService1 = sortServiceCreator(serviceNc1, new SortHandler(serviceNc1))
//                    .userDiscoveryDispatcher(dShared).build();
//                String sortServiceId1 = sortService1.getId();
//                CompletableFuture<Boolean> sortDone1 = sortService1.startService();
//
//                Service echoService2 = echoServiceCreator(serviceNc2, new EchoHandler(serviceNc1))
//                    .statsDataHandlers(sds, sdd)
//                    .build();
//                String echoServiceId2 = echoService2.getId();
//                CompletableFuture<Boolean> echoDone2 = echoService2.startService();
//
//                Service sortService2 = sortServiceCreator(serviceNc2, new SortHandler(serviceNc2)).build();
//                String sortServiceId2 = sortService2.getId();
//                CompletableFuture<Boolean> sortDone2 = sortService2.startService();
//
//                assertNotEquals(echoServiceId1, echoServiceId2);
//                assertNotEquals(sortServiceId1, sortServiceId2);
//
//                // service request execution
//                int requestCount = 10;
//                for (int x = 0; x < requestCount; x++) {
//                    verifyServiceExecution(clientNc, ECHO_SERVICE_NAME, ECHO_SERVICE_SUBJECT);
//                    verifyServiceExecution(clientNc, SORT_SERVICE_NAME, SORT_SERVICE_SUBJECT);
//                }
//
//                InfoResponse echoInfoResponse = echoService1.getInfo();
//                InfoResponse sortInfoResponse = sortService1.getInfo();
//                SchemaResponse echoSchemaResponse = echoService1.getSchemaResponse();
//                SchemaResponse sortSchemaResponse = sortService1.getSchemaResponse();
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
//                verifyDiscovery(echoInfoResponse, discovery.ping(ECHO_SERVICE_NAME), pingValidator, echoServiceId1, echoServiceId2);
//                verifyDiscovery(sortInfoResponse, discovery.ping(SORT_SERVICE_NAME), pingValidator, sortServiceId1, sortServiceId2);
//                verifyDiscovery(echoInfoResponse, discovery.ping(ECHO_SERVICE_NAME, echoServiceId1), pingValidator, echoServiceId1);
//                verifyDiscovery(sortInfoResponse, discovery.ping(SORT_SERVICE_NAME, sortServiceId1), pingValidator, sortServiceId1);
//                verifyDiscovery(echoInfoResponse, discovery.ping(ECHO_SERVICE_NAME, echoServiceId2), pingValidator, echoServiceId2);
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
//                verifyDiscovery(echoInfoResponse, discovery.info(ECHO_SERVICE_NAME), infoValidator, echoServiceId1, echoServiceId2);
//                verifyDiscovery(sortInfoResponse, discovery.info(SORT_SERVICE_NAME), infoValidator, sortServiceId1, sortServiceId2);
//                verifyDiscovery(echoInfoResponse, discovery.info(ECHO_SERVICE_NAME, echoServiceId1), infoValidator, echoServiceId1);
//                verifyDiscovery(sortInfoResponse, discovery.info(SORT_SERVICE_NAME, sortServiceId1), infoValidator, sortServiceId1);
//                verifyDiscovery(echoInfoResponse, discovery.info(ECHO_SERVICE_NAME, echoServiceId2), infoValidator, echoServiceId2);
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
//                verifyDiscovery(echoSchemaResponse, discovery.schema(ECHO_SERVICE_NAME), schemaValidator, echoServiceId1, echoServiceId2);
//                verifyDiscovery(sortSchemaResponse, discovery.schema(SORT_SERVICE_NAME), schemaValidator, sortServiceId1, sortServiceId2);
//                verifyDiscovery(echoSchemaResponse, discovery.schema(ECHO_SERVICE_NAME, echoServiceId1), schemaValidator, echoServiceId1);
//                verifyDiscovery(sortSchemaResponse, discovery.schema(SORT_SERVICE_NAME, sortServiceId1), schemaValidator, sortServiceId1);
//                verifyDiscovery(echoSchemaResponse, discovery.schema(ECHO_SERVICE_NAME, echoServiceId2), schemaValidator, echoServiceId2);
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
//                    if (statsResponse.getName().equals(ECHO_SERVICE_NAME)) {
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
//                StatsResponse sr = discovery.stats(ECHO_SERVICE_NAME, echoServiceId1);
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
//                sr = discovery.stats(ECHO_SERVICE_NAME, echoServiceId1);
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
//                echoDone1.get();
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
//            }
//        }
//    }

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

//    private static void verifyServiceExecution(Connection nc, String serviceName, String serviceSubject) {
//        try {
//            String request = Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()); // just some random text
//            CompletableFuture<Message> future = nc.request(serviceSubject, request.getBytes());
//            Message m = future.get();
//            String response = new String(m.getData());
//            String expected = serviceName.equals(ECHO_SERVICE_NAME) ? echo(request) : sort(request);
//            assertEquals(expected, response);
//        }
//        catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }

    static class EchoHandler implements MessageHandler {
        Connection conn;

        public EchoHandler(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            ServiceReplyUtils.reply(conn, msg, echo(msg.getData()), new Headers().put("handlerId", Integer.toString(hashCode())));
        }
    }

    static class SortHandler implements MessageHandler {
        Connection conn;

        public SortHandler(Connection conn) {
            this.conn = conn;
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            ServiceReplyUtils.reply(conn, msg, sort(msg.getData()), new Headers().put("handlerId", Integer.toString(hashCode())));
        }
    }

    private static String echo(String data) {
        return "Echo " + data;
    }

    private static String echo(byte[] data) {
        return echo(new String(data));
    }

    private static String sort(byte[] data) {
        Arrays.sort(data);
        return "Sort " + new String(data);
    }

    private static String sort(String data) {
        return sort(data.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void testHandlerException() throws Exception {
//        runInServer(nc -> {
//            Service devexService = new ServiceBuilder()
//                .connection(nc)
//                .name("HandlerExceptionService")
//                .subject("hesSubject")
//                .version("0.0.1")
//                .serviceMessageHandler( m-> { throw new RuntimeException("handler-problem"); })
//                .build();
//            devexService.startService();
//
//            CompletableFuture<Message> future = nc.request("hesSubject", null);
//            Message m = future.get();
//            assertEquals("handler-problem", m.getHeaders().getFirst(NATS_SERVICE_ERROR));
//            assertEquals("500", m.getHeaders().getFirst(NATS_SERVICE_ERROR_CODE));
//            assertEquals(1, devexService.getStats().getNumRequests());
//            assertEquals(1, devexService.getStats().getNumErrors());
//            assertEquals("java.lang.RuntimeException: handler-problem", devexService.getStats().getLastError());
//        });
    }

    @Test
    public void testEndpointObjectValidation() {
        Endpoint e = new Endpoint(PLAIN);
        assertEquals(PLAIN, e.getName());
        assertEquals(PLAIN, e.getSubject());
        assertNull(e.getSchema());

        e = new Endpoint(PLAIN, SUBJECT);
        assertEquals(PLAIN, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertNull(e.getSchema());

        e = new Endpoint(PLAIN, SUBJECT, "schema-request", null);
        assertEquals(PLAIN, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertEquals("schema-request", e.getSchema().getRequest());
        assertNull(e.getSchema().getResponse());

        e = new Endpoint(PLAIN, SUBJECT, null, "schema-response");
        assertEquals(PLAIN, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertNull(e.getSchema().getRequest());
        assertEquals("schema-response", e.getSchema().getResponse());

        e = Endpoint.builder()
            .name(PLAIN).subject(SUBJECT)
            .schemaRequest("schema-request").schemaResponse("schema-response")
            .build();

        assertEquals(PLAIN, e.getName());
        assertEquals(SUBJECT, e.getSubject());
        assertEquals("schema-request", e.getSchema().getRequest());
        assertEquals("schema-response", e.getSchema().getResponse());

        // some subject testing
        e = new Endpoint(PLAIN, "foo.>");
        assertEquals("foo.>", e.getSubject());
        e = new Endpoint(PLAIN, "foo.*");
        assertEquals("foo.*", e.getSubject());

        // many names are bad
        assertThrows(IllegalArgumentException.class, () -> new Endpoint(null));
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
    public void testGroup() {
        Group g1 = new Group(subject(1));
        Group g2 = new Group(subject(2));
        assertEquals(subject(1), g1.getName());
        assertEquals(subject(1), g1.getSubject());
        assertEquals(subject(2), g2.getName());
        assertEquals(subject(2), g2.getSubject());
        g1.appendGroup(g2);
        assertEquals(subject(1), g1.getName());
        assertEquals(subject(1) + DOT + subject(2), g1.getSubject());
        assertEquals(subject(2), g2.getName());
        assertEquals(subject(2), g2.getSubject());

        g1 = new Group("foo.*");
        assertEquals("foo.*", g1.getName());

        assertThrows(IllegalArgumentException.class, () -> new Group(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> new Group(HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> new Group(HAS_GT)); // invalid in the middle
        assertThrows(IllegalArgumentException.class, () -> new Group(HAS_127));
        assertThrows(IllegalArgumentException.class, () -> new Group("foo.>")); // gt is last segment
        assertThrows(IllegalArgumentException.class, () -> new Group("foo.>.bar")); // gt is not last segment
    }

    @Test
    public void testUtilToDiscoverySubject() {
        assertEquals("$SRV.PING", ServiceUtil.toDiscoverySubject(PING, null, null));
        assertEquals("$SRV.PING.myservice", ServiceUtil.toDiscoverySubject(PING, "myservice", null));
        assertEquals("$SRV.PING.myservice.123", ServiceUtil.toDiscoverySubject(PING, "myservice", "123"));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testApiCoverage() {
        new PingResponse("id", "name", "version").toString();
        new Schema("request", "response").toString();
        new InfoResponse("id", "name", "description", "version", Arrays.asList("subject1", "subject2"));
        assertNull(Schema.optionalInstance(null));
    }

    @Test
    public void testApiJsonInOut() {
        PingResponse pr1 = new PingResponse("id", "name", "0.0.0");
        PingResponse pr2 = new PingResponse(pr1.toJson().getBytes());
        validateApiInOutServiceResponse(pr1);
        validateApiInOutServiceResponse(pr2);

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
        statsList.add(new EndpointStats("endName0", "endSubject0", 10, 20, 30, 40, "lastError0", data[0], endStarteds[0]));
        statsList.add(new EndpointStats("endName1", "endSubject1", 11, 21, 31, 41, "lastError1", data[1], endStarteds[1]));

        StatsResponse stat1 = new StatsResponse(pr1, serviceStarted, statsList);
        StatsResponse stat2 = new StatsResponse(stat1.toJson().getBytes());
        validateApiInOutStatsResponse(stat1, serviceStarted, endStarteds, data);
        validateApiInOutStatsResponse(stat2, serviceStarted, endStarteds, data);
    }

    private static void validateApiInOutStatsResponse(StatsResponse stat, ZonedDateTime serviceStarted, ZonedDateTime[] endStarteds, JsonValue[] data) {
        validateApiInOutServiceResponse(stat);
        assertEquals(serviceStarted, stat.getStarted());
        assertEquals(2, stat.getEndpointStats().size());
        for (int x = 0; x < 2; x++) {
            EndpointStats e = stat.getEndpointStats().get(x);
            assertEquals("endName" + x, e.getName());
            assertEquals("endSubject" + x, e.getSubject());
            assertEquals(10 + x, e.getNumRequests());
            assertEquals(20 + x, e.getNumErrors());
            assertEquals(30 + x, e.getProcessingTime());
            assertEquals(40 + x, e.getAverageProcessingTime());
            assertEquals("lastError" + x, e.getLastError());
            assertEquals(new TestStatsData(data[x]), new TestStatsData(e.getData()));
            assertEquals(endStarteds[x], e.getStarted());
        }
    }

    private static void validateApiInOutSchemaResponse(SchemaResponse r) {
        validateApiInOutServiceResponse(r);
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
        validateApiInOutServiceResponse(r);
        assertEquals(2, r.getSubjects().size());
        assertTrue(r.getSubjects().contains("subject1"));
        assertTrue(r.getSubjects().contains("subject2"));
    }

    private static void validateApiInOutServiceResponse(ServiceResponse r) {
        assertEquals("id", r.getId());
        assertEquals("name", r.getName());
        assertEquals("0.0.0", r.getVersion());
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
