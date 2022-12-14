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

import io.nats.client.*;
import io.nats.client.impl.Headers;
import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.impl.NatsMessage;
import io.nats.service.api.*;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.impl.NatsPackageScopeWorkarounds.getDispatchers;
import static io.nats.service.ServiceUtil.*;
import static org.junit.jupiter.api.Assertions.*;

public class ServiceTests extends JetStreamTestBase {
    private static final String ECHO_SERVICE = "EchoService";
    private static final String SORT_SERVICE = "SortService";

    static ServiceDescriptor SD_ECHO = new ServiceDescriptor(
        ECHO_SERVICE, "An Echo Service", "0.0.1", ECHO_SERVICE,
        "echo request string", "echo response string");

    static ServiceDescriptor SD_SORT = new ServiceDescriptor(
        SORT_SERVICE, "A Sort Service", "0.0.1", SORT_SERVICE,
        "sort request", "sort response");

    @Test
    public void testJetStreamContextCreate() throws Exception {

        try (NatsTestServer ts = new NatsTestServer())
        {
            try (Connection serviceNc1 = standardConnection(ts.getURI());
                 Connection serviceNc2 = standardConnection(ts.getURI());
                 Connection clientNc = standardConnection(ts.getURI())) {

                // construction
                Dispatcher dShared = serviceNc1.createDispatcher(); // services can share dispatchers if the user wants to

                // DISCOVERY coverage include no results

                Service echoService1 = new Service(serviceNc1, SD_ECHO, new EchoHandler(11, serviceNc1), null, dShared);
                String echoServiceId1 = verifyServiceCreation(SD_ECHO, echoService1);
                echoService1.setDrainTimeout(DEFAULT_DRAIN_TIMEOUT); // coverage

                Service sortService1 = new Service(serviceNc1, SD_SORT, new SortHandler(21, serviceNc1), dShared, null);
                String sortServiceId1 = verifyServiceCreation(SD_SORT, sortService1);

                Service echoService2 = new Service(serviceNc2, SD_ECHO, new EchoHandler(12, serviceNc2));
                String echoServiceId2 = verifyServiceCreation(SD_ECHO, echoService2);

                Service sortService2 = new Service(serviceNc2, SD_SORT, new SortHandler(22, serviceNc2));
                String sortServiceId2 = verifyServiceCreation(SD_SORT, sortService2);

                assertNotEquals(echoServiceId1, echoServiceId2);
                assertNotEquals(sortServiceId1, sortServiceId2);

                // service request execution
                int requestCount = 10;
                for (int x = 0; x < requestCount; x++) {
                    verifyServiceExecution(clientNc, ECHO_SERVICE);
                    verifyServiceExecution(clientNc, SORT_SERVICE);
                }

                // discovery - wait at most 500 millis for responses, 5 total responses max
                Discovery discovery = new Discovery(clientNc, 500, 5);

                // ping discovery
                DiscoveryVerifier pingValidator = (descriptor, o) -> {
                    assertTrue(o instanceof Ping);
                    Ping pr = (Ping)o;
                    if (descriptor != null) {
                        assertEquals(descriptor.name, pr.getName());
                    }
                    return pr.getServiceId();
                };
                verifyDiscovery(null, discovery.ping(), pingValidator, echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
                verifyDiscovery(SD_ECHO, discovery.ping(ECHO_SERVICE), pingValidator, echoServiceId1, echoServiceId2);
                verifyDiscovery(SD_SORT, discovery.ping(SORT_SERVICE), pingValidator, sortServiceId1, sortServiceId2);
                verifyDiscovery(SD_ECHO, discovery.ping(ECHO_SERVICE, echoServiceId1), pingValidator, echoServiceId1);
                verifyDiscovery(SD_SORT, discovery.ping(SORT_SERVICE, sortServiceId1), pingValidator, sortServiceId1);
                verifyDiscovery(SD_ECHO, discovery.ping(ECHO_SERVICE, echoServiceId2), pingValidator, echoServiceId2);
                verifyDiscovery(SD_SORT, discovery.ping(SORT_SERVICE, sortServiceId2), pingValidator, sortServiceId2);

                // info discovery
                DiscoveryVerifier infoValidator = (descriptor, o) -> {
                    assertTrue(o instanceof Info);
                    Info ir = (Info)o;
                    if (descriptor != null) {
                        assertEquals(descriptor.name, ir.getName());
                        assertEquals(descriptor.description, ir.getDescription());
                        assertEquals(descriptor.version, ir.getVersion());
                        assertEquals(descriptor.subject, ir.getSubject());
                    }
                    return ir.getServiceId();
                };
                verifyDiscovery(null, discovery.info(), infoValidator, echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
                verifyDiscovery(SD_ECHO, discovery.info(ECHO_SERVICE), infoValidator, echoServiceId1, echoServiceId2);
                verifyDiscovery(SD_SORT, discovery.info(SORT_SERVICE), infoValidator, sortServiceId1, sortServiceId2);
                verifyDiscovery(SD_ECHO, discovery.info(ECHO_SERVICE, echoServiceId1), infoValidator, echoServiceId1);
                verifyDiscovery(SD_SORT, discovery.info(SORT_SERVICE, sortServiceId1), infoValidator, sortServiceId1);
                verifyDiscovery(SD_ECHO, discovery.info(ECHO_SERVICE, echoServiceId2), infoValidator, echoServiceId2);
                verifyDiscovery(SD_SORT, discovery.info(SORT_SERVICE, sortServiceId2), infoValidator, sortServiceId2);

                // schema discovery
                DiscoveryVerifier schemaValidator = (descriptor, o) -> {
                    assertTrue(o instanceof SchemaInfo);
                    SchemaInfo sr = (SchemaInfo)o;
                    if (descriptor != null) {
                        assertEquals(descriptor.name, sr.getName());
                        assertEquals(descriptor.version, sr.getVersion());
                        assertEquals(descriptor.schemaRequest, sr.getSchema().getRequest());
                        assertEquals(descriptor.schemaResponse, sr.getSchema().getResponse());
                    }
                    return sr.getServiceId();
                };
                verifyDiscovery(null, discovery.schema(), schemaValidator, echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
                verifyDiscovery(SD_ECHO, discovery.schema(ECHO_SERVICE), schemaValidator, echoServiceId1, echoServiceId2);
                verifyDiscovery(SD_SORT, discovery.schema(SORT_SERVICE), schemaValidator, sortServiceId1, sortServiceId2);
                verifyDiscovery(SD_ECHO, discovery.schema(ECHO_SERVICE, echoServiceId1), schemaValidator, echoServiceId1);
                verifyDiscovery(SD_SORT, discovery.schema(SORT_SERVICE, sortServiceId1), schemaValidator, sortServiceId1);
                verifyDiscovery(SD_ECHO, discovery.schema(ECHO_SERVICE, echoServiceId2), schemaValidator, echoServiceId2);
                verifyDiscovery(SD_SORT, discovery.schema(SORT_SERVICE, sortServiceId2), schemaValidator, sortServiceId2);

                // stats discovery
                discovery = new Discovery(clientNc); // coverage for the simple constructor
                List<Stats> srList = discovery.stats();
                assertEquals(4, srList.size());
                int responseEcho = 0;
                int responseSort = 0;
                int requestsEcho = 0;
                int requestsSort = 0;
                for (Stats sr : srList) {
                    assertEquals(sr.getName(), sr.getName());
                    if (sr.getName().equals(ECHO_SERVICE)) {
                        responseEcho++;
                        requestsEcho += sr.getNumRequests();
                    }
                    else {
                        responseSort++;
                        requestsSort += sr.getNumRequests();
                    }
                }
                assertEquals(2, responseEcho);
                assertEquals(2, responseSort);
                assertEquals(requestCount, requestsEcho);
                assertEquals(requestCount, requestsSort);

                // stats one specific instance so I can also test reset
                Stats sr = discovery.stats(ECHO_SERVICE, echoServiceId1);
                assertEquals(echoServiceId1, sr.getServiceId());
                assertEquals(SD_ECHO.version, sr.getVersion());

                // reset stats
                echoService1.reset();
                sr = echoService1.stats();
                assertEquals(0, sr.getNumRequests());
                assertEquals(0, sr.getNumErrors());
                assertEquals(0, sr.getTotalProcessingTime());
                assertEquals(0, sr.getAverageProcessingTime());

                sr = discovery.stats(ECHO_SERVICE, echoServiceId1);
                assertEquals(0, sr.getNumRequests());
                assertEquals(0, sr.getNumErrors());
                assertEquals(0, sr.getTotalProcessingTime());
                assertEquals(0, sr.getAverageProcessingTime());

                // shutdown
                Map<String, Dispatcher> dispatchers = getDispatchers(serviceNc1);
                assertEquals(3, dispatchers.size()); // user supplied plus echo discovery plus sort discovery
                dispatchers = getDispatchers(serviceNc2);
                assertEquals(4, dispatchers.size()); // echo service, echo discovery, sort service, sort discovery

                sortService1.stop();
                sortService1.done().get();
                dispatchers = getDispatchers(serviceNc1);
                assertEquals(2, dispatchers.size()); // user supplied plus echo discovery
                dispatchers = getDispatchers(serviceNc2);
                assertEquals(4, dispatchers.size()); // echo service, echo discovery, sort service, sort discovery

                echoService1.stop(null); // coverage of public void stop(Throwable t)
                sortService1.done().get();
                dispatchers = getDispatchers(serviceNc1);
                assertEquals(1, dispatchers.size()); // user supplied is not managed by the service since it was supplied by the user
                dispatchers = getDispatchers(serviceNc2);
                assertEquals(4, dispatchers.size());  // echo service, echo discovery, sort service, sort discovery

                sortService2.stop(true); // coverage of public void stop(boolean drain)
                sortService2.done().get();
                dispatchers = getDispatchers(serviceNc1);
                assertEquals(1, dispatchers.size()); // no change so just user supplied
                dispatchers = getDispatchers(serviceNc2);
                assertEquals(2, dispatchers.size());  // echo service, echo discovery

                echoService2.stop(new Exception()); // coverage
                sortService2.done().join();
                dispatchers = getDispatchers(serviceNc1);
                assertEquals(1, dispatchers.size()); // no change so user supplied
                dispatchers = getDispatchers(serviceNc2);
                assertEquals(0, dispatchers.size());  // no user supplied
            }
        }
    }

    interface DiscoveryVerifier {
        String verify(ServiceDescriptor expected, Object o);
    }

    private static void verifyDiscovery(ServiceDescriptor expectedSd, Object object, DiscoveryVerifier idEx, String... expectedIds) {
        verifyDiscovery(expectedSd, Collections.singletonList(object), idEx, expectedIds);
    }

    @SuppressWarnings("rawtypes") // verifyDiscovery
    private static void verifyDiscovery(ServiceDescriptor expectedSd, List objects, DiscoveryVerifier dv, String... expectedIds) {
        List<String> expectedList = Arrays.asList(expectedIds);
        assertEquals(expectedList.size(), objects.size());
        for (Object o : objects) {
            String id = dv.verify(expectedSd, o);
            assertTrue(expectedList.contains(id));
        }
    }

    private static void verifyServiceExecution(Connection nc, String serviceName) {
        try {
            String request = Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()); // just some random text
            CompletableFuture<Message> future = nc.request(serviceName, request.getBytes());
            Message m = future.get();
            String response = new String(m.getData());
            String expected = serviceName.equals(ECHO_SERVICE) ? echo(request.getBytes()) : sort(request.getBytes());
            assertEquals(expected, response);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String verifyServiceCreation(ServiceDescriptor sd, Service service) {
        Info info = service.info();
        String id = info.getServiceId();
        assertNotNull(id);
        assertEquals(sd.name, info.getName());
        assertEquals(sd.version, info.getVersion());
        assertEquals(sd.subject, info.getSubject());
        assertNotNull(service.toString()); // coverage
        return id;
    }

    static class EchoHandler implements MessageHandler {
        int handlerId;
        Connection conn;

        public EchoHandler(int handlerId, Connection conn) {
            this.handlerId = handlerId;
            this.conn = conn;
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            conn.publish(NatsMessage.builder()
                .subject(msg.getReplyTo())
                .data(echo(msg.getData()))
                .headers(new Headers().put("handlerId", Integer.toString(handlerId)))
                .build());
        }
    }

    static class SortHandler implements MessageHandler {
        int handlerId;
        Connection conn;

        public SortHandler(int handlerId, Connection conn) {
            this.handlerId = handlerId;
            this.conn = conn;
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            conn.publish(NatsMessage.builder()
                .subject(msg.getReplyTo())
                .data(sort(msg.getData()))
                .headers(new Headers().put("handlerId", Integer.toString(handlerId)))
                .build());
        }
    }

    private static String echo(byte[] data) {
        return "Echo " + new String(data);
    }

    private static String sort(byte[] data) {
        Arrays.sort(data);
        return "Sort " + new String(data);
    }

    @Test
    public void testServiceDescriptorValidation() {
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor(null, "desc", "version", "subject", null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("", "desc", "version", "subject", null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "", "subject", null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", null, "subject", null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", "", null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", null, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", "this is not value", null, null));

        new ServiceDescriptor("name", "desc", "version", PLAIN); // constructor coverage
        new ServiceDescriptor("name", "desc", "version", PLAIN, null, null);
        new ServiceDescriptor("name", "desc", "version", HAS_DASH, null, null);
        new ServiceDescriptor("name", "desc", "version", HAS_UNDER, null, null);

        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", HAS_SPACE, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", HAS_PRINTABLE, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", HAS_DOT, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", HAS_STAR, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", HAS_GT, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", HAS_DOLLAR, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", HAS_LOW, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", HAS_127, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", HAS_FWD_SLASH, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", HAS_BACK_SLASH, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", HAS_EQUALS, null, null));
        assertThrows(IllegalArgumentException.class, () -> new ServiceDescriptor("name", "desc", "version", HAS_TIC, null, null));
    }

    @Test
    public void testServiceUtil() {
        assertEquals("$SRV.PING", toDiscoverySubject(PING, null, null));
        assertEquals("$SRV.PING.myservice", toDiscoverySubject(PING, "myservice", null));
        assertEquals("$SRV.PING.myservice.123", toDiscoverySubject(PING, "myservice", "123"));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testApiCoverage() {
        new Ping("id", "name").toString();
        new Schema("request", "response").toString();
        new Info("id", SD_ECHO).toString();
        assertNull(Schema.optionalInstance("{}"));

        ServiceDescriptor sd = new ServiceDescriptor("name", "desc", "version", "subject", "", null);
        SchemaInfo sch = new SchemaInfo("serviceId", sd);
        assertNull(sch.getSchema());

        sd = new ServiceDescriptor("name", "desc", "version", "subject", "request", null);
        sch = new SchemaInfo("serviceId", sd);
        assertNotNull(sch.getSchema());

        sd = new ServiceDescriptor("name", "desc", "version", "subject", "", "response");
        sch = new SchemaInfo("serviceId", sd);
        assertNotNull(sch.getSchema());
    }

    @Test
    public void testApiJsonInOut() {
        Ping pr1 = new Ping("{\"name\":\"ServiceName\",\"id\":\"serviceId\"}");
        Ping pr2 = new Ping(pr1.toJson());
        assertEquals("ServiceName", pr1.getName());
        assertEquals("serviceId", pr1.getServiceId());
        assertEquals(pr1.getName(), pr2.getName());
        assertEquals(pr1.getServiceId(), pr2.getServiceId());

        Info ir1 = new Info("{\"name\":\"ServiceName\",\"id\":\"serviceId\",\"description\":\"desc\",\"version\":\"0.0.1\",\"subject\":\"ServiceSubject\"}");
        Info ir2 = new Info(ir1.toJson());
        assertEquals("ServiceName", ir1.getName());
        assertEquals("serviceId", ir1.getServiceId());
        assertEquals("desc", ir1.getDescription());
        assertEquals("0.0.1", ir1.getVersion());
        assertEquals("ServiceSubject", ir1.getSubject());
        assertEquals(ir1.getName(), ir2.getName());
        assertEquals(ir1.getServiceId(), ir2.getServiceId());
        assertEquals(ir1.getDescription(), ir2.getDescription());
        assertEquals(ir1.getVersion(), ir2.getVersion());
        assertEquals(ir1.getSubject(), ir2.getSubject());

        SchemaInfo sr1 = new SchemaInfo("{\"name\":\"ServiceName\",\"id\":\"serviceId\",\"version\":\"0.0.1\",\"schema\":{\"request\":\"rqst\",\"response\":\"rspns\"}}");
        SchemaInfo sr2 = new SchemaInfo(sr1.toJson());
        assertEquals("ServiceName", sr1.getName());
        assertEquals("serviceId", sr1.getServiceId());
        assertEquals("0.0.1", sr1.getVersion());
        assertEquals("rqst", sr1.getSchema().getRequest());
        assertEquals("rspns", sr1.getSchema().getResponse());
        assertEquals(sr1.getName(), sr2.getName());
        assertEquals(sr1.getServiceId(), sr2.getServiceId());
        assertEquals(sr1.getVersion(), sr2.getVersion());
        assertEquals(sr1.getSchema().getRequest(), sr2.getSchema().getRequest());
        assertEquals(sr1.getSchema().getResponse(), sr2.getSchema().getResponse());

        sr1 = new SchemaInfo("{\"name\":\"ServiceName\",\"id\":\"serviceId\",\"version\":\"0.0.1\"}");
        sr2 = new SchemaInfo(sr1.toJson());
        assertEquals("ServiceName", sr1.getName());
        assertEquals("serviceId", sr1.getServiceId());
        assertEquals("0.0.1", sr1.getVersion());
        assertEquals(sr1.getName(), sr2.getName());
        assertEquals(sr1.getServiceId(), sr2.getServiceId());
        assertEquals(sr1.getVersion(), sr2.getVersion());
        assertNull(sr1.getSchema());
        assertNull(sr2.getSchema());

        Stats stats1 = new Stats("{\"name\":\"ServiceName\",\"id\":\"serviceId\",\"version\":\"0.0.1\",\"num_requests\":1,\"num_errors\":2,\"last_error\":\"lastErr\",\"total_processing_time\":3,\"average_processing_time\":4}");
        Stats stats2 = new Stats(stats1.toJson());
        assertEquals("ServiceName", stats1.getName());
        assertEquals("serviceId", stats1.getServiceId());
        assertEquals("0.0.1", stats1.getVersion());
        assertEquals(stats1.getName(), stats2.getName());
        assertEquals(stats1.getServiceId(), stats2.getServiceId());
        assertEquals(stats1.getVersion(), stats2.getVersion());
        assertEquals(1, stats1.getNumRequests());
        assertEquals(1, stats2.getNumRequests());
        assertEquals(2, stats1.getNumErrors());
        assertEquals(2, stats2.getNumErrors());
        assertEquals("lastErr", stats1.getLastError());
        assertEquals("lastErr", stats2.getLastError());
        assertEquals(3, stats1.getTotalProcessingTime());
        assertEquals(3, stats2.getTotalProcessingTime());
        assertEquals(4, stats1.getAverageProcessingTime());
        assertEquals(4, stats2.getAverageProcessingTime());
    }
}
