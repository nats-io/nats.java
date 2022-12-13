// Copyright 2020 The NATS Authors
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
import java.util.concurrent.CompletableFuture;

import static io.nats.service.ServiceUtil.*;
import static org.junit.jupiter.api.Assertions.*;

public class ServiceTests extends JetStreamTestBase {
    private static final String ECHO_SERVICE = "EchoService";
    private static final String SORT_SERVICE = "SortService";

    @Test
    public void testJetStreamContextCreate() throws Exception {
        ServiceDescriptor sdEcho = new ServiceDescriptor(
            ECHO_SERVICE, "An Echo Service", "0.0.1", ECHO_SERVICE,
            "echo request string", "echo response string");

        ServiceDescriptor sdSort = new ServiceDescriptor(
            SORT_SERVICE, "A Sort Service", "0.0.1", SORT_SERVICE,
            "sort request", "sort response");

        try (NatsTestServer ts = new NatsTestServer())
        {
            try (Connection serviceNc1 = standardConnection(ts.getURI());
                 Connection serviceNc2 = standardConnection(ts.getURI());
                 Connection clientNc = standardConnection(ts.getURI())) {

                Dispatcher dShared = serviceNc1.createDispatcher();

                Service echoService1 = new Service(serviceNc1, sdEcho, new EchoHandler(11, serviceNc1), null, dShared);
                String echoServiceId1 = verifyServiceCreation(sdEcho, echoService1);

                Service sortService1 = new Service(serviceNc1, sdSort, new SortHandler(21, serviceNc1), null, dShared);
                String sortServiceId1 = verifyServiceCreation(sdSort, sortService1);

                Service echoService2 = new Service(serviceNc1, sdEcho, new EchoHandler(12, serviceNc2));
                String echoServiceId2 = verifyServiceCreation(sdEcho, echoService2);

                Service sortService2 = new Service(serviceNc1, sdSort, new SortHandler(22, serviceNc2));
                String sortServiceId2 = verifyServiceCreation(sdSort, sortService2);

                assertNotEquals(echoServiceId1, echoServiceId2);
                assertNotEquals(sortServiceId1, sortServiceId2);

                int requestCount = 10;
                for (int x = 0; x < requestCount; x++) {
                    verifyServiceExecution(clientNc, ECHO_SERVICE);
                    verifyServiceExecution(clientNc, SORT_SERVICE);
                }

                Discovery discovery = new Discovery(clientNc, 500, 5);

                DiscoveryVerifier pingValidator = (descriptor, o) -> {
                    assertTrue(o instanceof PingResponse);
                    PingResponse pr = (PingResponse)o;
                    if (descriptor != null) {
                        assertEquals(descriptor.name, pr.getName());
                    }
                    return pr.getServiceId();
                };
                verifyDiscovery(null, discovery.ping(), pingValidator, echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
                verifyDiscovery(sdEcho, discovery.ping(ECHO_SERVICE), pingValidator, echoServiceId1, echoServiceId2);
                verifyDiscovery(sdSort, discovery.ping(SORT_SERVICE), pingValidator, sortServiceId1, sortServiceId2);
                verifyDiscovery(sdEcho, discovery.ping(ECHO_SERVICE, echoServiceId1), pingValidator, echoServiceId1);
                verifyDiscovery(sdSort, discovery.ping(SORT_SERVICE, sortServiceId1), pingValidator, sortServiceId1);
                verifyDiscovery(sdEcho, discovery.ping(ECHO_SERVICE, echoServiceId2), pingValidator, echoServiceId2);
                verifyDiscovery(sdSort, discovery.ping(SORT_SERVICE, sortServiceId2), pingValidator, sortServiceId2);

                DiscoveryVerifier infoValidator = (descriptor, o) -> {
                    assertTrue(o instanceof InfoResponse);
                    InfoResponse ir = (InfoResponse)o;
                    if (descriptor != null) {
                        assertEquals(descriptor.name, ir.getName());
                        assertEquals(descriptor.description, ir.getDescription());
                        assertEquals(descriptor.version, ir.getVersion());
                        assertEquals(descriptor.subject, ir.getSubject());
                    }
                    return ir.getServiceId();
                };
                verifyDiscovery(null, discovery.info(), infoValidator, echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
                verifyDiscovery(sdEcho, discovery.info(ECHO_SERVICE), infoValidator, echoServiceId1, echoServiceId2);
                verifyDiscovery(sdSort, discovery.info(SORT_SERVICE), infoValidator, sortServiceId1, sortServiceId2);
                verifyDiscovery(sdEcho, discovery.info(ECHO_SERVICE, echoServiceId1), infoValidator, echoServiceId1);
                verifyDiscovery(sdSort, discovery.info(SORT_SERVICE, sortServiceId1), infoValidator, sortServiceId1);
                verifyDiscovery(sdEcho, discovery.info(ECHO_SERVICE, echoServiceId2), infoValidator, echoServiceId2);
                verifyDiscovery(sdSort, discovery.info(SORT_SERVICE, sortServiceId2), infoValidator, sortServiceId2);

                DiscoveryVerifier schemaValidator = (descriptor, o) -> {
                    assertTrue(o instanceof SchemaResponse);
                    SchemaResponse sr = (SchemaResponse)o;
                    if (descriptor != null) {
                        assertEquals(descriptor.name, sr.getName());
                        assertEquals(descriptor.version, sr.getVersion());
                        assertEquals(descriptor.schemaRequest, sr.getSchema().getRequest());
                        assertEquals(descriptor.schemaResponse, sr.getSchema().getResponse());
                    }
                    return sr.getServiceId();
                };
                verifyDiscovery(null, discovery.schema(), schemaValidator, echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
                verifyDiscovery(sdEcho, discovery.schema(ECHO_SERVICE), schemaValidator, echoServiceId1, echoServiceId2);
                verifyDiscovery(sdSort, discovery.schema(SORT_SERVICE), schemaValidator, sortServiceId1, sortServiceId2);
                verifyDiscovery(sdEcho, discovery.schema(ECHO_SERVICE, echoServiceId1), schemaValidator, echoServiceId1);
                verifyDiscovery(sdSort, discovery.schema(SORT_SERVICE, sortServiceId1), schemaValidator, sortServiceId1);
                verifyDiscovery(sdEcho, discovery.schema(ECHO_SERVICE, echoServiceId2), schemaValidator, echoServiceId2);
                verifyDiscovery(sdSort, discovery.schema(SORT_SERVICE, sortServiceId2), schemaValidator, sortServiceId2);

                List<StatsResponse> srList = discovery.stats();
                assertEquals(4, srList.size());
                int responseEcho = 0;
                int responseSort = 0;
                int requestsEcho = 0;
                int requestsSort = 0;
                for (StatsResponse sr : srList) {
                    assertEquals(1, sr.getStats().size());
                    EndpointStats es = sr.getStats().get(0);
                    assertEquals(sr.getName(), es.name);
                    if (sr.getName().equals(ECHO_SERVICE)) {
                        responseEcho++;
                        requestsEcho += es.numRequests.get();
                    }
                    else {
                        responseSort++;
                        requestsSort += es.numRequests.get();
                    }
                }
                assertEquals(2, responseEcho);
                assertEquals(2, responseSort);
                assertEquals(requestCount, requestsEcho);
                assertEquals(requestCount, requestsSort);

                srList = discovery.stats(true);
                assertEquals(4, srList.size());
                for (StatsResponse sr : srList) {
                    assertEquals(5, sr.getStats().size());
                    assertEquals(sr.getName(), sr.getStats().get(0).name); // because it's sorted
                    assertEquals(PING, sr.getStats().get(1).name);
                    assertEquals(INFO, sr.getStats().get(2).name);
                    assertEquals(SCHEMA, sr.getStats().get(3).name);
                    assertEquals(STATS, sr.getStats().get(4).name);
                    if (sr.getName().equals(ECHO_SERVICE)) {
                        responseEcho++;
                    }
                    else {
                        responseSort++;
                    }
                }
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
        String id = service.getId();
        assertNotNull(id);
        assertEquals(sd, service.getServiceDescriptor());
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
}