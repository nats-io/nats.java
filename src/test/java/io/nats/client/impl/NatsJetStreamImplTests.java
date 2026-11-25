// Copyright 2026 The NATS Authors
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

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerCreateRequest;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class NatsJetStreamImplTests extends JetStreamTestBase {

    @Test
    void testCreateConsumerUnsubscribeOnExceptionWithInvalidatedSubscription() throws Exception {
        // Create a connection and jetstream
        Connection nc = Nats.connect(jsServer.getURI());
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();
        NatsJetStream natsJs = (NatsJetStream) js;

        // Create a stream
        String stream = "TEST-STREAM";
        jsm.addStream(StreamConfiguration.builder()
                .name(stream)
                .subjects("test.>")
                .build());

        // Get the internal NatsJetStreamImpl to access package-private method
        NatsJetStreamImpl jsImpl = (NatsJetStreamImpl) js;

        // Create a mock subscription that we can invalidate
        String sid = mockSid();
        NatsJetStreamSubscription sub = new NatsJetStreamSubscription(
                sid,
                "test.deliver",
                null,
                (NatsConnection) nc,
                null, // no dispatcher
                natsJs,
                stream,
                "test-consumer",
                new PullMessageManager((NatsConnection) nc, null, false));

        // Verify subscription is active initially
        assertTrue(sub.isActive(), "Subscription should be active initially");

        // Invalidate the subscription (simulating connection close race)
        ((NatsConnection) nc).invalidate(sub);

        // Verify subscription is now inactive
        assertFalse(sub.isActive(), "Subscription should be inactive after invalidate");

        // Create a ConsumerConfiguration
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable("test-consumer")
                .build();

        // Create a test implementation that throws IOException
        NatsJetStreamImpl testImpl = new NatsJetStreamImpl((NatsConnection) nc, null) {
            @Override
            ConsumerInfo _createConsumer(String streamName, ConsumerConfiguration config, ConsumerCreateRequest.Action action) throws IOException {
                throw new IOException("Timeout or no response waiting for NATS JetStream server");
            }
        };

        // Try to create consumer - this should throw the original exception
        // Since the subscription is already inactive, no unsubscribe is attempted,
        // so no suppressed exception is added.
        IOException thrown = assertThrows(IOException.class, () -> {
            testImpl._createConsumerUnsubscribeOnException(stream, cc, sub);
        });

        // Verify the original exception is thrown
        assertNotNull(thrown);
        assertEquals("Timeout or no response waiting for NATS JetStream server", thrown.getMessage());

        // Verify no suppressed exception (because isActive() is false, unsubscribe is not attempted)
        Throwable[] suppressed = thrown.getSuppressed();
        assertEquals(0, suppressed.length, "Should have no suppressed exception when subscription is already inactive");

        // Verify the subscription was already cleaned up (removed from connection by invalidate call)
        Map<String, NatsSubscription> subscribers = getSubscribers((NatsConnection) nc);
        assertFalse(subscribers.containsKey(sid),
                "Subscription should be removed from connection subscribers");

        nc.close();
    }

    @Test
    void testCreateConsumerUnsubscribeOnExceptionWithRuntimeException() throws Exception {
        // Create a connection and jetstream
        Connection nc = Nats.connect(jsServer.getURI());
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();
        NatsJetStream natsJs = (NatsJetStream) js;

        // Create a stream
        String stream = "TEST-STREAM-2";
        jsm.addStream(StreamConfiguration.builder()
                .name(stream)
                .subjects("test2.>")
                .build());

        // Get the internal NatsJetStreamImpl
        NatsJetStreamImpl jsImpl = (NatsJetStreamImpl) js;

        // Create a subscription that will be invalidated
        String sid = mockSid();
        NatsJetStreamSubscription sub = new NatsJetStreamSubscription(
                sid,
                "test2.deliver",
                null,
                (NatsConnection) nc,
                null,
                natsJs,
                stream,
                "test-consumer-2",
                new PullMessageManager((NatsConnection) nc, null, false));

        // Invalidate the subscription
        ((NatsConnection) nc).invalidate(sub);

        // Create a ConsumerConfiguration
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable("test-consumer-2")
                .build();

        // Override _createConsumer to throw a RuntimeException
        NatsJetStreamImpl testImpl = new NatsJetStreamImpl((NatsConnection) nc, null) {
            @Override
            ConsumerInfo _createConsumer(String streamName, ConsumerConfiguration config, ConsumerCreateRequest.Action action) {
                throw new IllegalStateException("Connection is Closed");
            }
        };

        // Try to create consumer - this should throw the RuntimeException
        // Since the subscription is already inactive, no unsubscribe is attempted,
        // so no suppressed exception is added.
        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> {
            testImpl._createConsumerUnsubscribeOnException(stream, cc, sub);
        });

        // Verify the original exception is thrown
        assertNotNull(thrown);
        assertEquals("Connection is Closed", thrown.getMessage());

        // Verify no suppressed exception (because isActive() is false, unsubscribe is not attempted)
        Throwable[] suppressed = thrown.getSuppressed();
        assertEquals(0, suppressed.length, "Should have no suppressed exception when subscription is already inactive");

        // Verify the subscription was already cleaned up
        Map<String, NatsSubscription> subscribers = getSubscribers((NatsConnection) nc);
        assertFalse(subscribers.containsKey(sid),
                "Subscription should be removed from connection subscribers");

        nc.close();
    }

    @Test
    void testCreateConsumerUnsubscribeOnExceptionWithDispatcher() throws Exception {
        // Create a connection and jetstream
        Connection nc = Nats.connect(jsServer.getURI());
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();
        NatsJetStream natsJs = (NatsJetStream) js;

        // Create a stream
        String stream = "TEST-STREAM-3";
        jsm.addStream(StreamConfiguration.builder()
                .name(stream)
                .subjects("test3.>")
                .build());

        // Get the internal NatsJetStreamImpl
        NatsJetStreamImpl jsImpl = (NatsJetStreamImpl) js;

        // Create a dispatcher
        Dispatcher dispatcher = nc.createDispatcher();
        NatsDispatcher natsDispatcher = (NatsDispatcher) dispatcher;

        // Create a subscription with dispatcher
        String sid = mockSid();
        NatsJetStreamSubscription sub = new NatsJetStreamSubscription(
                sid,
                "test3.deliver",
                null,
                (NatsConnection) nc,
                natsDispatcher,
                natsJs,
                stream,
                "test-consumer-3",
                new PullMessageManager((NatsConnection) nc, null, false));

        // Verify subscription is active initially
        assertTrue(sub.isActive(), "Subscription should be active initially");
        assertNotNull(sub.getDispatcher(), "Subscription should have dispatcher");

        // Invalidate the subscription (simulating connection close race)
        ((NatsConnection) nc).invalidate(sub);

        // Verify subscription is now inactive
        assertFalse(sub.isActive(), "Subscription should be inactive after invalidate");
        assertNull(sub.getDispatcher(), "Dispatcher should be null after invalidate");

        // Create a ConsumerConfiguration
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable("test-consumer-3")
                .build();

        // Create a test implementation that throws IOException
        NatsJetStreamImpl testImpl = new NatsJetStreamImpl((NatsConnection) nc, null) {
            @Override
            ConsumerInfo _createConsumer(String streamName, ConsumerConfiguration config, ConsumerCreateRequest.Action action) throws IOException {
                throw new IOException("Timeout or no response waiting for NATS JetStream server");
            }
        };

        // Try to create consumer - this should throw the original exception
        // Since the subscription is already inactive, no unsubscribe is attempted,
        // so no suppressed exception is added.
        IOException thrown = assertThrows(IOException.class, () -> {
            testImpl._createConsumerUnsubscribeOnException(stream, cc, sub);
        });

        // Verify the original exception is thrown
        assertNotNull(thrown);
        assertEquals("Timeout or no response waiting for NATS JetStream server", thrown.getMessage());

        // Verify no suppressed exception (because isActive() is false, unsubscribe is not attempted)
        Throwable[] suppressed = thrown.getSuppressed();
        assertEquals(0, suppressed.length, "Should have no suppressed exception when subscription is already inactive");

        // Verify the subscription was already cleaned up
        Map<String, NatsSubscription> subscribers = getSubscribers((NatsConnection) nc);
        assertFalse(subscribers.containsKey(sid),
                "Subscription should be removed from connection subscribers");
        Map<String, NatsSubscription> dispatcherSubs = getDispatcherSubscriptions(natsDispatcher);
        assertFalse(dispatcherSubs.containsKey(sid),
                "Subscription should be removed from dispatcher");

        nc.close();
    }

    @Test
    void testCreateConsumerUnsubscribeOnExceptionWithJetStreamApiException() throws Exception {
        // Create a connection and jetstream
        Connection nc = Nats.connect(jsServer.getURI());
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();
        NatsJetStream natsJs = (NatsJetStream) js;

        // Create a stream
        String stream = "TEST-STREAM-4";
        jsm.addStream(StreamConfiguration.builder()
                .name(stream)
                .subjects("test4.>")
                .build());

        // Get the internal NatsJetStreamImpl to access package-private method
        NatsJetStreamImpl jsImpl = (NatsJetStreamImpl) js;

        // Create a mock subscription that we can invalidate
        String sid = mockSid();
        NatsJetStreamSubscription sub = new NatsJetStreamSubscription(
                sid,
                "test4.deliver",
                null,
                (NatsConnection) nc,
                null, // no dispatcher
                natsJs,
                stream,
                "test-consumer-4",
                new PullMessageManager((NatsConnection) nc, null, false));

        // Verify subscription is active initially
        assertTrue(sub.isActive(), "Subscription should be active initially");

        // Invalidate the subscription (simulating connection close race)
        ((NatsConnection) nc).invalidate(sub);

        // Verify subscription is now inactive
        assertFalse(sub.isActive(), "Subscription should be inactive after invalidate");

        // Create a ConsumerConfiguration
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable("test-consumer-4")
                .build();

        // Create a test implementation that throws JetStreamApiException
        NatsJetStreamImpl testImpl = new NatsJetStreamImpl((NatsConnection) nc, null) {
            @Override
            ConsumerInfo _createConsumer(String streamName, ConsumerConfiguration config, ConsumerCreateRequest.Action action) throws JetStreamApiException {
                JsonValue errorJson = JsonValueUtils.mapBuilder()
                        .put("error", JsonValueUtils.mapBuilder()
                                .put("code", 404)
                                .put("err_code", 1001)
                                .put("description", "Consumer not found")
                                .toJsonValue())
                        .put("type", "io.nats.jetstream.api.v1.consumer_create_response")
                        .toJsonValue();
                ConsumerInfo errorResponse = new ConsumerInfo(errorJson);
                throw new JetStreamApiException(errorResponse);
            }
        };

        // Try to create consumer - this should throw the original exception
        // Since the subscription is already inactive, no unsubscribe is attempted,
        // so no suppressed exception is added.
        JetStreamApiException thrown = assertThrows(JetStreamApiException.class, () -> {
            testImpl._createConsumerUnsubscribeOnException(stream, cc, sub);
        });

        // Verify the original exception is thrown
        assertNotNull(thrown);
        assertTrue(thrown.getMessage().contains("Consumer not found"));

        // Verify no suppressed exception (because isActive() is false, unsubscribe is not attempted)
        Throwable[] suppressed = thrown.getSuppressed();
        assertEquals(0, suppressed.length, "Should have no suppressed exception when subscription is already inactive");

        // Verify the subscription was already cleaned up (removed from connection by invalidate call)
        Map<String, NatsSubscription> subscribers = getSubscribers((NatsConnection) nc);
        assertFalse(subscribers.containsKey(sid),
                "Subscription should be removed from connection subscribers");

        nc.close();
    }

    @Test
    void testCreateConsumerUnsubscribeOnExceptionWithActiveSubscriptionAndUnsubscribeFailure() throws Exception {
        // Create a connection and jetstream
        Connection nc = Nats.connect(jsServer.getURI());
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();
        NatsJetStream natsJs = (NatsJetStream) js;

        // Create a stream
        String stream = "TEST-STREAM-5";
        jsm.addStream(StreamConfiguration.builder()
                .name(stream)
                .subjects("test5.>")
                .build());

        // Get the internal NatsJetStreamImpl to access package-private method
        NatsJetStreamImpl jsImpl = (NatsJetStreamImpl) js;

        // Create a mock subscription that is active but will fail on unsubscribe
        // We do this by creating a subscription with a dispatcher that is closed
        String sid = mockSid();
        Dispatcher dispatcher = nc.createDispatcher();
        NatsDispatcher natsDispatcher = (NatsDispatcher) dispatcher;
        natsDispatcher.stop(true); // Close the dispatcher

        NatsJetStreamSubscription sub = new NatsJetStreamSubscription(
                sid,
                "test5.deliver",
                null,
                (NatsConnection) nc,
                natsDispatcher,
                natsJs,
                stream,
                "test-consumer-5",
                new PullMessageManager((NatsConnection) nc, null, false));

        // Verify subscription is active (dispatcher is set, but dispatcher is closed)
        assertTrue(sub.isActive(), "Subscription should be active initially");
        assertNotNull(sub.getDispatcher(), "Subscription should have dispatcher");

        // Create a ConsumerConfiguration
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable("test-consumer-5")
                .build();

        // Create a test implementation that throws IOException
        NatsJetStreamImpl testImpl = new NatsJetStreamImpl((NatsConnection) nc, null) {
            @Override
            ConsumerInfo _createConsumer(String streamName, ConsumerConfiguration config, ConsumerCreateRequest.Action action) throws IOException {
                throw new IOException("Timeout or no response waiting for NATS JetStream server");
            }
        };

        // Try to create consumer - this should throw the original exception
        // The subscription is active, so unsubscribe is attempted, but the dispatcher is closed,
        // so unsubscribe throws an exception which should be suppressed.
        IOException thrown = assertThrows(IOException.class, () -> {
            testImpl._createConsumerUnsubscribeOnException(stream, cc, sub);
        });

        // Verify the original exception is thrown
        assertNotNull(thrown);
        assertEquals("Timeout or no response waiting for NATS JetStream server", thrown.getMessage());

        // Verify the suppressed exception is the IllegalStateException from unsubscribe
        Throwable[] suppressed = thrown.getSuppressed();
        assertEquals(1, suppressed.length, "Should have one suppressed exception");
        assertTrue(suppressed[0] instanceof IllegalStateException);
        assertEquals("Dispatcher is closed", suppressed[0].getMessage());

        // Verify the subscription was cleaned up by conn.invalidate(sub) fallback
        Map<String, NatsSubscription> subscribers = getSubscribers((NatsConnection) nc);
        assertFalse(subscribers.containsKey(sid),
                "Subscription should be removed from connection subscribers");
        Map<String, NatsSubscription> dispatcherSubs = getDispatcherSubscriptions(natsDispatcher);
        assertFalse(dispatcherSubs.containsKey(sid),
                "Subscription should be removed from dispatcher");

        nc.close();
    }

    // Helper to access package-private subscribers map via reflection
    private Map<String, NatsSubscription> getSubscribers(NatsConnection conn) {
        try {
            Field field = NatsConnection.class.getDeclaredField("subscribers");
            field.setAccessible(true);
            //noinspection unchecked
            return (Map<String, NatsSubscription>) field.get(conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Helper to access package-private dispatcher subscriptions map via reflection
    private Map<String, NatsSubscription> getDispatcherSubscriptions(NatsDispatcher dispatcher) {
        try {
            Field field = NatsDispatcher.class.getDeclaredField("subWithNonDefaultHandlerBySid");
            field.setAccessible(true);
            //noinspection unchecked
            return (Map<String, NatsSubscription>) field.get(dispatcher);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}