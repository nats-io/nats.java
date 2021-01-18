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

package io.nats.client;

import io.nats.client.ConsumerConfiguration.AckPolicy;
import io.nats.client.StreamConfiguration.StorageType;
import io.nats.client.StreamInfo.StreamState;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

public class JetstreamTests {

    private static StreamInfo createMemoryStream(JetStream js, String streamName, String subject)
            throws TimeoutException, InterruptedException {
        String[] subjects = new String[1];
        subjects[0] = subject;

        return createMemoryStream(js, streamName, subjects);
    }

    private static StreamInfo createMemoryStream(JetStream js, String streamName, String[] subjects)
            throws TimeoutException, InterruptedException {

        StreamConfiguration sc = StreamConfiguration.builder().name(streamName).storageType(StorageType.Memory)
                .subjects(subjects).build();

        return js.addStream(sc);
    }

    @Test
    public void testStreamAndConsumerCreate() throws IOException, InterruptedException, ExecutionException {
        try (NatsTestServer ts = new NatsTestServer(false, true); Connection nc = Nats.connect(ts.getURI())) {

            String[] subjects = { "foo" };
            try {
                JetStream js = nc.jetStream();
                StreamConfiguration sc = StreamConfiguration.builder().name("foo-stream")
                        .storageType(StorageType.Memory).subjects(subjects).build();

                StreamInfo si = js.addStream(sc);

                sc = si.getConfiguration();
                assertNotNull(sc);
                assertEquals("foo-stream", sc.getName());

                // spot check state
                StreamState state = si.getStreamState();
                assertEquals(0, state.getMsgCount());

                ConsumerConfiguration cc = ConsumerConfiguration.builder().deliverSubject("bar").durable("mydurable")
                        .build();
                ConsumerInfo ci = js.addConsumer("foo-stream", cc);

                cc = ci.getConsumerConfiguration();
                assertNotNull(cc);
                assertEquals("bar", cc.getDeliverSubject());
            } catch (Exception ex) {
                Assertions.fail(ex);
            } finally {
                nc.close();
            }
        }
    }

    @Test
    public void testJetstreamPublishDefaultOptions() throws IOException, InterruptedException, ExecutionException {
        try (NatsTestServer ts = new NatsTestServer(false, true); Connection nc = Nats.connect(ts.getURI())) {

            try {
                JetStream js = nc.jetStream();
                createMemoryStream(js, "foo-stream", "foo");

                PublishAck ack = js.publish("foo", null);
                assertEquals(1, ack.getSeqno());
            } catch (Exception ex) {
                Assertions.fail("Exception:  " + ex.getMessage());
            } finally {
                nc.close();
            }
        }
    }

    @Test
    public void testJetstreamNotAvailable() throws IOException, InterruptedException, ExecutionException {
        try (NatsTestServer ts = new NatsTestServer(false, false); Connection nc = Nats.connect(ts.getURI())) {
            assertThrows(TimeoutException.class, () -> {
                nc.jetStream();
            });
        }
    }

    @Test
    public void testJetstreamPublish() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false, true); Connection nc = Nats.connect(ts.getURI())) {

            try {
                JetStream js = nc.jetStream();

                // check for failure w/ no stream.
                assertThrows(Exception.class, () -> js.publish("foo", "hello".getBytes()));

                createMemoryStream(js, "foo-stream", "foo");

                // this should succeed
                js.publish("foo", "hello".getBytes());

                // Set the stream and publish.
                PublishOptions popts = PublishOptions.builder().stream("foo-stream").build();
                js.publish("foo", null, popts);

                popts.setStream("bar-stream");
                assertThrows(Exception.class, () -> js.publish("foo", "hello".getBytes(), popts));

                PublishAck pa = js.publish("foo", null);
                assertEquals(4, pa.getSeqno());
                assertEquals("foo-stream", pa.getStream());
                assertFalse(pa.isDuplicate());

            } catch (Exception ex) {
                Assertions.fail(ex);
            } finally {
                nc.close();
            }
        }
    }

    @Test
    public void testJetstreamPublishOptions() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false, true);
             Connection nc = Nats.connect(ts.getURI())) {
                            
            try {
                JetStream js = nc.jetStream();

                createMemoryStream(js, "foo-stream", "foo");

                PublishOptions opts = PublishOptions.builder().build();
                
                // check with no previous message id
                opts.setExpectedLastMsgId("invalid");
                assertThrows(IllegalStateException.class, ()-> { js.publish("foo", null, opts); });

                // this should succeed.  Reset our last expected Msg ID, and set this one.
                opts.setExpectedLastMsgId(null);
                opts.setMessageId("mid1");
                js.publish("foo", null, opts);

                // this should succeed.
                opts.setExpectedLastMsgId("mid1");
                opts.setMessageId("mid2");
                js.publish("foo", null, opts);

                // test invalid last ID.
                opts.setMessageId(null);
                opts.setExpectedLastMsgId("invalid");
                assertThrows(IllegalStateException.class, ()-> { js.publish("foo", null, opts); });

                // We're expecting two messages.  Reset the last expeccted ID.
                opts.setExpectedLastMsgId(null);
                opts.setExpectedLastSeqence(2);
                js.publish("foo", null, opts);

                // invalid last sequence.
                opts.setExpectedLastSeqence(42);
                assertThrows(IllegalStateException.class, ()-> { js.publish("foo", null, opts); });

                // check success - TODO - debug...
                // opts.setExpectedStream("foo-stream");
                // opts.setExpectedLastSeqence(PublishOptions.unsetLastSequence);
                // js.publish("foo", null, opts);

                // check failure
                opts.setExpectedStream("oof");
                assertThrows(IllegalStateException.class, ()-> { js.publish("foo", null, opts); });

            } catch (Exception ex) {
                Assertions.fail(ex);
            }
            finally {
                nc.close();
            }
        }
    }                

    @Test
    public void testJetstreamSubscribe() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false, true);
             Connection nc = Nats.connect(ts.getURI())) {
                
            String[] subjects = { "foo", "bar", "baz", "foo.*" };
            
            try {
                JetStream js = nc.jetStream();
                createMemoryStream(js, "test-stream", subjects);

                // publish to foo
                PublishOptions popts = PublishOptions.builder().stream("test-stream").build();
                js.publish("foo", "payload".getBytes(), popts);

                // default ephememeral subscription.
                Subscription s = js.subscribe("foo");
                Message m = s.nextMessage(Duration.ofSeconds(5));
                assertNotNull(m);
                assertEquals(new String("payload"), new String(m.getData()));

                // set the stream
                ConsumerConfiguration c = ConsumerConfiguration.builder().build();
                SubscribeOptions so = SubscribeOptions.builder().configuration("test-stream", c).build();
                s = js.subscribe("foo", so);
                m = s.nextMessage(Duration.ofSeconds(5));
                assertNotNull(m);
                assertEquals(new String("payload"), new String(m.getData()));
                s.unsubscribe();

                // FIXME test an invalid stream - is this a bug???
                c = ConsumerConfiguration.builder().build();
                so = SubscribeOptions.builder().configuration("garbage", c).build();
                s = js.subscribe("foo", so);
                s.unsubscribe();

                // try using a dispatcher and an ephemeral consumer.
                CountDownLatch latch = new CountDownLatch(1);
                Dispatcher d = nc.createDispatcher(null);
                JetStreamSubscription jsub = js.subscribe("foo", d, (msg) -> {
                    latch.countDown();
                });

                assertTrue(latch.await(5, TimeUnit.SECONDS));

                ConsumerInfo ci = jsub.getConsumerInfo();
                assertEquals(AckPolicy.Explicit, ci.getConsumerConfiguration().getAckPolicy());
                assertEquals(1, ci.getDelivered().getConsumerSequence());
                assertEquals(1, ci.getDelivered().getStreamSequence());
                assertEquals(1, ci.getAckFloor().getConsumerSequence());
                assertEquals(1, ci.getAckFloor().getStreamSequence());

                // create a sync subscriber that is durable
                so = SubscribeOptions.builder().durable("colin").build();
                jsub = js.subscribe("foo", so);

                // check we registered as a durable.
                assertEquals("colin", jsub.getConsumerInfo().getConsumerConfiguration().getDurable());

                String deliver = jsub.getSubject();
                jsub.unsubscribe();

                // recreate and see if we get the same durable and different delivery
                jsub = js.subscribe("foo", so);
                assertEquals("colin", jsub.getConsumerInfo().getConsumerConfiguration().getDurable());
                assertNotEquals(deliver, jsub.getSubject());
                deliver = jsub.getSubject();

                // now attach to the durable.
                so = SubscribeOptions.builder().attach("test-stream", "colin").build();
                JetStreamSubscription jsub2 = js.subscribe("foo", so);
                assertEquals("colin", jsub2.getConsumerInfo().getConsumerConfiguration().getDurable());
                assertEquals(deliver, jsub2.getSubject());
                jsub.unsubscribe();
                jsub2.unsubscribe();


            } catch (Exception ex) {
                Assertions.fail(ex);
            }
            finally {
                nc.close();
            }
        }
    }

    private static long waitForPending(Subscription sub, int batch, Duration d) {
        long start = System.currentTimeMillis();
        long count;
        while (true) {
            count = sub.getPendingMessageCount();
            // FIXME - should be ==, not sure why receiving all messages in
            // pull based.
            if (count == batch) {
                break;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // noop
            }
            if (System.currentTimeMillis() - start > d.toMillis()) {
                break;
            }
        }
        return count;
    }

    @Test
    public void testJetstreamPullBasedSubscribe() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false, true);
             Connection nc = Nats.connect(ts.getURI())) {
                
            String[] subjects = { "foo", "bar", "baz", "foo.*" };

            try {
                JetStream js = nc.jetStream();
                createMemoryStream(js, "test-stream", subjects);

                // check invalid subscription
                Dispatcher d = nc.createDispatcher(null);
                final SubscribeOptions sop = SubscribeOptions.builder().pull(10).build();
                assertThrows(IllegalStateException.class, () -> {
                    js.subscribe("bar", d, (msg) -> {}, sop);
                });


                int batch = 5;
                int toSend = 10;

                for (int i = 0; i < toSend; i++) {
                    js.publish("bar", null);
                }

                SubscribeOptions so = SubscribeOptions.builder().durable("dur").pull(batch).build();
                JetStreamSubscription jsub = js.subscribe("bar", so);
                assertEquals(batch, waitForPending(jsub, batch, Duration.ofSeconds(2)));

                ConsumerInfo ci = jsub.getConsumerInfo();
                assertNotNull(ci);
                assertEquals(batch, ci.getNumPending());
                assertEquals(batch, ci.getNumAckPending());

                // consume these and ack (not ackNext)
                for (int i = 0; i < batch; i++) {
                    Message m = jsub.nextMessage(Duration.ofSeconds(1));
                    nc.publish(m.getReplyTo(), null, "+ACK".getBytes());
                }

                ci = jsub.getConsumerInfo();
                assertNotNull(ci);
                assertEquals(batch, ci.getAckFloor().getConsumerSequence());

                assertEquals(0, waitForPending(jsub, 0, Duration.ofSeconds(2)));

                // check for a timeout.
                Message m = jsub.nextMessage(Duration.ofSeconds(1));
                assertNull(m);

                // now poll again.
                jsub.poll();
                assertEquals(batch, waitForPending(jsub, batch, Duration.ofSeconds(2)));
                jsub.unsubscribe();

                
                assertThrows(IllegalStateException.class, () -> {
                    js.subscribe("baz", SubscribeOptions.builder().
                        attach("test-stream", "rip").pull(batch).build());
                });

                // send 10 more messages.
                for (int i = 0; i < toSend; i++) {
                    js.publish("bar", null);
                }

                jsub = js.subscribe("bar", SubscribeOptions.builder().
                        attach("test-stream", "dur").
                        pull(batch).
                        build());

                assertEquals(batch, waitForPending(jsub, batch, Duration.ofSeconds(2)));

                ci = jsub.getConsumerInfo();
                assertEquals(batch * 2, ci.getNumAckPending());
                assertEquals(toSend-batch, ci.getNumPending());

                for (int i = 0; i < toSend; i++) {
                    m = jsub.nextMessage(Duration.ofSeconds(1));
                    assertNotNull(m);
                    m.ack();
                }
                
            } catch (Exception ex) {
                Assertions.fail(ex);
            }
            finally {
                nc.close();
            }
        }
    }

    @Test
    public void negativePathwaysCoverage() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false, true);
             Connection nc = Nats.connect(ts.getURI())) {

            JetStream js = nc.jetStream();
            Dispatcher dispatcher = nc.createDispatcher(null);
            Dispatcher dispatcherNull = null;
            MessageHandler handler = msg -> {};
            MessageHandler handlerNull = null;
            String invalid = "in val id";
            String strNull = null;
            String strEmpty = "";
            SubscribeOptions optNull = null;
            String subject = "sub";
            String queue = "queue";
            createMemoryStream(js, "sub-stream", subject);

            assertThrows(IllegalArgumentException.class, () -> js.subscribe(invalid)); // subject  invalid

            assertThrows(IllegalArgumentException.class, () -> js.subscribe(invalid, optNull)); // subject invalid
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, optNull)); // option null

            assertThrows(IllegalArgumentException.class, () -> js.subscribe(invalid, strNull, optNull));  // subject invalid
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, strNull, optNull));  // queue null
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, strEmpty, optNull)); // queue empty
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, invalid, optNull));  // queue invalid
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, queue, optNull));    // options null

            assertThrows(IllegalArgumentException.class, () -> js.subscribe(invalid, dispatcherNull, handlerNull, optNull)); // subject invalid
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, dispatcherNull, handlerNull, optNull)); // dispatcher null
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, dispatcher, handlerNull, optNull));     // handler null
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, dispatcher, handler, optNull));         // options null

            assertThrows(IllegalArgumentException.class, () -> js.subscribe(invalid, strNull, dispatcherNull, handlerNull));  // subject invalid
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, strNull, dispatcherNull, handlerNull));  // queue null
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, strEmpty, dispatcherNull, handlerNull)); // queue empty
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, invalid, dispatcherNull, handlerNull));  // queue invalid
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, queue, dispatcherNull, handlerNull));    // dispatcher null
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, queue, dispatcher, handlerNull));        // handler null

            assertThrows(IllegalArgumentException.class, () -> js.subscribe(invalid, strNull, dispatcherNull, handlerNull, optNull));  // subject invalid
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, strNull, dispatcherNull, handlerNull, optNull));  // queue null
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, strEmpty, dispatcherNull, handlerNull, optNull)); // queue empty
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, invalid, dispatcherNull, handlerNull, optNull));  // queue invalid
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, queue, dispatcherNull, handlerNull, optNull));    // dispatcher null
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, queue, dispatcher, handlerNull, optNull));        // handler null
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subject, queue, dispatcher, handler, optNull));            // options null
        }
    }

    @Test
    public void testJetstreamAttachDirectNoConsumer() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false, true);
             Connection nc = Nats.connect(ts.getURI())) {
                
            try {
                JetStreamOptions jso = JetStreamOptions.builder().
                    direct(true).
                    build();

                JetStream js = nc.jetStream(jso);

                // Create a test-stream, but do not create the consumer.
                createMemoryStream(js, "test-stream", "not.important");

                SubscribeOptions so = SubscribeOptions.builder().
                    attach("test-stream", "test-consumer").
                    durable("test-consumer").
                    pushDirect("push.subject").
                    build();

                JetStreamSubscription jsub = js.subscribe("push.subject", so);
                Message m = jsub.nextMessage(Duration.ofMillis(100));
                assertNull(m);

            } catch (Exception ex) {
                Assertions.fail(ex);
            }
            finally {
                nc.close();
            }
        }
    }
}
