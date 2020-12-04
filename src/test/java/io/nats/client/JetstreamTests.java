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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.nats.client.ConsumerConfiguration.AckPolicy;
import io.nats.client.StreamConfiguration.StorageType;
import io.nats.client.StreamInfo.StreamState;

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

                // TODO - test with message ID

                // TODO - test with expectations

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

                // check the info
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
    public void testJetstreamAck() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        Connection nc = null;
        try (NatsTestServer ts = new NatsTestServer(false, true)) {
            try {
                Options options = new Options.Builder().server(ts.getURI()).oldRequestStyle().build();

                nc = Nats.connect(options);
                JetStream js = nc.jetStream();
                createMemoryStream(js, "test-stream", "foo");

                ts.createPullConsumer("test-stream", "pull-durable");

                // publish to foo
                PublishOptions popts = PublishOptions.builder().stream("test-stream").build();
                nc.jetStream().publish("foo", "payload".getBytes(), popts);

                Message m = nc.request("$JS.API.CONSUMER.MSG.NEXT.test-stream.pull-durable", null, Duration.ofSeconds(2));
                assertNotNull(m);
                assertEquals("payload", new String(m.getData()));

                m.ack();

                m = nc.request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", null, Duration.ofSeconds(1));
                assertNull(m);

            } catch (Exception ex) {
                Assertions.fail("Exception:  " + ex.getMessage());
            }
            finally {
                if (nc != null) {
                    nc.close();
                }
            }
        }
    } 
    
    @Test
    public void testJetstreamNack() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        Connection nc = null;
        try (NatsTestServer ts = new NatsTestServer(false, true)) {
            try {
                Options options = new Options.Builder().server(ts.getURI()).oldRequestStyle().build();

                nc = Nats.connect(options);
                ts.createMemoryStream("test-stream", "foo");
                ts.createPullConsumer("test-stream", "pull-durable");

                // publish to foo
                PublishOptions popts = PublishOptions.builder().stream("test-stream").build();
                nc.jetStream().publish("foo", "payload".getBytes(), popts);

                Message m = nc.request("$JS.API.CONSUMER.MSG.NEXT.test-stream.pull-durable", null, Duration.ofSeconds(2));
                assertNotNull(m);
                assertEquals("payload", new String(m.getData()));
                
                m.nak();

                /* TODO - should this work?
                m = nc.request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", null, Duration.ofSeconds(2));
                assertNotNull(m);
                assertEquals("payload", new String(m.getData()));
                */

            } catch (Exception ex) {
                Assertions.fail("Exception:  " + ex.getMessage());
            }
            finally {
                if (nc != null) {
                    nc.close();
                }
            }
        }
    }

    @Test
    public void testJetstreamAckTerm() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        Connection nc = null;
        try (NatsTestServer ts = new NatsTestServer(false, true)) {
            try {
                Options options = new Options.Builder().server(ts.getURI()).oldRequestStyle().build();

                nc = Nats.connect(options);
                ts.createMemoryStream("test-stream", "foo");
                ts.createPullConsumer("test-stream", "pull-durable");

                // publish to foo
                PublishOptions popts = PublishOptions.builder().stream("test-stream").build();
                nc.jetStream().publish("foo", "payload".getBytes(), popts);

                Message m = nc.request("$JS.API.CONSUMER.MSG.NEXT.test-stream.pull-durable", null, Duration.ofSeconds(2));
                assertNotNull(m);
                assertEquals("payload", new String(m.getData()));
                
                m.term();

                m = nc.request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", null, Duration.ofSeconds(1));
                assertNull(m);

            } catch (Exception ex) {
                Assertions.fail("Exception:  " + ex.getMessage());
            }
            finally {
                if (nc != null) {
                    nc.close();
                }
            }
        }
    }  
    
    @Test
    public void testJetstreamAckProgress() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        Connection nc = null;
        try (NatsTestServer ts = new NatsTestServer(false, true)) {
            try {
                Options options = new Options.Builder().server(ts.getURI()).oldRequestStyle().build();

                nc = Nats.connect(options);
                ts.createMemoryStream("test-stream", "foo");
                ts.createPullConsumer("test-stream", "pull-durable");

                // publish to foo
                PublishOptions popts = PublishOptions.builder().stream("test-stream").build();
                nc.jetStream().publish("foo", "payload".getBytes(), popts);

                Message m = nc.request("$JS.API.CONSUMER.MSG.NEXT.test-stream.pull-durable", null, Duration.ofSeconds(2));
                assertNotNull(m);
                assertEquals("payload", new String(m.getData()));
                
                m.inProgress();

                m = nc.request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", null, Duration.ofSeconds(1));
                assertNull(m);

            } catch (Exception ex) {
                Assertions.fail("Exception:  " + ex.getMessage());
            }
            finally {
                if (nc != null) {
                    nc.close();
                }
            }
        }
    }
    
    @Test
    public void testJetstreamAckAndFetch() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        Connection nc = null;
        try (NatsTestServer ts = new NatsTestServer(false, true)) {
            try {
                Options options = new Options.Builder().server(ts.getURI()).oldRequestStyle().build();

                nc = Nats.connect(options);
                ts.createMemoryStream("test-stream", "foo");
                ts.createPullConsumer("test-stream", "pull-durable");

                JetStream js = nc.jetStream();
                // publish to foo
                PublishOptions popts = PublishOptions.builder().stream("test-stream").build();
                js.publish("foo", "payload1".getBytes(), popts);
                js.publish("foo", "payload2".getBytes(), popts);

                Message m = nc.request("$JS.API.CONSUMER.MSG.NEXT.test-stream.pull-durable", null, Duration.ofSeconds(2));
                assertNotNull(m);
                assertEquals("payload1", new String(m.getData()));
                
                /*Message next = m.ackAndFetch(Duration.ofSeconds(2));
                assertNotNull(next);
                assertEquals("payload2", new String(next.getData()));*/

                m = nc.request("$JS.API.CONSUMER.MSG.NEXT.TEST.PULL", null, Duration.ofSeconds(1));
                assertNull(m);

            } catch (Exception ex) {
                Assertions.fail("Exception:  " + ex.getMessage());
            }
            finally {
                if (nc != null) {
                    nc.close();
                }
            }
        }
    }  
    
    @Test
    public void testJetstreamAckNextRequest() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        Connection nc = null;
        try (NatsTestServer ts = new NatsTestServer(false, true)) {
            try {
                Options options = new Options.Builder().server(ts.getURI()).oldRequestStyle().build();

                nc = Nats.connect(options);
                ts.createMemoryStream("test-stream", "foo");
                ts.createPullConsumer("test-stream", "pull-durable");

                // publish to foo
                PublishOptions popts = PublishOptions.builder().stream("test-stream").build();
                JetStream js = nc.jetStream();
                js.publish("foo", "payload1".getBytes(), popts);
                js.publish("foo", "payload2".getBytes(), popts);
                js.publish("foo", "payload3".getBytes(), popts);

                Message m = nc.request("$JS.API.CONSUMER.MSG.NEXT.test-stream.pull-durable", null, Duration.ofSeconds(2));
                assertNotNull(m);
                assertEquals("payload1", new String(m.getData()));
                /*
                m.ackNextRequest(null, 2, false);

                ZonedDateTime zdt = ZonedDateTime.now().plusSeconds(10);
                m.ackNextRequest(zdt, 15, false);
                */
            } catch (Exception ex) {
                Assertions.fail("Exception:  " + ex.getMessage());
            }
            finally {
                if (nc != null) {
                    nc.close();
                }
            }
        }
    }       
}