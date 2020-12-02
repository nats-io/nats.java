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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.nats.client.StreamConfiguration.StorageType;
import io.nats.client.StreamInfo.StreamState;

public class JetstreamTests {

    private static StreamInfo createMemoryStream(JetStream js, String streamName, String subject)
            throws TimeoutException, InterruptedException {
        String[] subjects = new String[1];
        subjects[0] = subject;

        StreamConfiguration sc = StreamConfiguration.builder().
            name(streamName).
            storageType(StorageType.Memory).
            subjects(subjects).
            build();

        return js.addStream(sc);
    }

    @Test
    public void testStreamAndConsumerCreate()throws IOException, InterruptedException,ExecutionException {
        try (NatsTestServer ts = new NatsTestServer(false, true);
             Connection nc = Nats.connect(ts.getURI())) {

            String[] subjects = { "foo" };
            try {
                JetStream js = nc.jetStream();
                StreamConfiguration sc = StreamConfiguration.builder().
                    name("foo-stream").
                    storageType(StorageType.Memory).
                    subjects(subjects).
                    build();

                StreamInfo si = js.addStream(sc);

                sc = si.getConfiguration();
                assertNotNull(sc);
                assertEquals("foo-stream", sc.getName());
                
                // spot check state
                StreamState state = si.getStreamState();
                assertEquals(0, state.getMsgCount());

                ConsumerConfiguration cc = ConsumerConfiguration.builder().deliverSubject("bar").durable("mydurable").build();
                ConsumerInfo ci = js.addConsumer("foo-stream", cc);

                cc = ci.getConsumerConfiguration();
                assertNotNull(cc);
                assertEquals("bar", cc.getDeliverSubject());
            } catch (Exception ex) {
                Assertions.fail("Exception:  " + ex.getMessage());
            }
            finally {
                nc.close();
            }
        }       
    }

    @Test
    public void testJetstreamPublishDefaultOptions() throws IOException, InterruptedException,ExecutionException {
        try (NatsTestServer ts = new NatsTestServer(false, true);
             Connection nc = Nats.connect(ts.getURI())) {

            try {
                JetStream js = nc.jetStream();
                createMemoryStream(js, "foo-stream", "foo");
                
                PublishAck ack = js.publish("foo", null);
                assertEquals(1, ack.getSeqno());
            } catch (Exception ex) {
                Assertions.fail("Exception:  " + ex.getMessage());
            }
            finally {
                nc.close();
            }
        }
    }

    @Test
    public void testJetstreamNotAvailable() throws IOException, InterruptedException,ExecutionException {
        try (NatsTestServer ts = new NatsTestServer(false, false);
            Connection nc = Nats.connect(ts.getURI())) {
            assertThrows(TimeoutException.class, ()-> { nc.jetStream(); });
        }
    }

    @Test
    public void testJetstreamPublish() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false, true);
             Connection nc = Nats.connect(ts.getURI())) {

            try {
                JetStream js = nc.jetStream();
                createMemoryStream(js, "foo-stream", "foo");

                PublishOptions popts = PublishOptions.builder().stream("foo-stream").build();
                js.publish("foo", null, popts);
            } catch (Exception ex) {
                Assertions.fail("Exception:  " + ex.getMessage());
            }
            finally {
                nc.close();
            }
        }
    }

    @Test
    public void testJetstreamPublishAndSubscribe() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false, true);
             Connection nc = Nats.connect(ts.getURI())) {

            try {
                JetStream js = nc.jetStream();
                createMemoryStream(js, "test-stream", "foo");

                // publish to foo
                PublishOptions popts = PublishOptions.builder().stream("test-stream").build();
                js.publish("foo", "payload".getBytes(), popts);

                // Using subscribe options, let a subscription to "bar" be from our stream.
                ConsumerConfiguration c = ConsumerConfiguration.builder().build();
                SubscribeOptions so = SubscribeOptions.builder().configuration("test-stream", c).build();
                Subscription s = js.subscribe("foo", so);
                Message m = s.nextMessage(Duration.ofSeconds(5));
                assertEquals(new String("payload"), new String(m.getData()));
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

                m.ack(Duration.ofSeconds(2));

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
                
                m.nak(Duration.ofSeconds(2));

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
                
                m.ackTerm(Duration.ofSeconds(2));

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
                
                m.ackProgress(Duration.ofSeconds(2));

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
                
                Message next = m.ackAndFetch(Duration.ofSeconds(2));
                assertNotNull(next);
                assertEquals("payload2", new String(next.getData()));

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
                
                m.ackNextRequest(null, 2, false);

                ZonedDateTime zdt = ZonedDateTime.now().plusSeconds(10);
                m.ackNextRequest(zdt, 15, false);
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