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

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JetstreamTests {

    @Test
    public void testJetstreamPublishEmptyOptions() throws IOException, InterruptedException,ExecutionException {
        try (NatsTestServer ts = new NatsTestServer(false, true);
             Connection nc = Nats.connect(ts.getURI())) {

            try {
                PublishOptions popts = PublishOptions.builder().build();
                nc.publish("subject", null, popts);
            } catch (Exception ex) {
                assertFalse(true, "Unexpected Exception: " + ex.getMessage());
            }
            finally {
                nc.close();
            }
        }
    }

    @Test
    public void testJetstreamPublish() throws IOException, InterruptedException,ExecutionException, TimeoutException {
        try (NatsTestServer ts = new NatsTestServer(false, true);
             Connection nc = Nats.connect(ts.getURI())) {

            try {
                ts.createMemoryStream("foo-stream", "foo");

                PublishOptions popts = PublishOptions.builder().stream("foo-stream").build();
                nc.publish("foo", null, popts);
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
        try (NatsTestServer ts = new NatsTestServer(true, true);
             Connection nc = Nats.connect(ts.getURI())) {

            try {
                ts.createMemoryStream("test-stream", "foo");

                // publish to foo
                PublishOptions popts = PublishOptions.builder().stream("test-stream").build();
                nc.publish("foo", "payload".getBytes(), popts);

                // Using subscribe options, let a subscription to "bar" be from our stream.
                ConsumerConfiguration c = ConsumerConfiguration.builder().build();
                SubscribeOptions so = SubscribeOptions.builder().consumer(c).stream("test-stream").build();
                Subscription s = nc.subscribe("bar", so);
                Message m = s.nextMessage(Duration.ofSeconds(5));
                assertEquals(new String("payload"), new String(m.getData()));
            } catch (Exception ex) {
                Assertions.fail("Exception:  " + ex.getMessage());
            }
            finally {
                nc.close();
            }
        }
    }       
}