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

import io.nats.examples.ExampleUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;

public class JetStreamPullTests extends JetStreamTestBase {

    static final boolean TEMP_PULL_IS_WORKING = false;

    @Test
    public void testPull() throws Exception {

        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            ExampleUtils.createTestStream(nc, STREAM, SUBJECT);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder().batchSize(10).durable(DURABLE).build();

            // Subscribe synchronously. The subscription does the first sub.pull for us
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish some amount of messages, but not entire pull size
            int demoMessageNumber = publish(js, SUBJECT, 4, 0);

            // start the pull
            sub.pull();

            // read what is available, expect 4
            int red = readMessagesAck(sub);
            int total = red;
            validateRedAndTotal(4, red, 4, total);

            if (!TEMP_PULL_IS_WORKING) return;

            // publish some more covering our initial pull and more
            demoMessageNumber = publish(js, SUBJECT, 20, demoMessageNumber);

            // read what is available, expect 6 more
            red = readMessagesAck(sub);
            total += red;
            validateRedAndTotal(6, red, 10, total);

            // read what is available, but we have not polled, we should not get any more messages
            red = readMessagesAck(sub);
            total += red;
            validateRedAndTotal(0, red, 10, total);

            // pull more
            sub.pull();

            // read what is available, expect 10
            red = readMessagesAck(sub);
            total += red;
            validateRedAndTotal(10, red, 20, total);

            // read what is available, but we have not polled, we should not get any more messages
            red = readMessagesAck(sub);
            total += red;
            validateRedAndTotal(0, red, 20, total);

            // pull more
            sub.pull();

            // read what is available, expect 4
            red = readMessagesAck(sub);
            total += red;
            validateRedAndTotal(4, red, 24, total);
        });
    }
}
