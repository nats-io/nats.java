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

package io.nats.examples;

import io.nats.client.*;
import io.nats.client.impl.JetStreamApiException;
import io.nats.client.impl.NatsMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class NatsJsPollSubDemo {

    public static void main(String[] args) {

        String unique = "A"; // change this if you run the demo multiple times on the same server
        String server = "nats://localhost:4222";
        String stream = "PollDemoStream" + unique;
        String subject = "PollDemoSubject" + unique;
        String durable = "PollDemoDurable" + unique;

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(server, true))) {

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            ExampleUtils.createTestStream(nc, stream, subject);

            // Build our subscription options. Durable is REQUIRED for pull based subscriptions
            PullSubscribeOptions options = PullSubscribeOptions.builder().batchSize(10).durable(durable).build();

            // Subscribe synchronously.
            JetStreamSubscription sub = js.subscribe(subject, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // publish some amount of messages, but not entire pull size
            int demoMessageNumber = publish(js, subject, 0, 4);

            // start the pull
            sub.pull();

            // read what is available, expect 4
            int red = readMessages(sub);
            int total = red;
            report(4, red, 4, total);

            // publish some more covering our initial pull and more
            demoMessageNumber = publish(js, subject, demoMessageNumber, 20);

            // read what is available, expect 6 more
            red = readMessages(sub);
            total += red;
            report(6, red, 10, total);

            // read what is available, but we have not polled, we should not get any more messages
            red = readMessages(sub);
            total += red;
            report(0, red, 10, total);

            // pull more
            sub.pull();

            // read what is available, expect 10
            red = readMessages(sub);
            total += red;
            report(10, red, 20, total);

            // read what is available, but we have not polled, we should not get any more messages
            red = readMessages(sub);
            total += red;
            report(0, red, 20, total);

            // pull more
            sub.pull();

            // read what is available, expect 4
            red = readMessages(sub);
            total += red;
            report(4, red, 24, total);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void report(int expectedRed, int actualRed, int expectedTotal, int actualTotal) {
        System.out.printf("Read should be %d got %d ? %s. Total should be %d is %d ? %s\n",
                expectedRed, actualRed, (expectedRed == actualRed ? "Ok" : "Fail"),
                expectedTotal, actualTotal, (expectedTotal == actualTotal ? "Ok" : "Fail"));
    }

    private static int readMessages(JetStreamSubscription sub) throws InterruptedException {
        int red = 0;
        Message msg = sub.nextMessage(Duration.ofSeconds(1));
        while (msg != null) {
            if (red++ == 0) {
                System.out.print("Reading... ");
            }
            System.out.print(new String(msg.getData(), StandardCharsets.UTF_8) + " ");
            msg.ack();
            msg = sub.nextMessage(Duration.ofSeconds(1));
        }
        System.out.println("Read " + red);
        return red;
    }

    private static int publish(JetStream js, String subject, int startId, int count) throws IOException, JetStreamApiException {
        System.out.println("Publishing " + count + ", #" + (startId + 1) + " to #" + (startId + count));
        for (int x = 0; x < count; x++) {
            Message msg = NatsMessage.builder()
                    .subject(subject)
                    .data("#" + (++startId), StandardCharsets.UTF_8)
                    .build();
            js.publish(msg);
        }
        return startId;
    }
}