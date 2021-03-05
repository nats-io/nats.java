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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.examples.ExampleUtils.sleep;

/**
 * This example will demonstrate a pull subscription with:
 * - a batch size + fetch i.e. subscription.fetch(10, Duration.ofSeconds(10))
 *
 * Usage: java NatsJsPullReceive [-s server] [-strm stream] [-sub subject] [-dur durable]
 *   Use tls:// or opentls:// to require tls, via the Default SSLContext
 *   Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
 *   Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
 *   Use the URL for user/pass/token authentication.
 */
public class NatsJsPullReceive extends NatsJsPullSubBase {
    static class ReceiveMessageHandler implements MessageHandler {
        boolean ack;
        int expected;
        AtomicInteger received;
        CountDownLatch latch;

        public ReceiveMessageHandler(boolean ack, int expected) {
            this.ack = ack;
            this.expected = expected;
            received = new AtomicInteger();
            latch = new CountDownLatch(1);
            System.out.print("Receive ->");
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            if (msg == null) {
                System.out.println(" <- ");
                System.out.println("We should have received " + expected + " total messages, we received: " + received);
                latch.countDown();
            }
            else {
                received.incrementAndGet();
                System.out.print(" " + new String(msg.getData()));
                if (ack) {
                    msg.ack();
                }
            }
        }
    }

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder()
                .defaultStream("receive-ack-stream")
                .defaultSubject("receive-ack-subject")
                .defaultDurable("receive-ack-durable")
                .build(args);
        
        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {

            createStream(nc, exArgs.stream, exArgs.subject);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // Build our consumer configuration and subscription options.
            // make sure the ack wait is sufficient to handle the reading and processing of the batch.
            // Durable is REQUIRED for pull based subscriptions
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .ackWait(Duration.ofMillis(2500))
                    .build();

            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .durable(exArgs.durable) // required
                    .configuration(cc)
                    .build();

            // 0.1 Initialize. subscription
            // 0.2 Flush outgoing communication with/to the server, useful when app is both publishing and subscribing.
            System.out.println("\n----------\n0. Initialize the subscription and pull.");
            JetStreamSubscription sub = js.subscribe(exArgs.subject, pullOptions);
            nc.flush(Duration.ofSeconds(1));

            // 1. Fetch, but there are no messages yet.
            // -  Read the messages, get them all (0)
            System.out.println("----------\n1. There are no messages yet");

            ReceiveMessageHandler handler = new ReceiveMessageHandler(true, 0);
            sub.receive(10, Duration.ofSeconds(3), handler);
            handler.latch.await();

            // 2. Publish 10 messages
            // -  Fetch messages, get 10
            System.out.println("----------\n2. Publish 10 which satisfies the batch");
            publish(js, exArgs.subject, "A", 10);
            handler = new ReceiveMessageHandler(true, 10);
            sub.receive(10, Duration.ofSeconds(3), handler);
            handler.latch.await();

            // 3. Publish 20 messages
            // -  Fetch messages, only get 10
            System.out.println("----------\n3. Publish 20 which is larger than the batch size.");
            publish(js, exArgs.subject, "B", 20);
            handler = new ReceiveMessageHandler(true, 10);
            sub.receive(10, Duration.ofSeconds(3), handler);
            handler.latch.await();

            // 4. There are still messages left from the last
            // -  Fetch messages, get 10
            System.out.println("----------\n4. Get the rest of the publish.");
            handler = new ReceiveMessageHandler(true, 10);
            sub.receive(10, Duration.ofSeconds(3), handler);
            handler.latch.await();

            // 5. Publish 5 messages
            // -  Fetch messages, get 5
            // -  Since there are less than batch size we only get what the server has.
            System.out.println("----------\n5. Publish 5 which is less than batch size.");
            publish(js, exArgs.subject, "C", 5);
            handler = new ReceiveMessageHandler(true, 5);
            sub.receive(10, Duration.ofSeconds(3), handler);
            handler.latch.await();

            // 6. Publish 15 messages
            // -  Fetch messages, only get 10
            System.out.println("----------\n6. Publish 15 which is more than the batch size.");
            publish(js, exArgs.subject, "D", 15);
            handler = new ReceiveMessageHandler(true, 10);
            sub.receive(10, Duration.ofSeconds(3), handler);
            handler.latch.await();

            // 7. There are 5 messages left
            // -  Fetch messages, only get 5
            System.out.println("----------\n7. There are 5 messages left.");
            handler = new ReceiveMessageHandler(true, 5);
            sub.receive(10, Duration.ofSeconds(3), handler);
            handler.latch.await();

            // 8. Read but don't ack.
            // -  Fetch messages, get 10, but either take too long to ack them or don't ack them
            System.out.println("----------\n8. Fetch but don't ack.");
            publish(js, exArgs.subject, "E", 10);
            handler = new ReceiveMessageHandler(false, 10);
            sub.receive(10, Duration.ofSeconds(3), handler);
            handler.latch.await();
            sleep(3000); // longer than the ackWait

            // 9. Fetch messages,
            // -  get the 10 messages we didn't ack
            System.out.println("----------\n9. Fetch, get the messages we did not ack.");
            handler = new ReceiveMessageHandler(true, 10);
            sub.receive(10, Duration.ofSeconds(3), handler);
            handler.latch.await();

            System.out.println("----------\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
