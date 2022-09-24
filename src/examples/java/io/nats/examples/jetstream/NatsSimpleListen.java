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

package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.examples.jetstream.NatsJsUtils.createOrReplaceStream;

/**
 * This example will demonstrate JetStream2 and simplified listening
 */
public class NatsSimpleListen {
    static String stream = "simple-stream";
    static String subject = "simple-subject";
    static String durable = "simple-durable";
    static int count = 20;

    public static void main(String[] args) {

        try (Connection nc = Nats.connect("nats://localhost")) {

            JetStreamManagement jsm = nc.jetStreamManagement();
            createOrReplaceStream(jsm, stream, StorageType.Memory, subject);

            JetStream2 js = nc.jetStream2();
            for (int x = 1; x <= count; x++) {
                js.publish(subject, ("m-" + x).getBytes());
            }

            // Create durable consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(durable)
                .build();
            jsm.addOrUpdateConsumer(stream, cc);

            CountDownLatch latch = new CountDownLatch(count);
            AtomicInteger red = new AtomicInteger();

            MessageHandler handler = msg -> {
                System.out.println("\nMessage Received:");
                System.out.printf("  Subject: %s\n  Data: %s\n",
                    msg.getSubject(), new String(msg.getData(), StandardCharsets.UTF_8));
                System.out.println("  " + msg.metaData());
                msg.ack();
                red.incrementAndGet();
                latch.countDown();
            };

            // Set up Listener
            SimpleConsumer simpleConsumer = js.listen(stream, durable, handler,
                SimpleConsumerOptions.builder().batchSize(10).build());

            latch.await(10, TimeUnit.SECONDS);

            System.out.println("\n" + red.get() + " message(s) were received.\n");

            simpleConsumer.unsubscribe();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
