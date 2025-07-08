// Copyright 2025 The NATS Authors
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
import io.nats.client.impl.ErrorListenerConsoleImpl;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.examples.jetstream.NatsJsUtils.createOrReplaceStream;

/**
 * This example will demonstrate simplified ordered consumer with a handler
 * To run, start a server and then start the example.
 * To test resiliency, kill the server, wait a couple seconds, then restart it.
 */
public class NatsJsPushSubOrdered {
    private static final String STREAM = "ordered-stream";
    private static final String SUBJECT = "ordered-subject";
    private static final String CONSUMER_PREFIX = "prefix";
    private static final String MESSAGE_PREFIX = "ordered";
    private static final int STOP_COUNT = 1_000_000;
    private static final int REPORT_EVERY = 500;

    private static final String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder()
            .server(SERVER)
            .errorListener(new ErrorListenerConsoleImpl())
            .build();
        try (Connection nc = Nats.connect(options)) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createOrReplaceStream(jsm, STREAM, StorageType.File, SUBJECT); // file is important, memory won't survive a restart

            System.out.println("Starting publish...");
            ResilientPublisher publisher = new ResilientPublisher(nc, jsm, STREAM, SUBJECT).basicDataPrefix(MESSAGE_PREFIX).jitter(10);
            Thread pubThread = new Thread(publisher);
            pubThread.start();

            CountDownLatch latch = new CountDownLatch(1);
            AtomicInteger atomicCount = new AtomicInteger();
            AtomicLong nextExpectedSequence = new AtomicLong(0);
            long start = System.nanoTime();
            MessageHandler handler = msg -> {
                if (msg.metaData().streamSequence() != nextExpectedSequence.incrementAndGet()) {
                    System.out.println("MESSAGE RECEIVED OUT OF ORDER!");
                    System.exit(-1);
                }
                msg.ack();
                int count = atomicCount.incrementAndGet();
                if (count % REPORT_EVERY == 0) {
                    report("Handler", start, count);
                }
                if (count == STOP_COUNT) {
                    latch.countDown();
                }
            };

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .name(CONSUMER_PREFIX)
                .filterSubject(SUBJECT)
                .idleHeartbeat(Duration.ofSeconds(1))
                .build();

            PushSubscribeOptions pso = PushSubscribeOptions.builder()
                .stream(STREAM)
                .configuration(cc)
                .ordered(true)
                .build();

            JetStream js = nc.jetStream();
            Dispatcher d = nc.createDispatcher();
            js.subscribe(null, d, handler, false, pso);

            latch.await();

            report("Final", start, atomicCount.get());

            publisher.stop(); // otherwise it will complain when the connection goes away
            pubThread.join();
        }
        catch (IOException | InterruptedException | JetStreamApiException e) {
            // IOException:
            //      problem making the connection
            // InterruptedException:
            //      thread interruption in the body of the example
            // JetStreamApiException:
            //      1. the stream or consumer did not exist
            //      2. api calls under the covers theoretically this could fail, but practically it won't.
        }
    }

    private static void report(String label, long start, int count) {
        long ms = (System.nanoTime() - start) / 1_000_000;
        System.out.println(label + ": Received " + count + " messages in " + ms + "ms.");
    }
}
