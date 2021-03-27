// Copyright 2015-2018 The NATS Authors
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

package io.nats.examples.benchmark;

import io.nats.client.*;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;
import io.nats.examples.ExampleUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.nats.examples.ExampleUtils.uniqueEnough;

public class JsPublishInRoundsBench {
    static String server = Options.DEFAULT_URL;
    static String stream = "pir-strm-" + uniqueEnough();
    static String subject = "pir-sub-" + uniqueEnough();

    static int TOTAL = 1_000_000;
    static int PUB_ROUND_SIZE = 10;
    static int PAYLOAD_SIZE = 64;

    public static void main(String[] args) {

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(server, true))) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(stream)
                    .storageType(StorageType.File)
                    .replicas(2)
                    .subjects(subject)
                    .build();

            try {
                jsm.addStream(sc);
                nc.flush(Duration.ofSeconds(5));
            }
            catch (Exception e) {
                System.out.println(e);
            }

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            final Message m = NatsMessage.builder().subject(subject).data(new byte[PAYLOAD_SIZE]).build();

            int sent = 0;
            int failed = 0;
            long totalElapsed = 0;
            while ((sent+failed) < TOTAL) {
                long start = System.currentTimeMillis();
                List<CompletableFuture<PublishAck>> futures = new ArrayList<>();
                for (int x = 0; x < PUB_ROUND_SIZE; x++) {
                    futures.add(js.publishAsync(m));
                }
                while (futures.size() > 0) {
                    List<CompletableFuture<PublishAck>> notDone = new ArrayList<>();
                    for (CompletableFuture<PublishAck> f : futures) {
                        if (f.isDone()) {
                            if (f.isCompletedExceptionally()) {
                                failed++;
                            } else {
                                sent++;
                            }
                        }
                        else {
                            notDone.add(f);
                        }
                    }
                    futures = notDone;
                }

                long roundElapsed = System.currentTimeMillis() - start;
                totalElapsed += roundElapsed;
                if ((sent+failed) % 10_000 == 0) {
                    long pubPerSec = totalElapsed == 0 ? 0 : (sent + failed) * 1000L / totalElapsed;
                    System.out.println("Sent " + sent + " | Failed " + failed + " | " + (sent + failed) + " | Round Elapsed " + roundElapsed + " | Total Elapsed " + totalElapsed + " | Pub/Sec " + pubPerSec);
                }
            }
            long pubPerSec = (sent+failed) * 1000L / totalElapsed;
            System.out.println("\nFINAL Sent " + sent + " | Failed " + failed + " | " + (sent+failed) + " | Total Elapsed " + totalElapsed + " | Pub/Sec " + pubPerSec);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}