// Copyright 2024 The NATS Authors
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

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.utility.Retrier;
import io.nats.client.utility.RetryConfig;

import java.io.IOException;

public class RetrierPublishExample {

    public static String STREAM = "retrier";
    public static String SUBJECT = "retriersub";

    public static void main(String[] args) throws InterruptedException {
        try (Connection nc = Nats.connect()) {
            try {
                nc.jetStreamManagement().deleteStream(STREAM);
            }
            catch (Exception ignore) {}

            // since the default backoff is {250, 250, 500, 500, 3000, 5000}
            new Thread(() -> {
                try {
                    Thread.sleep(1100);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                try {
                    System.out.println("Creating Stream @ " + System.currentTimeMillis());
                    nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                        .name(STREAM)
                        .subjects(SUBJECT)
                        .storageType(StorageType.Memory)
                        .build());
                }
                catch (IOException | JetStreamApiException e) {
                    throw new RuntimeException(e);
                }
            }).start();

            RetryConfig config = RetryConfig.builder().attempts(10).build();
            long now = System.currentTimeMillis();

            System.out.println("Publishing @ " + now);
            PublishAck pa = Retrier.publish(config, nc.jetStream(), SUBJECT, null);
            long done = System.currentTimeMillis();

            System.out.println("Publish Ack: " + pa.getJv().toJson());
            System.out.println("Done @ " + done + ", Elapsed: " + (done - now));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
