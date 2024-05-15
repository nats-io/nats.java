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

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.utility.Retrier;
import io.nats.client.utility.RetryConfig;

import java.io.IOException;

public class RetrierExample {

    public static void main(String[] args) throws InterruptedException {
        try (Connection nc = Nats.connect()) {
            try {
                nc.jetStreamManagement().deleteStream("test");
            }
            catch (Exception ignore) {}

            new Thread(() -> {
                try {
                    Thread.sleep(3000);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                try {
                    System.out.println("Creating Stream @ " + System.currentTimeMillis());
                    nc.jetStreamManagement().addStream(StreamConfiguration.builder()
                        .name("test")
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
            System.out.println(Retrier.publish(config, nc.jetStream(), "test", null).getJv().toJson());
            long done = System.currentTimeMillis();
            System.out.println("Done @ " + done + ", Elapsed: " + (done - now));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
