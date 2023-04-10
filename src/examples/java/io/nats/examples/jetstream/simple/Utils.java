// Copyright 2020-2023 The NATS Authors
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

package io.nats.examples.jetstream.simple;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.nats.examples.jetstream.NatsJsUtils.createOrReplaceStream;

public class Utils {
    public static String SIMPLE_STREAM = "simple-stream";
    public static String SIMPLE_SUBJECT = "simple-subject";
    public static String SIMPLE_CONSUMER_NAME = "simple-consumer";
    public static int SIMPLE_MESSAGE_COUNT = 20;

    public static void setupStreamAndDataAndConsumer(Connection nc) throws IOException, JetStreamApiException {
        setupStreamAndDataAndConsumer(nc, SIMPLE_STREAM, SIMPLE_SUBJECT, SIMPLE_MESSAGE_COUNT, SIMPLE_CONSUMER_NAME);
    }

    public static void setupStreamAndDataAndConsumer(Connection nc, int count) throws IOException, JetStreamApiException {
        setupStreamAndDataAndConsumer(nc, SIMPLE_STREAM, SIMPLE_SUBJECT, count, SIMPLE_CONSUMER_NAME);
    }

    public static void setupStreamAndDataAndConsumer(Connection nc, String stream, String subject, int count, String durable) throws IOException, JetStreamApiException {
        setupStream(nc.jetStreamManagement(), stream, subject);
        setupPublish(nc.jetStream(), subject, count);
        setupConsumer(nc.jetStreamManagement(), stream, durable, null);
    }

    public static void setupStreamAndData(Connection nc) throws IOException, JetStreamApiException {
        setupStreamAndData(nc, SIMPLE_STREAM, SIMPLE_SUBJECT, SIMPLE_MESSAGE_COUNT);
    }

    public static void setupStreamAndData(Connection nc, String stream, String subject, int count) throws IOException, JetStreamApiException {
        setupStream(nc.jetStreamManagement(), stream, subject);
        setupPublish(nc.jetStream(), subject, count);
    }

    public static void setupStream(Connection nc) throws IOException, JetStreamApiException {
        setupStream(nc.jetStreamManagement(), SIMPLE_STREAM, SIMPLE_SUBJECT);
    }

    public static void setupStream(JetStreamManagement jsm, String stream, String subject) throws IOException, JetStreamApiException {
        createOrReplaceStream(jsm, stream, StorageType.Memory, subject);
    }

    public static void setupPublish(JetStream js, String subject, int count) throws IOException, JetStreamApiException {
        for (int x = 1; x <= count; x++) {
            js.publish(subject, ("simple-message-" + x).getBytes());
        }
    }

    public static void setupConsumer(JetStreamManagement jsm, String stream, String durable, String name) throws IOException, JetStreamApiException {
        // Create durable consumer
        ConsumerConfiguration cc =
            ConsumerConfiguration.builder()
                .name(name)
                .durable(durable)
                .build();
        jsm.addOrUpdateConsumer(stream, cc);
    }

    public static class Publisher implements Runnable {
        private final JetStream js;
        private final String subject;
        private final int jitter;
        private final AtomicBoolean keepGoing = new AtomicBoolean(true);
        private int pubNo;

        public Publisher(JetStream js, String subject, int jitter) {
            this.js = js;
            this.subject = subject;
            this.jitter = jitter;
        }

        public void stop() {
            keepGoing.set(false);
        }

        @Override
        public void run() {
            try {
                while (keepGoing.get()) {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(jitter));
                    js.publish(subject, ("simple-message-" + (++pubNo)).getBytes());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
