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
import java.util.ArrayList;
import java.util.List;

public abstract class NatsJsPullSubBase {

    public static void createStream(Connection nc, String streamName, String subject) throws IOException, JetStreamApiException {
        createStream(nc.jetStreamManagement(), streamName, subject);
    }

    public static void createStream(JetStreamManagement jsm, String streamName, String subject) throws IOException, JetStreamApiException {
        StreamInfo si = NatsJsUtils.getStreamInfo(jsm, streamName);
        if (si == null) {
            NatsJsUtils.createStream(jsm, streamName, StreamConfiguration.StorageType.Memory, new String[] {subject});
        }
        else {
            throw new IllegalStateException("Stream already exist, examples require specific data configuration. Change the stream name or restart the server if the stream is a memory stream.");
        }
    }

    public static void publish(JetStream js, String subject, String prefix, int count) throws IOException, JetStreamApiException {
        publish(js, subject, prefix, count, true);
    }

    public static void publish(JetStream js, String subject, String prefix, int count, boolean verbose) throws IOException, JetStreamApiException {
        if (verbose) {
            System.out.print("Publish ->");
        }
        for (int x = 1; x <= count; x++) {
            String data;
            data = prefix + x;
            if (verbose) {
                System.out.print(" " + data);
            }
            Message msg = NatsMessage.builder()
                    .subject(subject)
                    .data(data.getBytes(StandardCharsets.US_ASCII))
                    .build();
            js.publish(msg);
        }
        if (verbose) {
            System.out.println(" <-");
        }
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub) throws InterruptedException {
        return readMessagesAck(sub, true);
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub, boolean verbose) throws InterruptedException {
        List<Message> messages = new ArrayList<>();
        Message msg = sub.nextMessage(Duration.ofSeconds(1));
        boolean first = true;
        while (msg != null) {
            if (first) {
                first = false;
                if (verbose) {
                    System.out.print("Read/Ack ->");
                }
            }
            messages.add(msg);
            if (msg.isJetStream()) {
                msg.ack();
                if (verbose) {
                    System.out.print(" " + new String(msg.getData()));
                }
            }
            else if (msg.isStatusMessage()) {
                if (verbose) {
                    System.out.print(" !" + msg.getStatus().getCode() + "!");
                }
            }
            else if (verbose) {
                System.out.print(" ?" + new String(msg.getData()) + "?");
            }
            msg = sub.nextMessage(Duration.ofSeconds(1));
        }

        if (verbose) {
            if (first) {
                System.out.println("No messages available.");
            }
            else {
                System.out.println(" <- ");
            }
        }

        return messages;
    }

    public static void report(List<Message> list) {
        System.out.print("Fetch ->");
        for (Message m : list) {
            System.out.print(" " + new String(m.getData()));
        }
        System.out.println(" <- ");
    }

    public static String uniqueEnough() {
        String hex = Long.toHexString(System.currentTimeMillis()).substring(6);
        StringBuilder sb = new StringBuilder();
        for (int x = 0; x < hex.length(); x++) {
            char c = hex.charAt(x);
            if (c < 58) {
                sb.append((char)(c+55));
            }
            else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
