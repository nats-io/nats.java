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

import static io.nats.examples.NatsJsUtils.printConsumerInfo;
import static io.nats.examples.NatsJsUtils.printStreamInfo;

abstract class NatsJsPullSubBase {

    public static void createStream(Connection nc, String streamName, String subject) throws IOException, JetStreamApiException {
        JetStreamManagement jsm = nc.jetStreamManagement();
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

    public static void publish(JetStream js, String subject, String prefix, int count, boolean noisy) throws IOException, JetStreamApiException {
        if (noisy) {
            System.out.print("Publish ->");
        }
        for (int x = 1; x <= count; x++) {
            String data;
            if (noisy) {
                data = "#" + prefix + x;
                System.out.print(" " + data);
            }
            else {
                data = prefix + x;
            }
            Message msg = NatsMessage.builder()
                    .subject(subject)
                    .data(data.getBytes(StandardCharsets.US_ASCII))
                    .build();
            js.publish(msg);
        }
        if (noisy) {
            System.out.println(" <-");
        }
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub) throws InterruptedException {
        return readMessagesAck(sub, true);
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub, boolean noisy) throws InterruptedException {
        List<Message> messages = new ArrayList<>();
        Message msg = sub.nextMessage(Duration.ofSeconds(1));
        boolean first = true;
        while (msg != null) {
            if (first) {
                first = false;
                if (noisy) {
                    System.out.print("Read/Ack ->");
                }
            }
            messages.add(msg);
            if (msg.isJetStream()) {
                msg.ack();
                if (noisy) {
                    System.out.print(" " + new String(msg.getData()));
                }
            }
            else if (msg.isStatusMessage()) {
                if (noisy) {
                    System.out.print(" !" + msg.getStatus().getCode() + "!");
                }
            }
            else if (noisy) {
                System.out.print(" ?" + new String(msg.getData()) + "?");
            }
            msg = sub.nextMessage(Duration.ofSeconds(1));
        }

        if (noisy) {
            if (first) {
                System.out.println("No messages available.");
            }
            else {
                System.out.println(" <- ");
            }
        }

        return messages;
    }

    public static String pad(int size) {
        return size < 10 ? " " + size : "" + size;
    }

    public static String yn(boolean b) {
        return b ? "Yes" : "No ";
    }

    public static void debug(Connection nc, String stream, String durable) throws IOException, JetStreamApiException {
        debug(nc.jetStreamManagement(), stream, durable);
    }

    public static void debug(JetStreamManagement jsm, String stream, String durable) throws IOException, JetStreamApiException {
        printStreamInfo(jsm.getStreamInfo(stream));
        printConsumerInfo(jsm.getConsumerInfo(stream, durable));
    }

    public static void debugConsumer(Connection nc, String stream, String durable) throws IOException, JetStreamApiException {
        debugConsumer(nc.jetStreamManagement(), stream, durable);
    }

    public static void debugConsumer(JetStreamManagement jsm, String stream, String durable) throws IOException, JetStreamApiException {
        ConsumerInfo ci = jsm.getConsumerInfo(stream, durable);
        // numPending=0, numWaiting=2, numAckPending=0
        System.out.println("Consumer numPending=" + ci.getNumPending() + " numWaiting=" + ci.getNumWaiting() + " numAckPending=" + ci.getNumAckPending());
    }
}
