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
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NatsJsUtils {

    // ----------------------------------------------------------------------------------------------------
    // STREAM CREATE / UPDATE / INFO
    // ----------------------------------------------------------------------------------------------------
    public static StreamInfo createStream(Connection nc, String stream, String... subjects) throws IOException, JetStreamApiException {
        return createStream(nc.jetStreamManagement(), stream, StorageType.Memory, subjects);
    }

    public static StreamInfo createStream(JetStreamManagement jsm, String streamName, String... subjects)
            throws IOException, JetStreamApiException {
        return createStream(jsm, streamName, StorageType.Memory, subjects);
    }

    public static void createStreamThrowWhenExists(Connection nc, String streamName, String subject) throws IOException, JetStreamApiException {
        createStreamThrowWhenExists(nc.jetStreamManagement(), streamName, subject);
    }

    public static void createStreamThrowWhenExists(JetStreamManagement jsm, String streamName, String subject) throws IOException, JetStreamApiException {
        StreamInfo si = NatsJsUtils.getStreamInfo(jsm, streamName);
        if (si == null) {
            createStream(jsm, streamName, StorageType.Memory, new String[] {subject});
        }
        else {
            throw new IllegalStateException("Stream already exist, examples require specific data configuration. Change the stream name or restart the server if the stream is a memory stream.");
        }
    }

    public static StreamInfo createStream(JetStreamManagement jsm, String streamName, StorageType storageType, String[] subjects) throws IOException, JetStreamApiException {
        // Create a stream, here will use a file storage type, and one subject,
        // the passed subject.
        StreamConfiguration sc = StreamConfiguration.builder()
                .name(streamName)
                .storageType(storageType)
                .subjects(subjects)
                .build();

        // Add or use an existing stream.
        StreamInfo si = jsm.addStream(sc);
        System.out.printf("Created stream %s with subject(s) %s created at %s.\n",
                streamName, String.join(",", subjects), si.getCreateTime().toLocalTime().toString());

        return si;
    }

    public static StreamInfo createOrUpdateStream(Connection nc, String stream, String... subjects) throws IOException, JetStreamApiException {
        return createOrUpdateStream(nc.jetStreamManagement(), stream, StorageType.Memory, subjects);
    }

    public static StreamInfo createOrUpdateStream(JetStreamManagement jsm, String streamName, String... subjects)
            throws IOException, JetStreamApiException {
        return createOrUpdateStream(jsm, streamName, StorageType.Memory, subjects);
    }

    public static StreamInfo createOrUpdateStream(JetStreamManagement jsm, String streamName, StorageType storageType, String... subjects)
            throws IOException, JetStreamApiException {

        StreamInfo si = getStreamInfo(jsm, streamName);
        if (si == null) {
            return createStream(jsm, streamName, storageType, subjects);
        }

        // check to see if the configuration has all the subject we want
        StreamConfiguration sc = si.getConfiguration();
        boolean needToUpdate = false;
        List<String> combined = sc.getSubjects();
        for (String sub : subjects) {
            if (!combined.contains(sub)) {
                needToUpdate = true;
                combined.add(sub);
            }
        }

        if (needToUpdate) {
            sc = StreamConfiguration.builder(sc).subjects(combined).build();
            si = jsm.updateStream(sc);
        }

        System.out.printf("Existing stream %s with subject(s) %s created at %s.\n",
                streamName, si.getConfiguration().getSubjects(), si.getCreateTime().toLocalTime().toString());

        return si;
    }

    public static boolean streamExists(Connection nc, String streamName) throws IOException, JetStreamApiException {
        return getStreamInfo(nc.jetStreamManagement(), streamName) != null;
    }

    public static boolean streamExists(JetStreamManagement jsm, String streamName) throws IOException, JetStreamApiException {
        return getStreamInfo(jsm, streamName) != null;
    }

    public static StreamInfo getStreamInfo(JetStreamManagement jsm, String streamName) throws IOException, JetStreamApiException {
        try {
            return jsm.getStreamInfo(streamName);
        }
        catch (JetStreamApiException jsae) {
            if (jsae.getErrorCode() == 404) {
                return null;
            }
            throw jsae;
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // PUBLISH
    // ----------------------------------------------------------------------------------------------------
    public static void publish(Connection nc, String subject, int count) throws IOException, JetStreamApiException {
        publish(nc.jetStream(), subject, "data", count, false);
    }

    public static void publish(JetStream js, String subject, int count) throws IOException, JetStreamApiException {
        publish(js, subject, "data", count, false);
    }

    public static void publish(JetStream js, String subject, String prefix, int count) throws IOException, JetStreamApiException {
        publish(js, subject, prefix, count, true);
    }

    public static void publish(JetStream js, String subject, String prefix, int count, boolean verbose) throws IOException, JetStreamApiException {
        if (verbose) {
            System.out.print("Publish ->");
        }
        for (int x = 1; x <= count; x++) {
            String data = prefix + x;
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

    public static void publishDontWait(JetStream js, String subject, String prefix, int count) {
        new Thread(() -> {
            try {
                for (int x = 1; x <= count; x++) {
                    String data = prefix + "-" + x;
                    Message msg = NatsMessage.builder()
                            .subject(subject)
                            .data(data.getBytes(StandardCharsets.US_ASCII))
                            .build();
                    js.publishAsync(msg);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }).start();
    }

    // ----------------------------------------------------------------------------------------------------
    // READ MESSAGES
    // ----------------------------------------------------------------------------------------------------
    public static List<Message> readMessagesAck(JetStreamSubscription sub) throws InterruptedException {
        return readMessagesAck(sub, true, Duration.ofSeconds(1));
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub, boolean verbose) throws InterruptedException {
        return readMessagesAck(sub, verbose, Duration.ofSeconds(1));
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub, Duration nextMessageTimeout) throws InterruptedException {
        return readMessagesAck(sub, true, nextMessageTimeout);
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub, boolean verbose, Duration nextMessageTimeout) throws InterruptedException {
        List<Message> messages = new ArrayList<>();
        Message msg = sub.nextMessage(nextMessageTimeout);
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
            msg = sub.nextMessage(nextMessageTimeout);
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

    // ----------------------------------------------------------------------------------------------------
    // PRINT
    // ----------------------------------------------------------------------------------------------------
    public static void printStreamInfo(StreamInfo si) {
        printObject(si, "StreamConfiguration", "StreamState", "ClusterInfo", "Mirror", "sources");
    }

    public static void printStreamInfoList(List<StreamInfo> list) {
        printObject(list, "!StreamInfo", "StreamConfiguration", "StreamState");
    }

    public static void printConsumerInfo(ConsumerInfo ci) {
        printObject(ci, "ConsumerConfiguration", "Delivered", "AckFloor");
    }

    public static void printConsumerInfoList(List<ConsumerInfo> list) {
        printObject(list, "!ConsumerInfo", "ConsumerConfiguration", "Delivered", "AckFloor");
    }

    public static void printObject(Object o, String... subObjectNames) {
        String s = o.toString();
        for (String sub : subObjectNames) {
            boolean noIndent = sub.startsWith("!");
            String sb = noIndent ? sub.substring(1) : sub;
            String rx1 = ", " + sb;
            String repl1 = (noIndent ? ",\n": ",\n    ") + sb;
            s = s.replace(rx1, repl1);
        }

        System.out.println(s + "\n");
    }

    // ----------------------------------------------------------------------------------------------------
    // REPORT
    // ----------------------------------------------------------------------------------------------------
    public static void report(List<Message> list) {
        System.out.print("Fetch ->");
        for (Message m : list) {
            System.out.print(" " + new String(m.getData()));
        }
        System.out.println(" <- ");
    }

    public static List<Message> report(Iterator<Message> list) {
        List<Message> messages = new ArrayList<>();
        System.out.print("Fetch ->");
        while (list.hasNext()) {
            Message m = list.next();
            messages.add(m);
            System.out.print(" " + new String(m.getData()));
        }
        System.out.println(" <- ");
        return messages;
    }

    // ----------------------------------------------------------------------------------------------------
    // MISC
    // ----------------------------------------------------------------------------------------------------
    public static int countJs(List<Message> messages) {
        int count = 0;
        for (Message m : messages) {
            if (m.isJetStream()) {
                count++;
            }
        }
        return count;
    }

    public static int count408s(List<Message> messages) {
        int count = 0;
        for (Message m : messages) {
            if (m.isStatusMessage() && m.getStatus().getCode() == 408) {
                count++;
            }
        }
        return count;
    }
}
