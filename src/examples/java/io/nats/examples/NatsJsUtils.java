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
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Message;
import io.nats.client.impl.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class NatsJsUtils {

    public static StreamInfo createStream(Connection nc, String stream, String... subjects) throws IOException, JetStreamApiException {
        return createStream(nc.jetStreamManagement(), stream, StreamConfiguration.StorageType.Memory, subjects);
    }

    public static StreamInfo createStream(JetStreamManagement jsm, String streamName, String... subjects)
            throws IOException, JetStreamApiException {
        return createStream(jsm, streamName, StreamConfiguration.StorageType.Memory, subjects);
    }

    public static StreamInfo createOrUpdateStream(Connection nc, String stream, String... subjects) throws IOException, JetStreamApiException {
        return createOrUpdateStream(nc.jetStreamManagement(), stream, StreamConfiguration.StorageType.Memory, subjects);
    }

    public static StreamInfo createOrUpdateStream(JetStreamManagement jsm, String streamName, String... subjects)
            throws IOException, JetStreamApiException {
        return createOrUpdateStream(jsm, streamName, StreamConfiguration.StorageType.Memory, subjects);
    }

    public static boolean streamExists(Connection nc, String streamName) throws IOException, JetStreamApiException {
        return getStreamInfo(nc.jetStreamManagement(), streamName) != null;
    }

    public static boolean streamExists(JetStreamManagement jsm, String streamName) throws IOException, JetStreamApiException {
        return getStreamInfo(jsm, streamName) != null;
    }

    public static StreamInfo createStream(JetStreamManagement jsm, String streamName, StreamConfiguration.StorageType storageType, String[] subjects) throws IOException, JetStreamApiException {
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

    public static StreamInfo createOrUpdateStream(JetStreamManagement jsm, String streamName, StreamConfiguration.StorageType storageType, String... subjects)
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

    public static void publish(Connection nc, String subject, int count) throws IOException, JetStreamApiException {
        publish(nc.jetStream(), subject, 1, count);
    }

    public static void publish(JetStream js, String subject, int count) throws IOException, JetStreamApiException {
        publish(js, subject, 1, count);
    }

    public static void publish(JetStream js, String subject, int startId, int count) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            Message msg = NatsMessage.builder()
                    .subject(subject)
                    .data(("data#" + x + startId).getBytes(StandardCharsets.US_ASCII))
                    .build();
            js.publish(msg);
        }
    }

    public static void printStreamInfo(StreamInfo si) {
        printObject(si, "StreamConfiguration", "StreamState");
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
}
