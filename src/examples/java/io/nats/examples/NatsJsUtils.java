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
import io.nats.client.JetStreamManagement;
import io.nats.client.StreamConfiguration;
import io.nats.client.StreamInfo;
import io.nats.client.impl.JetStreamApiException;

import java.io.IOException;
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
            sc = new StreamConfiguration.Builder(sc).subjects(combined).build();
            si = jsm.updateStream(sc);
        }

        System.out.printf("Existing stream %s with subject(s) %s created at %s.\n",
                streamName, si.getConfiguration().getSubjects(), si.getCreateTime().toLocalTime().toString());

        return si;
    }

    public static StreamInfo getStreamInfo(JetStreamManagement jsm, String streamName) throws IOException, JetStreamApiException {
        try {
            return jsm.streamInfo(streamName);
        }
        catch (JetStreamApiException jsae) {
            if (jsae.getErrorCode() == 404) {
                return null;
            }
            throw jsae;
        }
    }
}
