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

package io.nats.client.impl;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.*;
import io.nats.client.utils.TestBase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JetStreamTestingContext implements AutoCloseable {
    public final NatsJetStreamManagement jsm;
    public final NatsJetStream js;
    public final NatsKeyValueManagement kvm;
    public final NatsObjectStoreManagement osm;

    private final String subjectBase;
    private final Map<Object, String> subjects;
    private final String consumerNameBase;
    private final Map<Object, String> consumerNames;
    public String stream;
    public StreamInfo si;

    private final Set<String> streams;
    private final Set<String> kvBuckets;
    private final Set<String> osBuckets;

    public JetStreamTestingContext(Connection nc, int subjectCount) throws JetStreamApiException, IOException {
        jsm = (NatsJetStreamManagement)nc.jetStreamManagement();
        js = (NatsJetStream)jsm.jetStream();
        kvm = (NatsKeyValueManagement)jsm.keyValueManagement();
        osm = (NatsObjectStoreManagement)jsm.objectStoreManagement();

        stream = TestBase.random();
        subjectBase = TestBase.random();
        subjects = new HashMap<>();
        consumerNameBase = TestBase.random();
        consumerNames = new HashMap<>();

        streams = new HashSet<>();
        kvBuckets = new HashSet<>();
        osBuckets = new HashSet<>();

        if (subjectCount > 0) {
            createOrReplaceStream(subjectCount);
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // JetStream
    // ----------------------------------------------------------------------------------------------------
    public String[] getSubjects(int subjectCount) {
        String[] subjects = new String[subjectCount];
        for (int x = 0; x < subjectCount; x++) {
            subjects[x] = subject(x);
        }
        return subjects;
    }

    public void createOrReplaceStream() throws JetStreamApiException, IOException {
        createOrReplaceStream(scBuilder(subject(0)).build());
    }

    public void createOrReplaceStream(int subjectCount) throws JetStreamApiException, IOException {
        createOrReplaceStream(scBuilder(getSubjects(subjectCount)).build());
    }

    public void createOrReplaceStream(String... subjects) throws JetStreamApiException, IOException {
        createOrReplaceStream(scBuilder(subjects).build());
    }

    public void createOrReplaceStream(StreamConfiguration.Builder builder) throws JetStreamApiException, IOException {
        createOrReplaceStream(builder.build());
    }

    public StreamInfo createOrReplaceStream(StreamConfiguration sc) throws JetStreamApiException, IOException {
        String streamName = sc.getName();
        try { jsm.deleteStream(streamName); } catch (Exception ignore) {}
        streams.remove(streamName);
        si = jsm.addStream(sc);
        streams.add(streamName);
        return si;
    }

    public StreamInfo addStream(StreamConfiguration sc) throws JetStreamApiException, IOException {
        String streamName = sc.getName();
        si = jsm.addStream(sc);
        streams.add(streamName);
        return si;
    }

    public StreamConfiguration.Builder scBuilder(int subjectCount) {
        StreamConfiguration.Builder b = StreamConfiguration.builder()
            .name(stream)
            .storageType(StorageType.Memory);
        if (subjectCount > 0) {
            b.subjects(getSubjects(subjectCount));
        }
        return b;
    }

    public StreamConfiguration.Builder scBuilder(String... subjects) {
        if (subjects.length == 0) {
            subjects = new String[]{subject(0)};
        }
        return StreamConfiguration.builder()
            .name(stream)
            .storageType(StorageType.Memory)
            .subjects(subjects);
    }

    public boolean deleteStream() throws JetStreamApiException, IOException {
        boolean deleted = jsm.deleteStream(stream);
        if (deleted) {
            streams.remove(stream);
        }
        return deleted;
    }

    public String subject() {
        return subject(0);
    }

    public String subject(Object variant) {
        return subjects.computeIfAbsent(variant, v -> subjectBase + "_" + v);
    }

    public String consumerName() {
        return consumerNameBase;
    }

    public String consumerName(Object variant) {
        return consumerNames.computeIfAbsent(variant, v -> consumerNameBase + "-" + v);
    }

    // ----------------------------------------------------------------------------------------------------
    // KeyValue
    // ----------------------------------------------------------------------------------------------------
    public KeyValueConfiguration.Builder kvBuilder(String bucketName) {
        return KeyValueConfiguration.builder()
            .name(bucketName)
            .storageType(StorageType.Memory);
    }

    public KeyValueStatus kvCreate(String bucketName) throws JetStreamApiException, IOException {
        return kvCreate(kvBuilder(bucketName).build());
    }

    public KeyValueStatus kvCreate(KeyValueConfiguration.Builder builder) throws JetStreamApiException, IOException {
        return kvCreate(builder.build());
    }

    public KeyValueStatus kvCreate(KeyValueConfiguration kvc) throws JetStreamApiException, IOException {
        kvBuckets.add(kvc.getBucketName());
        return kvm.create(kvc);
    }

    // ----------------------------------------------------------------------------------------------------
    // ObjectStore
    // ----------------------------------------------------------------------------------------------------
    public ObjectStoreConfiguration.Builder osBuilder(String bucketName) {
        return ObjectStoreConfiguration.builder()
            .name(bucketName)
            .storageType(StorageType.Memory);
    }

    public ObjectStoreStatus osCreate(String bucketName) throws JetStreamApiException, IOException {
        return osCreate(osBuilder(bucketName).build());
    }

    public ObjectStoreStatus osCreate(ObjectStoreConfiguration.Builder builder) throws JetStreamApiException, IOException {
        return osCreate(builder.build());
    }

    public ObjectStoreStatus osCreate(ObjectStoreConfiguration osc) throws JetStreamApiException, IOException {
        osBuckets.add(osc.getBucketName());
        return osm.create(osc);
    }

    @Override
    public void close() throws Exception {
        for (String strm : streams) {
            try { jsm.deleteStream(strm); } catch (Exception ignore) {}
        }

        for (String bucket : kvBuckets) {
            try { kvm.delete(bucket); } catch (Exception ignore) {}
        }

        for (String bucket : osBuckets) {
            try { osm.delete(bucket); } catch (Exception ignore) {}
        }
    }
}
