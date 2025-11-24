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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private final List<String> kvBuckets;
    private final List<String> osBuckets;

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

        if (subjectCount > 0) {
            si = TestBase.createMemoryStream(jsm, stream, getSubjects(subjectCount));
        }
        else {
            si = null;
        }
        kvBuckets = new ArrayList<>();
        osBuckets = new ArrayList<>();
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

    public void createStream() throws JetStreamApiException, IOException {
        si = TestBase.createMemoryStream(jsm, stream, subject(0));
    }

    public void createStream(int subjectCount) throws JetStreamApiException, IOException {
        si = TestBase.createMemoryStream(jsm, stream, getSubjects(subjectCount));
    }

    public void createStream(String... subjects) throws JetStreamApiException, IOException {
        si = TestBase.createMemoryStream(jsm, stream, subjects);
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
        return StreamConfiguration.builder()
            .name(stream)
            .storageType(StorageType.Memory)
            .subjects(subjects);
    }

    public StreamInfo addStream(StreamConfiguration.Builder builder) throws JetStreamApiException, IOException {
        si = jsm.addStream(builder.name(stream).storageType(StorageType.Memory).build());
        return si;
    }

    public StreamInfo addStream(StreamConfiguration sc) throws JetStreamApiException, IOException {
        si = jsm.addStream(sc);
        return si;
    }

    public void replaceStream(String... newSubjects) throws JetStreamApiException, IOException {
        jsm.deleteStream(stream);
        createStream(newSubjects);
    }

    public void replaceStream(StreamConfiguration newSc) throws JetStreamApiException, IOException {
        jsm.deleteStream(stream);
        addStream(newSc);
    }

    public boolean deleteStream() throws JetStreamApiException, IOException {
        return jsm.deleteStream(stream);
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
        try { jsm.deleteStream(stream); } catch (Exception ignore) {}

        for (String bucket : kvBuckets) {
            try { kvm.delete(bucket); } catch (Exception ignore) {}
        }

        for (String bucket : osBuckets) {
            try { osm.delete(bucket); } catch (Exception ignore) {}
        }
    }
}
