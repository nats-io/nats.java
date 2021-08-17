// Copyright 2021 The NATS Authors
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

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.nats.client.support.NatsKeyValueUtil.*;

public class NatsKeyValueManagement extends NatsJetStreamImplBase implements KeyValueManagement {
    private final JetStreamManagement jsm;
    private final JetStream js;

    public NatsKeyValueManagement(NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        super(connection, jsOptions);
        jsm = new NatsJetStreamManagement(connection, jsOptions);
        js = new NatsJetStream(connection, jsOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BucketInfo createBucket(BucketConfiguration config) throws IOException, JetStreamApiException {
        return new BucketInfo(jsm.addStream(config.getBackingConfig()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteBucket(String bucketName) throws IOException, JetStreamApiException {
        Validator.validateBucketNameRequired(bucketName);
        return jsm.deleteStream(streamName(bucketName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BucketInfo getBucketInfo(String bucketName) throws IOException, JetStreamApiException {
        Validator.validateBucketNameRequired(bucketName);
        return new BucketInfo(jsm.getStreamInfo(streamName(bucketName)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purgeBucket(String bucketName) throws IOException, JetStreamApiException {
        Validator.validateBucketNameRequired(bucketName);
        return jsm.purgeStream(streamName(bucketName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purgeKey(String bucketName, String key) throws IOException, JetStreamApiException {
        Validator.validateBucketNameRequired(bucketName);
        Validator.validateKeyRequired(key);
        return jsm.purgeSubject(streamName(bucketName), keySubject(bucketName, key));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<KvEntry> getHistory(String bucketName, String key) throws IOException, JetStreamApiException, InterruptedException {
        Validator.validateBucketNameRequired(bucketName);
        Validator.validateKeyRequired(key);
        List<KvEntry> list = new ArrayList<>();
        visit(keySubject(bucketName, key), list::add);
        return list;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> keys(String bucketName) throws IOException, JetStreamApiException, InterruptedException {
        Validator.validateBucketNameRequired(bucketName);
        Set<String> set = new HashSet<>();
        visit(streamSubject(bucketName), kve -> set.add(kve.getKey()));
        return set;
    }

    private void visit(String subject, KvEntryHandler handler) throws IOException, JetStreamApiException, InterruptedException {
        PushSubscribeOptions pso = PushSubscribeOptions.builder()
                .configuration(ConsumerConfiguration.builder().ackPolicy(AckPolicy.None).build())
                .build();
        JetStreamSubscription sub = js.subscribe(subject, pso);
        Message m = sub.nextMessage(Duration.ofMillis(1000)); // give a little time for the first
        while (m != null) {
            handler.handle(new KvEntry(m));
            m = sub.nextMessage(Duration.ofMillis(100)); // the rest should come pretty quick
        }
        sub.unsubscribe();
    }
}
