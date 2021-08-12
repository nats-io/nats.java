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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class NatsKeyValueManagement extends NatsKeyValueImplBase implements KeyValueManagement {
    private static PushSubscribeOptions historyPso;

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
        return jsm.deleteStream(toStreamName(bucketName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BucketInfo getBucketInfo(String bucketName) throws IOException, JetStreamApiException {
        return new BucketInfo(jsm.getStreamInfo(toStreamName(bucketName)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purgeBucket(String bucketName) throws IOException, JetStreamApiException {
        return jsm.purgeStream(toStreamName(bucketName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PurgeResponse purgeKey(String bucketName, String key) throws IOException, JetStreamApiException {
        return jsm.purgeSubject(toStreamName(bucketName), toSubject(bucketName, key));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<KvEntry> getHistory(String bucketName, String key) throws IOException, JetStreamApiException, InterruptedException {

        List<KvEntry> list = new ArrayList<>();
        JetStreamSubscription sub = js.subscribe(toSubject(bucketName, key), getHistoryPso());
        Message m = sub.nextMessage(Duration.ofMillis(1000)); // give a little time for the first
        while (m != null) {
            list.add(new KvEntry(m, bucketName, key));
            m = sub.nextMessage(Duration.ofMillis(100)); // the rest should come pretty quick
        }

        return list;
//        throw new UnsupportedOperationException("Key Value Management 'history' function is not implemented");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> keys(String bucketName) throws IOException, JetStreamApiException {
        throw new UnsupportedOperationException("Key Value Management 'keys' function is not implemented");
    }

    private static PushSubscribeOptions getHistoryPso() {
        if (historyPso == null) {
            historyPso = PushSubscribeOptions.builder()
                    .configuration(
                            ConsumerConfiguration.builder()
                                    .ackPolicy(AckPolicy.None)
                                    .build()
                    )
                    .build();
        }
        return historyPso;
    }
}
