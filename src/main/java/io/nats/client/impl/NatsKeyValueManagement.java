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

import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValueManagement;
import io.nats.client.KeyValueOptions;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.NatsKeyValueUtil.*;

public class NatsKeyValueManagement implements KeyValueManagement {
    private final NatsJetStreamManagement jsm;

    NatsKeyValueManagement(NatsConnection connection, KeyValueOptions kvo) throws IOException {
        jsm = new NatsJetStreamManagement(connection, kvo == null ? null : kvo.getJetStreamOptions());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueStatus create(KeyValueConfiguration config) throws IOException, JetStreamApiException {
        StreamConfiguration sc = config.getBackingConfig();
        if ( jsm.conn.getServerInfo().isOlderThanVersion("2.7.2") ) {
            sc = StreamConfiguration.builder(sc).discardPolicy(null).build(); // null discard policy will use default
        }
        return new KeyValueStatus(jsm.addStream(sc));
    }

    @Override
    public KeyValueStatus update(KeyValueConfiguration config) throws IOException, JetStreamApiException {
        return new KeyValueStatus(jsm.updateStream(config.getBackingConfig()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getBucketNames() throws IOException, JetStreamApiException, InterruptedException {
        List<String> buckets = new ArrayList<>();
        List<String> names = jsm.getStreamNames();
        for (String name : names) {
            if (name.startsWith(KV_STREAM_PREFIX)) {
                buckets.add(extractBucketName(name));
            }
        }
        return buckets;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueStatus getBucketInfo(String bucketName) throws IOException, JetStreamApiException {
        Validator.validateKvBucketNameRequired(bucketName);
        return new KeyValueStatus(jsm.getStreamInfo(toStreamName(bucketName)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String bucketName) throws IOException, JetStreamApiException {
        Validator.validateKvBucketNameRequired(bucketName);
        jsm.deleteStream(toStreamName(bucketName));
    }
}
