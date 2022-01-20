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
import io.nats.client.ObjectStoreManagement;
import io.nats.client.ObjectStoreOptions;
import io.nats.client.api.ObjectStoreConfiguration;
import io.nats.client.api.ObjectStoreStatus;
import io.nats.client.support.Validator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.NatsObjectStoreUtil.*;

public class NatsObjectStoreManagement extends NatsFeatureBase implements ObjectStoreManagement {

    NatsObjectStoreManagement(NatsConnection connection, ObjectStoreOptions oso) throws IOException {
        super(connection, oso);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectStoreStatus create(ObjectStoreConfiguration config) throws IOException, JetStreamApiException {
        return new ObjectStoreStatus(jsm.addStream(config.getBackingConfig()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getObjectStoreBucketNames() throws IOException, JetStreamApiException, InterruptedException {
        List<String> buckets = new ArrayList<>();
        List<String> streamNames = jsm.getStreamNames();
        for (String sn : streamNames) {
            if (sn.startsWith(OBJ_STREAM_PREFIX)) {
                buckets.add(extractBucketName(sn));
            }
        }
        return buckets;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectStoreStatus getObjectStoreInfo(String bucketName) throws IOException, JetStreamApiException {
        Validator.validateObjectStoreBucketNameRequired(bucketName);
        return new ObjectStoreStatus(jsm.getStreamInfo(toStreamName(bucketName)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String bucketName) throws IOException, JetStreamApiException {
        Validator.validateObjectStoreBucketNameRequired(bucketName);
        jsm.deleteStream(toStreamName(bucketName));
    }
}
