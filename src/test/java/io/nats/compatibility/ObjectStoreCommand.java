// Copyright 2023 The NATS Authors
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

package io.nats.compatibility;

import io.nats.client.Connection;
import io.nats.client.ObjectStore;
import io.nats.client.ObjectStoreManagement;
import io.nats.client.api.*;
import io.nats.client.impl.NatsObjectStoreWatchSubscription;
import io.nats.client.support.ApiConstants;
import io.nats.client.support.JsonValueUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ObjectStoreCommand extends Command {

    final String bucket;
    final String object;

    public ObjectStoreCommand(Connection nc, TestMessage tm) {
        super(nc, tm);
        bucket = JsonValueUtils.readString(full, "bucket");
        object = JsonValueUtils.readString(full, "object");
    }

    public void execute() {
        switch (test) {
            case DEFAULT_BUCKET:  doCreateBucket(); break;
            case CUSTOM_BUCKET:   doCreateBucket(); break;
            case GET_OBJECT:      doGetObject(); break;
            case PUT_OBJECT:      doPutObject(); break;
            case GET_LINK:        doGetLink(); break;
            case PUT_LINK:        doPutLink(); break;
            case UPDATE_METADATA: doUpdateMetadata(); break;
            case WATCH:           doWatch(); break;
            case WATCH_UPDATE:    doWatchUpdate(); break;
            default:
                respond();
                break;
        }
    }

    private void doCreateBucket() {
        try {
            _createBucket();
            respond();
        }
        catch (Exception e) {
            handleException(e);
        }
    }

    private void doPutObject() {
        try {
            _createBucket();
            String objectName = JsonValueUtils.readString(config, "name");
            String description = JsonValueUtils.readString(config, "description");
            _putObject(objectName, description);
            respond();
        }
        catch (Exception e) {
            handleException(e);
        }
    }

    private void doGetObject() {
        try {
            _createBucket();
            _putObject(object, null);
            ObjectStore os = nc.objectStore(bucket);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            _respondDigest(os.get(object, baos));
        }
        catch (Exception e) {
            handleException(e);
        }
    }

    private void doUpdateMetadata() {
        try {
            _createBucket();
            _putObject(object, null);
            ObjectStore os = nc.objectStore(bucket);
            String objectName = JsonValueUtils.readString(config, "name");
            String description = JsonValueUtils.readString(config, "description");
            ObjectMeta meta = ObjectMeta.builder(objectName).description(description).build();
            os.updateMeta(object, meta);
            respond();
        }
        catch (Exception e) {
            handleException(e);
        }
    }

    private void doGetLink() {
        try {
            ObjectStore os = nc.objectStore(bucket);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            _respondDigest(os.get(object, baos));
        }
        catch (Exception e) {
            handleException(e);
        }
    }

    private void doPutLink() {
        try {
            String linkName = JsonValueUtils.readString(full, "link_name");

            ObjectStore os = nc.objectStore(bucket);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectInfo oi = os.get(object, baos);
            os.addLink(linkName, oi);
            respond();
        }
        catch (Exception e) {
            handleException(e);
        }
    }

    private void doWatch() {
        try {
            ObjectStore os = nc.objectStore(bucket);
            WatchWatcher ww = new WatchWatcher();
            try (NatsObjectStoreWatchSubscription ws = os.watch(ww)) {
                ww.latch.await(2, TimeUnit.SECONDS);
            }
            respond(ww.result);
        }
        catch (Exception e) {
            handleException(e);
        }
    }

    static class WatchWatcher implements ObjectStoreWatcher {
        public CountDownLatch latch = new CountDownLatch(1);
        public String result;

        @Override
        public void watch(ObjectInfo oi) {
            if (result == null) {
                result = oi.getDigest();
            }
            else {
                result = result + "," + oi.getDigest();
                latch.countDown();
            }
        }

        @Override
        public void endOfData() {}
    }

    private void doWatchUpdate() {
        try {
            ObjectStore os = nc.objectStore(bucket);
            WatchUpdatesWatcher wuw = new WatchUpdatesWatcher();
            try (NatsObjectStoreWatchSubscription ws = os.watch(wuw)) {
                wuw.latch.await(2, TimeUnit.SECONDS);
            }
            respond(wuw.result);
        }
        catch (Exception e) {
            handleException(e);
        }
    }

    static class WatchUpdatesWatcher implements ObjectStoreWatcher {
        public CountDownLatch latch = new CountDownLatch(1);
        public String result;

        @Override
        public void watch(ObjectInfo oi) {
            if (result == null) {
                result = oi.getDigest();
            }
            else {
                result = oi.getDigest();
                latch.countDown();
            }
        }

        @Override
        public void endOfData() {}
    }

    protected void _respondDigest(ObjectInfo oi) {
        String digest = oi.getDigest();
        Log.info("RESPOND " + subject + " digest " + digest);
        byte[] payload = Base64.getUrlDecoder().decode(digest.substring(8));
        nc.publish(replyTo, payload);
    }

    private void _putObject(String objectName, String description) {
        try {
            ObjectStore os = nc.objectStore(bucket);
            ObjectMeta meta = ObjectMeta.builder(objectName).description(description).build();
            InputStream inputStream = Utility.getFileAsInputStream("nats-server.zip");
            os.put(meta, inputStream);
        }
        catch (Exception e) {
            handleException(e);
        }
    }

    private void _createBucket() {
        ObjectStoreManagement osm;
        try {
            osm = nc.objectStoreManagement();
        }
        catch (IOException e) {
            handleException(e);
            return;
        }

        ObjectStoreConfiguration osc = extractObjectStoreConfiguration();
        try {
            osm.delete(osc.getBucketName());
        }
        catch (Exception ignore) {}

        try {
            osm.create(osc);
        }
        catch (Exception e) {
            handleException(e);
        }
    }

    private ObjectStoreConfiguration extractObjectStoreConfiguration() {
        ObjectStoreConfiguration.Builder builder = ObjectStoreConfiguration.builder();
        if (bucket != null) {
            builder.name(bucket);
        }
        else {
            String s = JsonValueUtils.readString(config, ApiConstants.BUCKET);
            if (s != null) {
                builder.name(s);
            }
            s = JsonValueUtils.readString(config, ApiConstants.DESCRIPTION);
            if (s != null) {
                builder.description(s);
            }
            Long l = JsonValueUtils.readLong(config, ApiConstants.MAX_BYTES);
            if (l != null) {
                builder.maxBucketSize(l);
            }
            Duration d = JsonValueUtils.readNanos(config, ApiConstants.MAX_AGE);
            if (d != null) {
                builder.ttl(d);
            }
            s = JsonValueUtils.readString(config, ApiConstants.STORAGE);
            if (s != null) {
                builder.storageType(StorageType.get(s));
            }
            Integer i = JsonValueUtils.readInteger(config, ApiConstants.NUM_REPLICAS);
            if (i != null) {
                builder.replicas(i);
            }
        }
        return builder.build();
    }
}
