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
import io.nats.client.support.Base64Utils;
import io.nats.client.support.JsonValueUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ObjectStoreCommand extends Command {
    final String bucket;
    final String object;
    final String url;

    public ObjectStoreCommand(Connection nc, TestMessage tm) {
        super(nc, tm);
        bucket = JsonValueUtils.readString(full, "bucket");
        object = JsonValueUtils.readString(full, "object");
        url = JsonValueUtils.readString(full, "url");
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
            createBucket();
            respond();
        }
        catch (Exception e) {
            handleException(e);
        }
    }

    private void doPutObject() {
        File f = null;
        try {
            String objectName = JsonValueUtils.readString(config, "name");
            String description = JsonValueUtils.readString(config, "description");
            ObjectStore os = nc.objectStore(bucket);
            ObjectMeta meta = ObjectMeta.builder(objectName).description(description).build();
            f = Utility.downloadToTempFile(url, "putobject", ".dat");
            InputStream inputStream = Files.newInputStream(f.toPath());
            os.put(meta, inputStream);
            respond();
        }
        catch (Exception e) {
            handleException(e);
        }
        finally {
            if (f != null) {
                //noinspection ResultOfMethodCallIgnored
                f.delete();
            }
        }
    }

    private void doGetObject() {
        try {
            ObjectStore os = nc.objectStore(bucket);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            respondDigest(os.get(object, baos));
        }
        catch (Exception e) {
            handleException(e);
        }
    }

    private void doUpdateMetadata() {
        try {
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
            respondDigest(os.get(object, baos));
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

    protected void respondDigest(ObjectInfo oi) throws UnsupportedEncodingException {
        String digest = oi.getDigest();
        Log.info("RESPOND " + subject + " digest " + digest);
        byte[] payload = Base64Utils.getUrlDecoder().decode(digest.substring(8));
        nc.publish(replyTo, payload);
    }

    private void createBucket() {
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
