// Copyright 2022 The NATS Authors
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
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.Digester;
import io.nats.client.support.Validator;

import java.io.*;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.nats.client.support.NatsConstants.GREATER_THAN;
import static io.nats.client.support.NatsJetStreamClientError.*;
import static io.nats.client.support.NatsObjectStoreUtil.*;

public class NatsObjectStore extends NatsFeatureBase implements ObjectStore {

    private final ObjectStoreOptions oso;
    private final String bucketName;
    private final String rawChunkPrefix;
    private final String pubSubChunkPrefix;
    private final String rawMetaPrefix;
    private final String pubSubMetaPrefix;

    NatsObjectStore(NatsConnection connection, String bucketName, ObjectStoreOptions oso) throws IOException {
        super(connection, oso);
        this.oso = oso;
        this.bucketName = Validator.validateBucketName(bucketName, true);
        streamName = toStreamName(bucketName);
        rawChunkPrefix = toChunkPrefix(bucketName);
        rawMetaPrefix = toMetaPrefix(bucketName);
        if (oso == null) {
            pubSubChunkPrefix = rawChunkPrefix;
            pubSubMetaPrefix = rawMetaPrefix;
        }
        else if (oso.getJetStreamOptions().isDefaultPrefix()) {
            pubSubChunkPrefix = rawChunkPrefix;
            pubSubMetaPrefix = rawMetaPrefix;
        }
        else {
            pubSubChunkPrefix = oso.getJetStreamOptions().getPrefix() + rawChunkPrefix;
            pubSubMetaPrefix = oso.getJetStreamOptions().getPrefix() + rawMetaPrefix;
        }
    }

    String rawChunkSubject(String nuid) {
        return rawChunkPrefix + nuid;
    }

    String pubSubChunkSubject(String nuid) {
        return pubSubChunkPrefix + nuid;
    }

    String rawMetaSubject(String name) {
        return rawMetaPrefix + encodeForSubject(name);
    }

    String rawAllMetaSubject() {
        return rawMetaPrefix + GREATER_THAN;
    }

    String pubSubMetaSubject(String name) {
        return pubSubMetaPrefix + encodeForSubject(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getBucketName() {
        return bucketName;
    }

    private ObjectInfo publishMeta(ObjectInfo info) throws IOException, JetStreamApiException {
        js.publish(NatsMessage.builder()
            .subject(rawMetaSubject(info.getObjectName()))
            .headers(getMetaHeaders())
            .data(info.serialize())
            .build()
        );
        return ObjectInfo.builder(info).modified(DateTimeUtils.gmtNow()).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo put(ObjectMeta meta, InputStream inputStream) throws IOException, JetStreamApiException, NoSuchAlgorithmException {
        Validator.validateNotNull(meta, "ObjectMeta");
        Validator.validateNotNull(meta.getObjectName(), "ObjectMeta name");
        Validator.validateNotNull(inputStream, "InputStream");
        if (meta.getObjectMetaOptions().getLink() != null) {
            throw OsLinkNotAllowOnPut.instance();
        }

        String nuid = NUID.nextGlobal();
        String chunkSubject = rawChunkSubject(nuid);

        int chunkSize = meta.getObjectMetaOptions().getChunkSize();
        if (chunkSize <= 0) {
            chunkSize = DEFAULT_CHUNK_SIZE;
        }

        try {
            Digester digester = new Digester();
            long totalSize = 0; // track total bytes read to make sure
            int chunks = 0;

            // working with chunkSize number of bytes each time.
            byte[] buffer = new byte[chunkSize];
            int red = inputStream.read(buffer);
            while (red != -1) { // keep reading while not receiving the end of file mark (-1)
                // copy if red is less than buffer length
                byte[] payload = red == buffer.length ? buffer : Arrays.copyOfRange(buffer, 0, red);

                // digest the actual bytes
                digester.update(payload);

                // publish the payload
                js.publish(chunkSubject, payload);

                // track total chunks and bytes
                chunks++;
                totalSize += red;

                red = inputStream.read(buffer);
            }

            return publishMeta(ObjectInfo.builder(bucketName, meta)
                .size(totalSize)
                .chunks(chunks)
                .nuid(nuid)
                .chunkSize(chunkSize)
                .digest(digester.getDigestEntry())
                .build());
        }
        catch (IOException | JetStreamApiException | NoSuchAlgorithmException e) {
            try {
                jsm.purgeStream(streamName, PurgeOptions.subject(rawChunkSubject(nuid)));
            }
            catch (Exception ignore) {}

            throw e;
        }
        finally {
            try { inputStream.close(); } catch (IOException ignore) {}
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo put(String objectName, InputStream inputStream) throws IOException, JetStreamApiException, NoSuchAlgorithmException {
        return put(ObjectMeta.objectName(objectName), inputStream);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo put(String objectName, byte[] input) throws IOException, JetStreamApiException, NoSuchAlgorithmException {
        return put(ObjectMeta.objectName(objectName), new ByteArrayInputStream(input));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo put(File file) throws IOException, JetStreamApiException, NoSuchAlgorithmException {
        return put(ObjectMeta.objectName(file.getName()), Files.newInputStream(file.toPath()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo get(String objectName, OutputStream out) throws IOException, JetStreamApiException, InterruptedException, NoSuchAlgorithmException {
        ObjectInfo oi = getInfo(objectName, false);
        if (oi == null) {
            throw OsObjectNotFound.instance();
        }

        if (oi.isLink()) {
            ObjectLink link = oi.getLink();
            if (link.isBucketLink()) {
                throw OsGetLinkToBucket.instance();
            }

            // is the link in the same bucket
            if (link.getBucket().equals(bucketName)) {
                return get(link.getObjectName(), out);
            }

            // different bucket
            // get the store for the linked bucket, then get the linked object
            return js.conn.objectStore(link.getBucket(), oso).get(link.getObjectName(), out);
        }

        Digester digester = new Digester();
        long totalBytes = 0;
        long totalChunks = 0;

        // if there is one chunk, just go get the message directly and we're done.
        if (oi.getChunks() == 1) {
            MessageInfo mi = jsm.getLastMessage(streamName, rawChunkSubject(oi.getNuid()));
            byte[] data = mi.getData();

            // track the byte count and chunks
            // update the digest
            // write the bytes to the output file
            totalBytes = data.length;
            totalChunks = 1;
            digester.update(data);
            out.write(data);
        }
        else {
            JetStreamSubscription sub = js.subscribe(rawChunkSubject(oi.getNuid()),
                PushSubscribeOptions.builder().stream(streamName).ordered(true).build());

            Message m = sub.nextMessage(Duration.ofSeconds(1));
            while (m != null) {
                byte[] data = m.getData();

                // track the byte count and chunks
                // update the digest
                // write the bytes to the output file
                totalBytes += data.length;
                totalChunks++;
                digester.update(data);
                out.write(data);

                // read until the subject is complete
                m = sub.nextMessage(Duration.ofSeconds(1));
            }

            sub.unsubscribe();
        }
        out.flush();

        if (totalBytes != oi.getSize()) { throw OsGetSizeMismatch.instance(); }
        if (totalChunks != oi.getChunks()) { throw OsGetChunksMismatch.instance(); }
        if (!digester.matches(oi.getDigest())) { throw OsGetDigestMismatch.instance(); }

        return oi;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo getInfo(String objectName) throws IOException, JetStreamApiException {
        return getInfo(objectName, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo getInfo(String objectName, boolean includingDeleted) throws IOException, JetStreamApiException {
        MessageInfo mi = _getLast(rawMetaSubject(objectName));
        if (mi == null) {
            return null;
        }
        ObjectInfo info = new ObjectInfo(mi);
        return includingDeleted || !info.isDeleted() ? info : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo updateMeta(String objectName, ObjectMeta meta) throws IOException, JetStreamApiException {
        Validator.validateNotNull(objectName, "object name");
        Validator.validateNotNull(meta, "ObjectMeta");
        Validator.validateNotNull(meta.getObjectName(), "ObjectMeta name");

        ObjectInfo currentInfo = getInfo(objectName, true);
        if (currentInfo == null) {
            throw OsObjectNotFound.instance();
        }
        if (currentInfo.isDeleted()) {
            throw OsObjectIsDeleted.instance();
        }

        boolean nameChange = !objectName.equals(meta.getObjectName());
        if (nameChange) {
            if (getInfo(meta.getObjectName(), false) != null) {
                throw OsObjectAlreadyExists.instance();
            }
        }

        currentInfo = publishMeta(ObjectInfo.builder(currentInfo)
            .objectName(meta.getObjectName())   // replace the name
            .description(meta.getDescription()) // replace the description
            .headers(meta.getHeaders())         // replace the headers
            .build());

        if (nameChange) {
            // delete the meta from the old name via purge stream for subject
            jsm.purgeStream(streamName, PurgeOptions.subject(rawMetaSubject(objectName)));
        }

        return currentInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo delete(String objectName) throws IOException, JetStreamApiException {
        ObjectInfo info = getInfo(objectName, true);
        if (info == null) {
            throw OsObjectNotFound.instance();
        }

        if (info.isDeleted()) {
            return info;
        }

        ObjectInfo deleted = publishMeta(ObjectInfo.builder(info)
            .deleted(true)
            .size(0)
            .chunks(0)
            .digest(null)
            .build());

        jsm.purgeStream(streamName, PurgeOptions.subject(rawChunkSubject(info.getNuid())));
        return deleted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo addLink(String objectName, ObjectInfo toInfo) throws IOException, JetStreamApiException {
        Validator.validateNotNull(objectName, "object name");
        Validator.validateNotNull(toInfo, "Link-To ObjectInfo");
        Validator.validateNotNull(toInfo.getObjectName(), "Link-To ObjectMeta");

        if (toInfo.isDeleted()) {
            throw OsObjectIsDeleted.instance();
        }

        if (toInfo.isLink()) {
            throw OsCantLinkToLink.instance();
        }

        ObjectInfo info = getInfo(objectName, false);
        if (info != null && !info.isLink()) {
            throw OsObjectAlreadyExists.instance();
        }

        return publishMeta(ObjectInfo.builder(bucketName, objectName)
            .nuid(NUID.nextGlobal())
            .objectLink(toInfo.getBucket(), toInfo.getObjectName())
            .build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo addBucketLink(String objectName, ObjectStore toStore) throws IOException, JetStreamApiException {
        Validator.validateNotNull(objectName, "object name");
        Validator.validateNotNull(toStore, "Link-To ObjectStore");

        ObjectInfo info = getInfo(objectName, false);
        if (info != null && !info.isLink()) {
            throw OsObjectAlreadyExists.instance();
        }

        return publishMeta(ObjectInfo.builder(bucketName, objectName)
            .nuid(NUID.nextGlobal())
            .bucketLink(toStore.getBucketName())
            .build());
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public ObjectStoreStatus seal() throws IOException, JetStreamApiException {
        StreamInfo si = jsm.getStreamInfo(streamName);
        si = jsm.updateStream(StreamConfiguration.builder(si.getConfiguration()).seal().build());
        return new ObjectStoreStatus(si);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ObjectInfo> getList() throws IOException, JetStreamApiException, InterruptedException {
        List<ObjectInfo> list = new ArrayList<>();
        visitSubject(rawAllMetaSubject(), DeliverPolicy.LastPerSubject, false, true, m -> {
            ObjectInfo oi = new ObjectInfo(m);
            if (!oi.isDeleted()) {
                list.add(oi);
            }
        });
        return list;
    }

    @Override
    public NatsObjectStoreWatchSubscription watch(ObjectStoreWatcher watcher, ObjectStoreWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException {
        return new NatsObjectStoreWatchSubscription(this, watcher, watchOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectStoreStatus getStatus() throws IOException, JetStreamApiException {
        return new ObjectStoreStatus(jsm.getStreamInfo(streamName));
    }
}
