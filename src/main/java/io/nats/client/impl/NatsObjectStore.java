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
import io.nats.client.support.Digester;
import io.nats.client.support.Validator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.DateTimeUtils.ZONE_ID_GMT;
import static io.nats.client.support.NatsJetStreamClientError.*;
import static io.nats.client.support.NatsObjectStoreUtil.*;

public class NatsObjectStore extends NatsFeatureBase implements ObjectStore {

    private final ObjectStoreOptions oso;
    private final String bucketName;
    private final String streamName;
    private final String chunkStreamSubject;
    private final String metaStreamSubject;
    private final String rawChunkPrefix;
    private final String pubSubChunkPrefix;
    private final String rawMetaPrefix;
    private final String pubSubMetaPrefix;

    NatsObjectStore(NatsConnection connection, String bucketName, ObjectStoreOptions oso) throws IOException {
        super(connection, oso);
        this.oso = oso;
        this.bucketName = Validator.validateBucketName(bucketName, true);
        streamName = toStreamName(bucketName);
        chunkStreamSubject = toChunkStreamSubject(bucketName);
        metaStreamSubject = toMetaStreamSubject(bucketName);
        rawChunkPrefix = toChunkPrefix(bucketName);
        rawMetaPrefix = toMetaPrefix(bucketName);
        if (oso == null) {
            pubSubChunkPrefix = rawChunkPrefix;
            pubSubMetaPrefix = rawMetaPrefix;
        }
        else {
            if (oso.getJetStreamOptions().isDefaultPrefix()) {
                pubSubChunkPrefix = rawChunkPrefix;
                pubSubMetaPrefix = rawMetaPrefix;
            }
            else {
                pubSubChunkPrefix = oso.getJetStreamOptions().getPrefix() + rawChunkPrefix;
                pubSubMetaPrefix = oso.getJetStreamOptions().getPrefix() + rawMetaPrefix;
            }
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
        return rawMetaPrefix + ">";
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

    @Override
    public ObjectInfo put(ObjectMeta meta, InputStream inputStream) throws IOException, JetStreamApiException, NoSuchAlgorithmException {
        // TODO
        return publishMeta(meta, 1025, 2, new Digester().update("blah"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo put(ObjectMeta meta, byte[] input) throws IOException, JetStreamApiException, NoSuchAlgorithmException {
        return put(meta, new ByteArrayInputStream(input));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo put(ObjectMeta meta, String input) throws IOException, JetStreamApiException, NoSuchAlgorithmException {
        return put(meta, new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo put(ObjectMeta meta, File file) throws IOException, JetStreamApiException, NoSuchAlgorithmException {
        return put(meta, new FileInputStream(file));
    }

    private int chunkSize(ObjectMeta meta) {
        int chunkSize = meta.getObjectMetaOptions().getChunkSize();
        return chunkSize > 0 ? chunkSize : DEFAULT_CHUNK_SIZE;
    }

    private ObjectInfo publishInfo(ObjectInfo.Builder infoBuilder) {
        ObjectInfo info = infoBuilder.modified(ZonedDateTime.now(ZONE_ID_GMT)).build();
        js.publishAsync(NatsMessage.builder()
            .subject(pubSubMetaSubject(info.getObjectName()))
            .headers(META_HEADERS)
            .data(info.serialize())
            .build()
        );
        return info;
    }

    private ObjectInfo publishMeta(ObjectMeta meta, int size, int chunks, Digester digester) {
        return publishInfo(ObjectInfo.builder(bucketName, meta)
            .nuid(NUID.nextGlobal())
            .size(size)
            .chunks(chunks)
            .digest(digester.getDigestEntry()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void get(String objectName, OutputStream outputStream) throws IOException, JetStreamApiException, InterruptedException {
        // TODO
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo getInfo(String objectName) throws IOException, JetStreamApiException {
        MessageInfo mi = _getLast(streamName, rawMetaSubject(objectName));
        return mi == null ? null : new ObjectInfo(mi);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo updateMeta(String objectName, ObjectMeta meta) throws IOException, JetStreamApiException {
        Validator.validateNotNull(objectName, "object name");
        Validator.validateNotNull(meta, "ObjectMeta");
        Validator.validateNotNull(meta.getObjectName(), "ObjectMeta name");

        ObjectInfo info = getInfo(objectName);
        if (info == null || info.isDeleted()) {
            throw OsObjectNotFoundOrIsDeleted.instance();
        }

        boolean nameChange = !objectName.equals(meta.getObjectName());
        if (nameChange) {
            ObjectInfo newOi = getInfo(meta.getObjectName());
            if (newOi != null && !newOi.isDeleted()) {
                throw OsObjectAlreadyExists.instance();
            }
        }

        info = publishInfo(ObjectInfo.builder(info)
            .objectName(meta.getObjectName())   // replace the name
            .description(meta.getDescription()) // replace the description
            .headers(meta.getHeaders()));       // replace the headers

        if (nameChange) {
            // delete the meta from the old name via purge stream for subject
            jsm.purgeStream(streamName, PurgeOptions.subject(rawMetaSubject(info.getObjectName())));
        }

        return info;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo delete(String objectName) throws IOException, JetStreamApiException {
        ObjectInfo info = getInfo(objectName);
        if (info == null) {
            throw OsObjectNotFound.instance();
        }

        if (info.isDeleted()) {
            return info;
        }

        ObjectInfo deleted = publishInfo(ObjectInfo.builder(info)
            .deleted(true)
            .size(0)
            .chunks(0)
            .digest(null));

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

        ObjectInfo info = getInfo(objectName);
        if (info != null && !info.isDeleted() && !info.isLink()) {
            throw OsObjectAlreadyExists.instance();
        }

        return publishInfo(ObjectInfo.builder(bucketName, objectName)
            .objectLink(toInfo.getBucket(), toInfo.getObjectName()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo addBucketLink(String objectName, ObjectStore toStore) throws IOException, JetStreamApiException {
        Validator.validateNotNull(objectName, "object name");
        Validator.validateNotNull(toStore, "Link-To ObjectStore");

        ObjectInfo info = getInfo(objectName);
        if (info != null && !info.isDeleted() && !info.isLink()) {
            throw OsObjectAlreadyExists.instance();
        }

        return publishInfo(ObjectInfo.builder(bucketName, objectName)
            .bucketLink(toStore.getBucketName()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seal() throws IOException, JetStreamApiException {
        StreamInfo si = jsm.getStreamInfo(streamName);
        jsm.updateStream(StreamConfiguration.builder(si.getConfiguration()).seal().build());
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
