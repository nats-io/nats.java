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
import io.nats.client.support.Digester;
import io.nats.client.support.Validator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;

import static io.nats.client.support.DateTimeUtils.ZONE_ID_GMT;
import static io.nats.client.support.NatsJetStreamClientError.*;
import static io.nats.client.support.NatsJetStreamConstants.JS_NO_MESSAGE_FOUND_ERR;
import static io.nats.client.support.NatsObjectStoreUtil.*;

public class NatsObjectStore implements ObjectStore {

    final NatsJetStream js;
    final JetStreamManagement jsm;

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
        this.oso = oso;
        this.bucketName = Validator.validateBucketName(bucketName, true);
        streamName = toStreamName(bucketName);
        chunkStreamSubject = toChunkStreamSubject(bucketName);
        metaStreamSubject = toMetaStreamSubject(bucketName);
        rawChunkPrefix = toChunkPrefix(bucketName);
        rawMetaPrefix = toMetaPrefix(bucketName);
        if (oso == null) {
            js = new NatsJetStream(connection, null);
            jsm = new NatsJetStreamManagement(connection, null);
            pubSubChunkPrefix = rawChunkPrefix;
            pubSubMetaPrefix = rawMetaPrefix;
        }
        else {
            js = new NatsJetStream(connection, oso.getJetStreamOptions());
            jsm = new NatsJetStreamManagement(connection, oso.getJetStreamOptions());
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

    private ObjectInfo publishMeta(ObjectInfo info) {
        js.publishAsync(NatsMessage.builder()
            .subject(pubSubMetaSubject(info.getName()))
            .headers(META_HEADERS)
            .data(info.serialize())
            .build()
        );
        return info;
    }

    private ObjectInfo publishMeta(ObjectMeta meta, int size, int chunks, Digester digester) {
        ObjectInfo info = ObjectInfo.builder()
            .bucket(bucketName)
            .nuid(NUID.nextGlobal())
            .size(size)
            .chunks(chunks)
            .digest(digester.getDigestEntry())
            .meta(meta)
            .modified(ZonedDateTime.now(ZONE_ID_GMT))
            .build();

        return publishMeta(info);
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
        try {
            return new ObjectInfo(jsm.getLastMessage(streamName, rawMetaSubject(objectName)));
        } catch (JetStreamApiException jsae) {
            if (jsae.getApiErrorCode() == JS_NO_MESSAGE_FOUND_ERR) {
                return null;
            }
            throw jsae;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo updateMeta(String objectName, ObjectMeta meta) throws IOException, JetStreamApiException {
        Validator.validateNotNull(objectName, "object name");
        Validator.validateNotNull(meta, "ObjectMeta");
        Validator.validateNotNull(meta.getName(), "ObjectMeta name");

        ObjectInfo info = getInfo(objectName);
        if (info == null || info.isDeleted()) {
            throw OsObjectNotFoundOrIsDeleted.instance();
        }

        boolean nameChange = !objectName.equals(meta.getName());
        if (nameChange) {
            ObjectInfo newOi = getInfo(meta.getName());
            if (newOi != null && !newOi.isDeleted()) {
                throw OsObjectAlreadyExists.instance();
            }
        }

        info = publishMeta(ObjectInfo.builder(info)
            .name(meta.getName())
            .description(meta.getDescription())
            .headers(meta.getHeaders())
            .build());

        if (nameChange) {
            // delete the meta from the old name via purge stream for subject
            jsm.purgeStream(streamName, PurgeOptions.subject(rawMetaSubject(info.getName())));
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

        ObjectInfo deleted = publishMeta(ObjectInfo.builder(info)
            .deleted(true)
            .size(0)
            .chunks(0)
            .digest(null)
            // .modified(null)
            // .description(null)
            // .chunkSize(-1)
            // .link(null)
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
        Validator.validateNotNull(toInfo.getName(), "Link-To ObjectMeta");

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

        return publishMeta(ObjectInfo.builder()
            .name(objectName)
            .link(ObjectLink.builder()
                .bucket(toInfo.getBucket())
                .name(toInfo.getName())
                .build())
            .modified(ZonedDateTime.now(ZONE_ID_GMT))
            .build());
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

        return publishMeta(ObjectInfo.builder()
            .name(objectName)
            .link(ObjectLink.builder()
                .bucket(toStore.getBucketName())
                .build())
            .modified(ZonedDateTime.now(ZONE_ID_GMT))
            .build());
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
    public ObjectStoreStatus getStatus() throws IOException, JetStreamApiException, InterruptedException {
        return new ObjectStoreStatus(jsm.getStreamInfo(streamName));
    }
}
