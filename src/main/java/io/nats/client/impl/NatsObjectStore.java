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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.nats.client.support.NatsObjectStoreUtil.*;
import static io.nats.client.support.Validator.validateNotNull;
import static io.nats.client.support.Validator.validateNotNullOrEmpty;

public class NatsObjectStore extends NatsFeatureBase implements ObjectStore {

    private final String bucketName;
    private final String streamName;
    private final String metaStreamSubject;
    private final String chunkStreamSubject;
    private final String plainMetaPrefix;
    private final String publishMetaPrefix;
    private final String plainChunkPrefix;
    private final String publishChunkPrefix;

    NatsObjectStore(NatsConnection connection, String bucketName, ObjectStoreOptions oso) throws IOException {
        super(connection, oso);
        this.bucketName = Validator.validateObjectStoreBucketNameRequired(bucketName);
        streamName = toStreamName(bucketName);
        metaStreamSubject = toMetaStreamSubject(bucketName);
        chunkStreamSubject = toChunkStreamSubject(bucketName);
        plainMetaPrefix = toMetaPrefix(bucketName);
        plainChunkPrefix = toChunkPrefix(bucketName);
        if (oso == null) {
            publishMetaPrefix = plainMetaPrefix;
            publishChunkPrefix = plainChunkPrefix;
        }
        else {
            publishMetaPrefix = oso.getFeaturePrefix() == null ? plainMetaPrefix : oso.getFeaturePrefix();
            publishChunkPrefix = oso.getFeaturePrefix() == null ? plainChunkPrefix : oso.getFeaturePrefix();
        }
    }

    String toPlainMetaSubject(String objectName) {
        return plainMetaPrefix + encodeForSubject(objectName);
    }

    String toPublishMetaSubject(String objectName) {
        return publishMetaPrefix + encodeForSubject(objectName);
    }

    String toPlainChunkSubject(String nuid) {
        return plainChunkPrefix + nuid;
    }

    String toPublishChunkSubject(String nuid) {
        return publishChunkPrefix + nuid;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getBucketName() {
        return bucketName;
    }

    String getStreamName() {
        return streamName;
    }

    String getMetaStreamSubject() {
        return metaStreamSubject;
    }

    String getChunkStreamSubject() {
        return chunkStreamSubject;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo put(ObjectMeta meta, InputStream inputStream) throws IOException, JetStreamApiException {
        validateNotNull(meta, "Object Meta");
        validateNotNullOrEmpty(meta.getName(), "Object Meta Name");

        String objectName = meta.getName();
        ObjectInfo originalInfo = getInfo(objectName);

        // each object gets a unique id
        String nuid = NUID.nextGlobal();

        // write all the blocks before writing meta data in case there is error
        String chunkSubject = toPublishChunkSubject(nuid);
        String metaSubject = toPublishMetaSubject(objectName);

        int chunkSize = meta.getObjectMetaOptions().getChunkSize();

        // start a builder for the info
        ObjectInfo.Builder infoBuilder = ObjectInfo.builder()
            .bucket(bucketName)
            .nuid(nuid)
            .meta(meta);

        // loop through the input stream reading a block at a time and writing it's message
        byte[] buffer = new byte[chunkSize];
        long size = 0;
        long chunks = 0;
        String digestValue = null;
        try {
            List<CompletableFuture<PublishAck>> pubAckFutures = new ArrayList<>();

            if (inputStream != null) {
                Digester digester;
                try {
                    digester = new Digester();
                } catch (NoSuchAlgorithmException nsae) {
                    // this is a system error in case the digest algorithm does not exist
                    throw new IOException(nsae);
                }
                int red = inputStream.read(buffer);
                while (red > 0) {
                    size += red;
                    chunks++;

                    // the payload is all bytes or red bytes depending
                    byte[] payload = Arrays.copyOfRange(buffer, 0, red);

                    // digest the actual bytes
                    digester.update(payload);

                    // publish the payload
                    pubAckFutures.add(_publishAsync(chunkSubject, payload, null));

                    // read more if we got a full read last time, otherwise, that's the last of the bytes
                    red = red == chunkSize ? inputStream.read(buffer) : -1;
                }
                digestValue = digester.getDigestValue();
            }

            // all the parts have been sent, publish the meta
            ObjectInfo info = infoBuilder
                .size(size)
                .chunks(chunks)
                .digest(digestValue)
                .build();
            pubAckFutures.add(_publishAsync(metaSubject, info.toJson(), META_HEADERS));

            // process all the pub acks
            while (pubAckFutures.size() > 0) {
                CompletableFuture<PublishAck> f = pubAckFutures.remove(0);
                if (f.isDone()) {
                    try {
                        f.get();
                    }
                    catch (Exception e) {
                        Throwable t = e;
                        if (e instanceof ExecutionException) { t = e.getCause(); }
                        if (t instanceof IOException) { throw (IOException) t; }
                        if (t instanceof JetStreamApiException) { throw (JetStreamApiException) t; }
                        throw new IOException(t);
                    }
                }
                else {
                    // wasn't "done" re queue it and try again later
                    pubAckFutures.add(f);
                }
            }

            // Delete any original chunks.
            if (originalInfo != null && !originalInfo.isDeleted()) {
                jsm.purgeStream(streamName, PurgeOptions.subject(toPlainChunkSubject(originalInfo.getNuid())));
            }
        }
        catch (Exception e) {
            // if there was an exception, the entire upload is not valid
            try {
                jsm.purgeStream(streamName, PurgeOptions.subject(toPlainChunkSubject(nuid)));
            }
            catch (Exception ignore) { /* ignore */ }

            if (e instanceof IOException) { throw (IOException) e; }
            if (e instanceof JetStreamApiException) { throw (JetStreamApiException) e; }
            throw new IOException(e);
        }

        return getInfo(objectName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo put(ObjectMeta meta, String input) throws IOException, JetStreamApiException {
        return put(meta, new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo put(ObjectMeta meta, byte[] input) throws IOException, JetStreamApiException {
        return put(meta, new ByteArrayInputStream(input));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo put(ObjectMeta meta, Object input) throws IOException, JetStreamApiException {
        if (input instanceof byte[]) {
            return put(meta, (byte[])input);
        }
        return put(meta, input.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void get(String objectName, OutputStream outputStream) throws IOException, JetStreamApiException, InterruptedException {

        ObjectInfo info = getInfo(objectName);
        if (info == null) {
            throw new IllegalArgumentException("Object " + objectName + " does not exist.");
        }

        ObjectLink link = info.getLink();
        if (link != null) {
            if (link.isBucketLink()) {
                throw new IllegalArgumentException("Object " + objectName + " is a link to bucket " + link.getBucket());
            }
            // link in the same bucket?
            if (bucketName.equals(link.getBucket())) {
                get(link.getName(), outputStream);
                return;
            }

            ObjectStore los = js.conn.objectStore(link.getBucket());
            los.get(link.getName(), outputStream);
        }

        visitSubject(toPlainChunkSubject(info.getNuid()), DeliverPolicy.All, false, true,
            m -> {
                outputStream.write(m.getData());
            });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getBytes(String objectName) throws IOException, JetStreamApiException, InterruptedException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        get(objectName, baos);
        return baos.toByteArray();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo getInfo(String objectName) throws IOException, JetStreamApiException {
        MessageInfo mi = _getLastMessage(streamName, toPlainMetaSubject(objectName));
        return mi == null ? null : new ObjectInfo(mi);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo updateMeta(String objectName, ObjectMeta meta) throws IOException, JetStreamApiException {
        validateNotNull(meta, "Object Meta");

        ObjectInfo info = getInfo(objectName);
        if (info == null) {
            throw new IllegalArgumentException("An object with the name " + objectName + " does not exist.");
        }

        // Update Meta prevents update of ObjectMetaOptions (Link, ChunkSize)
        // These should only be updated internally when appropriate.
        ObjectMeta newMeta = ObjectMeta.builder()
            .name(meta.getName())
            .description(meta.getDescription())
            .options(meta.getObjectMetaOptions())
            .build();

        ObjectInfo newInfo = ObjectInfo.builder(info).meta(newMeta).build();
        _publish(toPublishMetaSubject(objectName), newInfo.toJson(), META_HEADERS);

        // did the name of this object change? We just stored the meta under the new name
        // so delete the meta from the old name
        // purge the chunks
        if (!objectName.equals(meta.getName())) {
            PurgeOptions po = PurgeOptions.subject(toPlainMetaSubject(objectName));
            jsm.purgeStream(streamName, po);
        }

        return getInfo(objectName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo delete(String objectName) throws IOException, JetStreamApiException {
        ObjectInfo info = getInfo(objectName);
        if (info != null && !info.isDeleted()) {
            info = ObjectInfo.builder(info)
                .size(0)
                .chunks(0)
                .digest(null)
                .modified(ZonedDateTime.now())
                .deleted(true)
                .build();

            // update the meta
            _publish(toPublishMetaSubject(objectName), info.toJson(), META_HEADERS);

            // purge the chunks
            PurgeOptions po = PurgeOptions.subject(toPlainChunkSubject(info.getNuid()));
            jsm.purgeStream(streamName, po);
        }
        return info;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo addLink(String objectName, ObjectInfo toInfo) throws IOException, JetStreamApiException {
        validateNotNull(toInfo, "Object Info Required");
        if (toInfo.isDeleted()) {
            throw new IllegalArgumentException("Link to object is deleted.");
        }

        ObjectMeta meta = ObjectMeta.builder()
            .name(objectName)
            .link(ObjectLink.builder().bucket(toInfo.getBucket()).name(toInfo.getName()).build())
            .build();

        return put(meta, (InputStream) null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInfo addBucketLink(String objectName, ObjectStore toStore) throws IOException, JetStreamApiException {
        validateNotNull(toStore, "Object Store Required");

        ObjectMeta meta = ObjectMeta.builder()
            .name(objectName)
            .link(ObjectLink.builder().bucket(toStore.getBucketName()).build())
            .build();

        return put(meta, (InputStream) null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seal() throws IOException, JetStreamApiException {
        StreamInfo si = jsm.getStreamInfo(streamName);
        StreamConfiguration sc = StreamConfiguration.builder(si.getConfiguration()).sealed(true).build();
        jsm.updateStream(sc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectStoreStatus getStatus() throws IOException, JetStreamApiException, InterruptedException {
        return new ObjectStoreStatus(jsm.getStreamInfo(streamName));
    }
}
