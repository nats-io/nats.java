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
package io.nats.client.api;

import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import io.nats.client.support.NatsKeyValueUtil;

import java.time.Duration;
import java.util.*;

import static io.nats.client.support.NatsJetStreamConstants.SERVER_DEFAULT_DUPLICATE_WINDOW_MS;
import static io.nats.client.support.NatsKeyValueUtil.*;
import static io.nats.client.support.Validator.*;

/**
 * The KeyValueConfiguration class contains the configuration for of a Key Value bucket.
 */
public class KeyValueConfiguration extends FeatureConfiguration {
    KeyValueConfiguration(StreamConfiguration sc) {
        super(sc, extractBucketName(sc.getName()));
    }

    /**
     * Gets the maximum number of history for any one key. Includes the current value.
     * @return the maximum number of values for any one key.
     */
    public long getMaxHistoryPerKey() {
        return sc.getMaxMsgsPerSubject();
    }

    /**
     * Gets the maximum size for an individual value in the bucket.
     * @deprecated the server value is a 32-bit signed value. Use {@link #getMaximumValueSize()} instead.
     * @return the maximum size for a value.
     */
    @Deprecated
    public long getMaxValueSize() {
        return sc.getMaximumMessageSize();
    }

    /**
     * Gets the maximum size for an individual value in the bucket.
     * @return the maximum size for a value.
     */
    public int getMaximumValueSize() {
        return sc.getMaximumMessageSize();
    }

    /**
     * Get the republish configuration. Might be null.
     * @return the republish object
     */
    public Republish getRepublish() {
        return sc.getRepublish();
    }

    /**
     * The mirror definition for this configuration
     * @return the mirror
     */
    public Mirror getMirror() {
        return sc.getMirror();
    }

    /**
     * The sources for this configuration
     * @return the sources
     */
    public List<Source> getSources() {
        return sc.getSources();
    }

    /**
     * The limit marker ttl if set
     * @return the duration
     */
    public Duration getLimitMarkerTtl() {
        return sc.getSubjectDeleteMarkerTtl();
    }

    @Override
    public String toString() {
        return "KeyValueConfiguration" + toJson();
    }

    @Override
    public JsonValue toJsonValue() {
        JsonValueUtils.MapBuilder mb = new JsonValueUtils.MapBuilder(super.toJsonValue());
        mb.jv.mapOrder.remove("metaData");
        mb.put("maxHistoryPerKey", getMaxHistoryPerKey());
        mb.put("maxValueSize", getMaxValueSize());
        mb.put("republish", getRepublish());
        mb.put("mirror", getMirror());
        mb.put("sources", getSources());
        mb.put("limitMarkerTtl", getLimitMarkerTtl());
        mb.jv.mapOrder.add("metaData");
        return mb.toJsonValue();
    }

    /**
     * Creates a builder for the Key Value Configuration.
     * @return a KeyValueConfiguration Builder
     */
    public static Builder builder() {
        return new Builder((KeyValueConfiguration)null);
    }

    /**
     * Creates a builder for the Key Value Configuration.
     * @param name the name of the key value bucket
     * @return a KeyValueConfiguration Builder
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    /**
     * Creates a builder to copy the key value configuration.
     * @param kvc an existing KeyValueConfiguration
     * @return a KeyValueConfiguration Builder
     */
    public static Builder builder(KeyValueConfiguration kvc) {
        return new Builder(kvc);
    }

    /**
     * KeyValueConfiguration is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     *
     * <p>{@code new Builder().build()} will create a new KeyValueConfiguration.
     *
     */
    public static class Builder
        extends FeatureConfiguration.Builder<Builder, KeyValueConfiguration>
    {
        Mirror mirror;
        Duration limitMarkerTtl;
        final List<Source> sources = new ArrayList<>();

        @Override
        protected Builder getThis() {
            return this;
        }

        /**
         * Default Builder
         */
        public Builder() {
            this((KeyValueConfiguration)null);
        }

        /**
         * Builder accepting the key value bucket name.
         * @param name name of the key value bucket.
         */
        public Builder(String name) {
            this((KeyValueConfiguration)null);
            name(name);
        }

        /**
         * Construct the builder by copying another configuration
         * @param kvc the configuration to copy
         */
        public Builder(KeyValueConfiguration kvc) {
            if (kvc == null) {
                scBuilder = new StreamConfiguration.Builder();
                maxHistoryPerKey(1);
                replicas(1);
            }
            else {
                scBuilder = new StreamConfiguration.Builder(kvc.sc);
                name = NatsKeyValueUtil.extractBucketName(kvc.sc.getName());
            }
        }

        /**
         * Sets the name of the key value bucket.
         * @param name name of the key value bucket.
         * @return the builder
         */
        @Override
        public Builder name(String name) {
            return super.name(name);
        }

        /**
         * Sets the description of the store.
         * @param description description of the store.
         * @return the builder
         */
        @Override
        public Builder description(String description) {
            return super.description(description);
        }

        /**
         * Sets the maximum number of history for any one key. Includes the current value.
         * @param maxHistoryPerKey the maximum history
         * @return Builder
         */
        public Builder maxHistoryPerKey(int maxHistoryPerKey) {
            scBuilder.maxMessagesPerSubject(validateMaxHistory(maxHistoryPerKey));
            return this;
        }

        /**
         * Sets the maximum number of bytes in the KeyValueConfiguration.
         * @param maxBucketSize the maximum number of bytes
         * @return Builder
         */
        @Override
        public Builder maxBucketSize(long maxBucketSize) {
            return super.maxBucketSize(maxBucketSize);
        }

        /**
         * Sets the maximum size for an individual value in the KeyValueConfiguration.
         * @deprecated the server value is a 32-bit signed value. Use {@link #maximumValueSize(int)} instead.
         * @param maxValueSize the maximum size for a value
         * @return Builder
         */
        @Deprecated
        public Builder maxValueSize(long maxValueSize) {
            scBuilder.maximumMessageSize((int)validateMaxValueSize(maxValueSize));
            return this;
        }

        /**
         * Sets the maximum size for an individual value in the KeyValueConfiguration.
         * @param maxValueSize the maximum size for a value
         * @return Builder
         */
        public Builder maximumValueSize(int maxValueSize) {
            scBuilder.maximumMessageSize((int)validateMaxValueSize(maxValueSize));
            return this;
        }

        /**
         * Sets the maximum age for a value in this KeyValueConfiguration.
         * @param ttl the maximum age
         * @return Builder
         */
        @Override
        public Builder ttl(Duration ttl) {
            return super.ttl(ttl);
        }

        /**
         * Sets the storage type in the KeyValueConfiguration.
         * @param storageType the storage type
         * @return Builder
         */
        @Override
        public Builder storageType(StorageType storageType) {
            return super.storageType(storageType);
        }

        /**
         * Sets the number of replicas a message must be stored on in the KeyValueConfiguration.
         * @param replicas the number of replicas
         * @return Builder
         */
        @Override
        public Builder replicas(int replicas) {
            return super.replicas(replicas);
        }

        /**
         * Sets the placement directive object
         * @param placement the placement directive object
         * @return Builder
         */
        @Override
        public Builder placement(Placement placement) {
            return super.placement(placement);
        }

        /**
         * Sets whether to use compression for the KeyValueConfiguration.
         * If set, will use the default compression algorithm of the KV backing store.
         * @param compression whether to use compression in the KeyValueConfiguration
         * @return Builder
         */
        @Override
        public Builder compression(boolean compression) {
            return super.compression(compression);
        }

        /**
         * Sets the metadata for the KeyValueConfiguration
         * @param metadata the metadata map
         * @return Builder
         */
        @Override
        public Builder metadata(Map<String, String> metadata) {
            return super.metadata(metadata);
        }

        /**
         * Sets the Republish options
         * @param republish the Republish object
         * @return Builder
         */
        public Builder republish(Republish republish) {
            scBuilder.republish(republish);
            return this;
        }

        /**
         * Sets the mirror in the KeyValueConfiguration.
         * @param mirror the KeyValue's mirror
         * @return Builder
         */
        public Builder mirror(Mirror mirror) {
            this.mirror = mirror;
            return this;
        }

        /**
         * Sets the sources in the KeyValueConfiguration.
         * @param sources the KeyValue's sources
         * @return Builder
         */
        public Builder sources(Source... sources) {
            this.sources.clear();
            return addSources(sources);
        }

        /**
         * Sets the sources in the KeyValueConfiguration
         * @param sources the KeyValue's sources
         * @return Builder
         */
        public Builder sources(Collection<Source> sources) {
            this.sources.clear();
            return addSources(sources);
        }

        /**
         * Add a source into the KeyValueConfiguration.
         * @param source a KeyValue source
         * @return Builder
         */
        public Builder addSource(Source source) {
            if (source != null && !this.sources.contains(source)) {
                this.sources.add(source);
            }
            return this;
        }

        /**
         * Adds the sources into the KeyValueConfiguration
         * @param sources the KeyValue's sources to add
         * @return Builder
         */
        public Builder addSources(Source... sources) {
            if (sources != null) {
                return addSources(Arrays.asList(sources));
            }
            return this;
        }

        /**
         * Adds the sources into the KeyValueConfiguration
         * @param sources the KeyValue's sources to add
         * @return Builder
         */
        public Builder addSources(Collection<Source> sources) {
            if (sources != null) {
                for (Source source : sources) {
                    if (source != null && !this.sources.contains(source)) {
                        this.sources.add(source);
                    }
                }
            }
            return this;
        }

        /**
         * The limit marker TTL duration. Server accepts 1 second or more.
         * Null or empty has the effect of clearing the limit marker ttl
         * @param limitMarkerTtl the TTL duration
         * @return The Builder
         */
        public Builder limitMarker(Duration limitMarkerTtl) {
            this.limitMarkerTtl = validateDurationNotRequiredGtOrEqSeconds(1, limitMarkerTtl, null, "Limit Marker Ttl");
            return this;
        }

        /**
         * The limit marker TTL duration in milliseconds. Server accepts 1 second or more.
         * 0 or less has the effect of clearing the limit marker ttl
         * @param limitMarkerTtlMillis the TTL duration
         * @return The Builder
         */
        public Builder limitMarker(long limitMarkerTtlMillis) {
            if (limitMarkerTtlMillis <= 0) {
                this.limitMarkerTtl = null;
            }
            else {
                this.limitMarkerTtl = validateDurationGtOrEqSeconds(1, limitMarkerTtlMillis, "Limit Marker Ttl");
            }
            return this;
        }

        /**
         * Builds the KeyValueConfiguration
         * @return the KeyValueConfiguration.
         */
        public KeyValueConfiguration build() {
            name = required(name, "name");
            scBuilder.name(toStreamName(name))
                .allowRollup(true)
                .allowDirect(true) // by design
                .discardPolicy(DiscardPolicy.New)
                .denyDelete(true);

            if (mirror != null) {
                scBuilder.mirrorDirect(true);
                String name = mirror.getName();
                if (hasPrefix(name)) {
                    scBuilder.mirror(mirror);
                }
                else {
                    scBuilder.mirror(
                        Mirror.builder(mirror)
                            .name(toStreamName(name))
                            .build());
                }
            }
            else if (!sources.isEmpty()) {
                for (Source source : sources) {
                    String name = source.getName();
                    if (hasPrefix(name)) {
                        scBuilder.addSource(source);
                    }
                    else {
                        scBuilder.addSource(
                            Source.builder(source)
                                .name(toStreamName(name))
                                .build());
                    }
                }
            }
            else {
                scBuilder.subjects(toStreamSubject(name));
            }

            if (limitMarkerTtl != null) {
                scBuilder.subjectDeleteMarkerTtl(limitMarkerTtl).allowMessageTtl();
            }

            // When stream's MaxAge is not set, server uses 2 minutes as the default
            // for the duplicate window. If MaxAge is set, and lower than 2 minutes,
            // then the duplicate window will be set to that. If MaxAge is greater,
            // we will cap the duplicate window to 2 minutes (to be consistent with
            // previous behavior).
            long ttlMs = ttl.toMillis();
            long dupeMs = SERVER_DEFAULT_DUPLICATE_WINDOW_MS;
            if (ttlMs > 0 && ttlMs < SERVER_DEFAULT_DUPLICATE_WINDOW_MS) {
                dupeMs = ttlMs;
            }
            scBuilder.duplicateWindow(dupeMs);

            return new KeyValueConfiguration(scBuilder.build());
        }
    }
}
