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

import io.nats.client.support.NatsKeyValueUtil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static io.nats.client.support.NatsKeyValueUtil.*;
import static io.nats.client.support.Validator.*;

/**
 * The KeyValueConfiguration class contains the configuration for of a Key Value bucket.
 */
public class KeyValueConfiguration extends FeatureConfiguration {
    static KeyValueConfiguration instance(String json) {
        return new KeyValueConfiguration(StreamConfiguration.instance(json));
    }

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
     * Gets the maximum number of bytes for this bucket.
     * @return the maximum number of bytes for this bucket.
     */
    public long getMaxBucketSize() {
        return sc.getMaxBytes();
    }

    /**
     * Gets the maximum size for an individual value in the bucket.
     * @return the maximum size for a value.
     */      
    public long getMaxValueSize() {
        return sc.getMaxMsgSize();
    }

    @Override
    public String toString() {
        return "KeyValueConfiguration{" +
            "name='" + bucketName + '\'' +
            ", description='" + getDescription() + '\'' +
            ", maxHistoryPerKey=" + getMaxHistoryPerKey() +
            ", maxBucketSize=" + getMaxBucketSize() +
            ", maxValueSize=" + getMaxValueSize() +
            ", ttl=" + getTtl() +
            ", storageType=" + getStorageType() +
            ", replicas=" + getReplicas() +
            ", placement=" + getPlacement() +
            ", republish=" + getRepublish() +
            '}';
    }

    /**
     * Get the republish configuration. Might be null.
     * @return the republish object
     */
    public Republish getRepublish() {
        return sc.getRepublish();
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
    public static class Builder {
        String name;
        Mirror mirror;
        final List<Source> sources = new ArrayList<>();
        final StreamConfiguration.Builder scBuilder;

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
        public Builder name(String name) {
            this.name = validateBucketName(name, true);
            return this;
        }

        /**
         * Sets the description of the store.
         * @param description description of the store.
         * @return the builder
         */
        public Builder description(String description) {
            scBuilder.description(description);
            return this;
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
        public Builder maxBucketSize(long maxBucketSize) {
            scBuilder.maxBytes(validateMaxBucketBytes(maxBucketSize));
            return this;
        }

        /**
         * Sets the maximum size for an individual value in the KeyValueConfiguration.
         * @param maxValueSize the maximum size for a value
         * @return Builder
         */
        public Builder maxValueSize(long maxValueSize) {
            scBuilder.maxMsgSize(validateMaxValueSize(maxValueSize));
            return this;
        }

        /**
         * Sets the maximum age for a value in this KeyValueConfiguration.
         * @param ttl the maximum age
         * @return Builder
         */
        public Builder ttl(Duration ttl) {
            scBuilder.maxAge(ttl);
            return this;
        }

        /**
         * Sets the storage type in the KeyValueConfiguration.
         * @param storageType the storage type
         * @return Builder
         */
        public Builder storageType(StorageType storageType) {
            scBuilder.storageType(storageType);
            return this;
        }

        /**
         * Sets the number of replicas a message must be stored on in the KeyValueConfiguration.
         * @param replicas the number of replicas
         * @return Builder
         */
        public Builder replicas(int replicas) {
            scBuilder.replicas(replicas);
            return this;
        }

        /**
         * Sets the placement directive object
         * @param placement the placement directive object
         * @return Builder
         */
        public Builder placement(Placement placement) {
            scBuilder.placement(placement);
            return this;
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
            else if (sources.size() > 0) {
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

            return new KeyValueConfiguration(scBuilder.build());
        }
    }
}
