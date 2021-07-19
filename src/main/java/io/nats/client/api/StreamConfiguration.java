// Copyright 2020 The NATS Authors
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

import io.nats.client.support.JsonSerializable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.Validator.*;

/**
 * The StreamConfiguration class specifies the configuration for creating a JetStream stream on the server.
 * Options are created using a {@link StreamConfiguration.Builder Builder}.
 */
public class StreamConfiguration implements JsonSerializable {

    // see builder for defaults
    private final String name;
    private final List<String> subjects;
    private final RetentionPolicy retentionPolicy;
    private final long maxConsumers;
    private final long maxMsgs;
    private final long maxBytes;
    private final Duration maxAge;
    private final long maxMsgSize;
    private final StorageType storageType;
    private final int replicas;
    private final boolean noAck;
    private final String templateOwner;
    private final DiscardPolicy discardPolicy;
    private final Duration duplicateWindow;
    private final Placement placement;
    private final Mirror mirror;
    private final List<Source> sources;

    // for the response from the server
    static StreamConfiguration instance(String json) {
        Builder builder = new Builder();

        Matcher m = RETENTION_RE.matcher(json);
        if (m.find()) {
            builder.retentionPolicy(RetentionPolicy.get(m.group(1)));
        }

        m = STORAGE_TYPE_RE.matcher(json);
        if (m.find()) {
            builder.storageType(StorageType.get(m.group(1)));
        }

        m = DISCARD_RE.matcher(json);
        if (m.find()) {
            builder.discardPolicy(DiscardPolicy.get(m.group(1)));
        }

        builder.name(readString(json, NAME_RE));
        readLong(json, MAX_CONSUMERS_RE, builder::maxConsumers);
        readLong(json, MAX_MSGS_RE, builder::maxMessages);
        readLong(json, MAX_BYTES_RE, builder::maxBytes);
        readNanos(json, MAX_AGE_RE, builder::maxAge);
        readLong(json, MAX_MSG_SIZE_RE, builder::maxMsgSize);
        readInt(json, NUM_REPLICAS_RE, builder::replicas);
        builder.noAck(readBoolean(json, NO_ACK_RE));
        builder.templateOwner(readString(json, TEMPLATE_OWNER_RE));
        readNanos(json, DUPLICATE_WINDOW_RE, builder::duplicateWindow);
        builder.subjects(getStringList(SUBJECTS, json));
        builder.placement(Placement.optionalInstance(json));
        builder.mirror(Mirror.optionalInstance(json));
        builder.sources(Source.optionalListOf(json));

        return builder.build();
    }

    // For the builder, assumes all validations are already done in builder
    StreamConfiguration(
            String name, List<String> subjects, RetentionPolicy retentionPolicy,
            long maxConsumers, long maxMsgs, long maxBytes,
            Duration maxAge, long maxMsgSize, StorageType storageType,
            int replicas, boolean noAck, String templateOwner,
            DiscardPolicy discardPolicy, Duration duplicateWindow,
            Placement placement, Mirror mirror, List<Source> sources)
    {
        this.name = name;
        this.subjects = subjects;
        this.retentionPolicy = retentionPolicy;
        this.maxConsumers = maxConsumers;
        this.maxMsgs = maxMsgs;
        this.maxBytes = maxBytes;
        this.maxAge = maxAge;
        this.maxMsgSize = maxMsgSize;
        this.storageType = storageType;
        this.replicas = replicas;
        this.noAck = noAck;
        this.templateOwner = templateOwner;
        this.discardPolicy = discardPolicy;
        this.duplicateWindow = duplicateWindow;
        this.placement = placement;
        this.mirror = mirror;
        this.sources = sources;
    }

    /**
     * Returns a JSON representation of this consumer configuration.
     * 
     * @return json consumer configuration to send to the server.
     */
    public String toJson() {

        StringBuilder sb = beginJson();

        addField(sb, NAME, name);
        addStrings(sb, SUBJECTS, subjects);
        addField(sb, RETENTION, retentionPolicy.toString());
        addField(sb, MAX_CONSUMERS, maxConsumers);
        addField(sb, MAX_MSGS, maxMsgs);
        addField(sb, MAX_BYTES, maxBytes);
        addFieldAsNanos(sb, MAX_AGE, maxAge);
        addField(sb, MAX_MSG_SIZE, maxMsgSize);
        addField(sb, STORAGE, storageType.toString());
        addField(sb, NUM_REPLICAS, replicas);
        addField(sb, NO_ACK, noAck);
        addField(sb, TEMPLATE_OWNER, templateOwner);
        addField(sb, DISCARD, discardPolicy.toString());
        addFieldAsNanos(sb, DUPLICATE_WINDOW, duplicateWindow);
        if (placement != null) {
            addField(sb, PLACEMENT, placement);
        }
        if (mirror != null) {
            addField(sb, MIRROR, mirror);
        }
        addJsons(sb, SOURCES, sources);

        return endJson(sb).toString();
    }

    /**
     * Gets the name of this stream configuration.
     * @return the name of the stream.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the subjects for this stream configuration.
     * @return the subject of the stream.
     */
    public List<String> getSubjects() {
        return subjects;
    }

    /**
     * Gets the discard policy for this stream configuration.
     * @return the discard policy of the stream.
     */
    public DiscardPolicy getDiscardPolicy() {
        return discardPolicy;
    }

    /**
     * Gets the retention policy for this stream configuration.
     * @return the retention policy for this stream.
     */
    public RetentionPolicy getRetentionPolicy() {
        return retentionPolicy;
    }

    /**
     * Gets the maximum number of consumers for this stream configuration.
     * @return the maximum number of consumers for this stream.
     */
    public long getMaxConsumers() {
        return maxConsumers;
    }

    /**
     * Gets the maximum messages for this stream configuration.
     * @return the maximum number of messages for this stream.
     */
    public long getMaxMsgs() {
        return maxMsgs;
    }

    /**
     * Gets the maximum number of bytes for this stream configuration.
     * @return the maximum number of bytes for this stream.
     */    
    public long getMaxBytes() {
        return maxBytes;
    }

    /**
     * Gets the maximum message age for this stream configuration.
     * @return the maximum message age for this stream.
     */  
    public Duration getMaxAge() {
        return maxAge;
    }

    /**
     * Gets the maximum message size for this stream configuration.
     * @return the maximum message size for this stream.
     */      
    public long getMaxMsgSize() {
        return maxMsgSize;
    }

    /**
     * Gets the storate type for this stream configuration.
     * @return the storage type for this stream.
     */
    public StorageType getStorageType() {
        return storageType;
    }

    /**
     * Gets the number of replicas for this stream configuration.
     * @return the number of replicas
     */    
    public int getReplicas() {
        return replicas;
    }

    /**
     * Gets whether or not acknowledgements are required in this stream configuration.
     * @return true if acknowedgments are not required.
     */
    public boolean getNoAck() {
        return noAck;
    }

    /**
     * Gets the template json for this stream configuration.
     * @return the template for this stream.
     */    
    public String getTemplateOwner() {
        return templateOwner;
    }

    /**
     * Gets the duplicate checking window stream configuration.  Duration.ZERO
     * means duplicate checking is not enabled.
     * @return the duration of the window.
     */    
    public Duration getDuplicateWindow() {
        return duplicateWindow;
    }

    /**
     * Placement directives to consider when placing replicas of this stream,
     * random placement when unset
     * @return the placement [directive object]
     */
    public Placement getPlacement() {
        return placement;
    }

    /**
     * The mirror definition for this stream
     * @return the mirror
     */
    public Mirror getMirror() {
        return mirror;
    }

    /**
     * The sources for this stream
     * @return the sources
     */
    public List<Source> getSources() {
        return sources;
    }

    @Override
    public String toString() {
        return "StreamConfiguration{" +
                "name='" + name + '\'' +
                ", subjects=" + subjects +
                ", retentionPolicy=" + retentionPolicy +
                ", maxConsumers=" + maxConsumers +
                ", maxMsgs=" + maxMsgs +
                ", maxBytes=" + maxBytes +
                ", maxAge=" + maxAge +
                ", maxMsgSize=" + maxMsgSize +
                ", storageType=" + storageType +
                ", replicas=" + replicas +
                ", noAck=" + noAck +
                ", template='" + templateOwner + '\'' +
                ", discardPolicy=" + discardPolicy +
                ", duplicateWindow=" + duplicateWindow +
                ", " + objectString("mirror", mirror) +
                ", " + objectString("placement", placement) +
                ", sources=" + sources +
                '}';
    }

    /**
     * Creates a builder for the stream configuration.
     * @return a stream configuration builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder to copy the stream configuration.
     * @param sc an existing StreamConfiguration
     * @return a stream configuration builder
     */
    public static Builder builder(StreamConfiguration sc) {
        return new Builder(sc);
    }

    /**
     * StreamConfiguration is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     * 
     * <p>{@code new StreamConfiguration.Builder().build()} will create a new ConsumerConfiguration.
     * 
     */
    public static class Builder {

        private String name = null;
        private final List<String> subjects = new ArrayList<>();
        private RetentionPolicy retentionPolicy = RetentionPolicy.Limits;
        private long maxConsumers = -1;
        private long maxMsgs = -1;
        private long maxBytes = -1;
        private Duration maxAge = Duration.ZERO;
        private long maxMsgSize = -1;
        private StorageType storageType = StorageType.File;
        private int replicas = 1;
        private boolean noAck = false;
        private String templateOwner = null;
        private DiscardPolicy discardPolicy = DiscardPolicy.Old;
        private Duration duplicateWindow = Duration.ZERO;
        private Placement placement = null;
        private Mirror mirror = null;
        private final List<Source> sources = new ArrayList<>();

        /**
         * Default Builder
         */
        public Builder() {}

        /**
         * Update Builder, useful if you need to update a configuration
         * @param sc the configuration to copy
         */
        public Builder(StreamConfiguration sc) {
            if (sc != null) {
                this.name = sc.name;
                subjects(sc.subjects);
                this.retentionPolicy = sc.retentionPolicy;
                this.maxConsumers = sc.maxConsumers;
                this.maxMsgs = sc.maxMsgs;
                this.maxBytes = sc.maxBytes;
                this.maxAge = sc.maxAge;
                this.maxMsgSize = sc.maxMsgSize;
                this.storageType = sc.storageType;
                this.replicas = sc.replicas;
                this.noAck = sc.noAck;
                this.templateOwner = sc.templateOwner;
                this.discardPolicy = sc.discardPolicy;
                this.duplicateWindow = sc.duplicateWindow;
                this.placement = sc.placement;
                this.mirror = sc.mirror;
                sources(sc.sources);
            }
        }

        /**
         * Sets the name of the stream.
         * @param name name of the stream.
         * @return the builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the subjects in the StreamConfiguration.
         * @param subjects the stream's subjects
         * @return Builder
         */
        public Builder subjects(String... subjects) {
            this.subjects.clear();
            return addSubjects(subjects);
        }

        /**
         * Sets the subjects in the StreamConfiguration.
         * @param subjects the stream's subjects
         * @return Builder
         */
        public Builder subjects(Collection<String> subjects) {
            this.subjects.clear();
            return addSubjects(subjects);
        }

        /**
         * Sets the subjects in the StreamConfiguration.
         * @param subjects the stream's subjects
         * @return Builder
         */
        public Builder addSubjects(String... subjects) {
            if (subjects != null) {
                return addSubjects(Arrays.asList(subjects));
            }
            return this;
        }

        /**
         * Sets the subjects in the StreamConfiguration.
         * @param subjects the stream's subjects
         * @return Builder
         */
        public Builder addSubjects(Collection<String> subjects) {
            if (subjects != null) {
                for (String sub : subjects) {
                    if (sub != null && !this.subjects.contains(sub)) {
                        this.subjects.add(sub);
                    }
                }
            }
            return this;
        }

        /**
         * Sets the retention policy in the StreamConfiguration.
         * @param policy the retention policy of the StreamConfiguration
         * @return Builder
         */
        public Builder retentionPolicy(RetentionPolicy policy) {
            this.retentionPolicy = policy == null ? RetentionPolicy.Limits : policy;
            return this;
        }

        /**
         * Sets the maximum number of consumers in the StreamConfiguration.
         * @param maxConsumers the maximum number of consumers
         * @return Builder
         */        
        public Builder maxConsumers(long maxConsumers) {
            this.maxConsumers = validateMaxConsumers(maxConsumers);
            return this;
        }

        /**
         * Sets the maximum number of consumers in the StreamConfiguration.
         * @param maxMsgs the maximum number of messages
         * @return Builder
         */
        public Builder maxMessages(long maxMsgs) {
            this.maxMsgs = validateMaxMessages(maxMsgs);
            return this;
        }

        /**
         * Sets the maximum number of bytes in the StreamConfiguration.
         * @param maxBytes the maximum number of bytes
         * @return Builder
         */        
        public Builder maxBytes(long maxBytes) {
            this.maxBytes = validateMaxBytes(maxBytes);
            return this;
        }

        /**
         * Sets the maximum age in the StreamConfiguration.
         * @param maxAge the maximum message age
         * @return Builder
         */
        public Builder maxAge(Duration maxAge) {
            this.maxAge = validateDurationNotRequiredGtOrEqZero(maxAge);
            return this;
        }

        /**
         * Sets the maximum age in the StreamConfiguration.
         * @param maxAgeMillis the maximum message age
         * @return Builder
         */
        public Builder maxAge(long maxAgeMillis) {
            this.maxAge = validateDurationNotRequiredGtOrEqZero(maxAgeMillis);
            return this;
        }

        /**
         * Sets the maximum message size in the StreamConfiguration.
         * @param maxMsgSize the maximum message size
         * @return Builder
         */
        public Builder maxMsgSize(long maxMsgSize) {
            this.maxMsgSize = validateMaxMessageSize(maxMsgSize);
            return this;
        }

        /**
         * Sets the storage type in the StreamConfiguration.
         * @param storageType the storage type
         * @return Builder
         */        
        public Builder storageType(StorageType storageType) {
            this.storageType = storageType == null ? StorageType.File : storageType;
            return this;
        }

        /**
         * Sets the number of replicas a message must be stored on in the StreamConfiguration.
         * @param replicas the number of replicas to store this message on
         * @return Builder
         */
        public Builder replicas(int replicas) {
            this.replicas = validateNumberOfReplicas(replicas);
            return this;
        }

        /**
         * Sets the acknowledgement mode of the StreamConfiguration.  if no acknowledgements are
         * set, then acknowledgements are not sent back to the client.  The default is false.
         * @param noAck true to disable acknowledgements.
         * @return Builder
         */        
        public Builder noAck(boolean noAck) {
            this.noAck = noAck;
            return this;
        }

        /**
         * Sets the template a stream in the form of raw JSON.
         * @param templateOwner the stream template of the stream.
         * @return the builder
         */
        public Builder templateOwner(String templateOwner) {
            this.templateOwner = emptyAsNull(templateOwner);
            return this;
        }

        /**
         * Sets the discard policy in the StreamConfiguration.
         * @param policy the discard policy of the StreamConfiguration
         * @return Builder
         */
        public Builder discardPolicy(DiscardPolicy policy) {
            this.discardPolicy = policy == null ? DiscardPolicy.Old : policy;
            return this;
        }

        /**
         * Sets the duplicate checking window in the the StreamConfiguration.  A Duration.Zero
         * disables duplicate checking.  Duplicate checking is disabled by default.
         * @param window duration to hold message ids for duplicate checking.
         * @return Builder
         */
        public Builder duplicateWindow(Duration window) {
            this.duplicateWindow = validateDurationNotRequiredGtOrEqZero(window);
            return this;
        }

        /**
         * Sets the duplicate checking window in the the StreamConfiguration.  A Duration.Zero
         * disables duplicate checking.  Duplicate checking is disabled by default.
         * @param windowMillis duration to hold message ids for duplicate checking.
         * @return Builder
         */
        public Builder duplicateWindow(long windowMillis) {
            this.duplicateWindow = validateDurationNotRequiredGtOrEqZero(windowMillis);
            return this;
        }

        /**
         * Sets the placement directive object
         * @param placement the placement directive object
         * @return Builder
         */
        public Builder placement(Placement placement) {
            this.placement = placement;
            return this;
        }

        /**
         * Sets the mirror  object
         * @param mirror the mirror object
         * @return Builder
         */
        public Builder mirror(Mirror mirror) {
            this.mirror = mirror;
            return this;
        }

        /**
         * Sets the sources in the StreamConfiguration.
         * @param sources the stream's sources
         * @return Builder
         */
        public Builder sources(Source... sources) {
            this.sources.clear();
            return addSources(sources);
        }

        /**
         * Sets the sources in the StreamConfiguration.
         * @param sources the stream's sources
         * @return Builder
         */
        public Builder sources(Collection<Source> sources) {
            this.sources.clear();
            return addSources(sources);
        }

        /**
         * Sets the sources in the StreamConfiguration.
         * @param sources the stream's sources
         * @return Builder
         */
        public Builder addSources(Source... sources) {
            return addSources(Arrays.asList(sources));
        }

        /**
         * Sets the sources in the StreamConfiguration.
         * @param sources the stream's sources
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
         * Builds the ConsumerConfiguration
         * @return a consumer configuration.
         */
        public StreamConfiguration build() {
            return new StreamConfiguration(
                    name,
                    subjects,
                    retentionPolicy,
                    maxConsumers,
                    maxMsgs,
                    maxBytes,
                    maxAge,
                    maxMsgSize,
                    storageType,
                    replicas,
                    noAck,
                    templateOwner,
                    discardPolicy,
                    duplicateWindow,
                    placement,
                    mirror,
                    sources
            );
        }

    }
}
