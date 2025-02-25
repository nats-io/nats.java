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

import io.nats.client.support.*;

import java.time.Duration;
import java.util.*;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.JsonValueUtils.readBoolean;
import static io.nats.client.support.JsonValueUtils.readInteger;
import static io.nats.client.support.JsonValueUtils.readLong;
import static io.nats.client.support.JsonValueUtils.readNanos;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.JsonValueUtils.*;
import static io.nats.client.support.Validator.*;

/**
 * The StreamConfiguration class specifies the configuration for creating a JetStream stream on the server.
 * Options are created using a {@link StreamConfiguration.Builder Builder}.
 */
public class StreamConfiguration implements JsonSerializable {

    // see builder for defaults
    private final String name;
    private final String description;
    private final List<String> subjects;
    private final RetentionPolicy retentionPolicy;
    private final CompressionOption compressionOption;
    private final long maxConsumers;
    private final long maxMsgs;
    private final long maxMsgsPerSubject;
    private final long maxBytes;
    private final Duration maxAge;
    private final int maxMsgSize;
    private final StorageType storageType;
    private final int replicas;
    private final boolean noAck;
    private final String templateOwner;
    private final DiscardPolicy discardPolicy;
    private final Duration duplicateWindow;
    private final Placement placement;
    private final Republish republish;
    private final SubjectTransform subjectTransform;
    private final ConsumerLimits consumerLimits;
    private final Mirror mirror;
    private final List<Source> sources;
    private final boolean sealed;
    private final boolean allowRollup;
    private final boolean allowDirect;
    private final boolean mirrorDirect;
    private final boolean denyDelete;
    private final boolean denyPurge;
    private final boolean discardNewPerSubject;
    private final Map<String, String> metadata;
    private final long firstSequence;
    private final boolean allowMessageTtl;
    private final Duration subjectDeleteMarkerTtl;

    static StreamConfiguration instance(JsonValue v) {
        Builder builder = new Builder();
        builder.retentionPolicy(RetentionPolicy.get(readString(v, RETENTION)));
        builder.compressionOption(CompressionOption.get(readString(v, COMPRESSION)));
        builder.storageType(StorageType.get(readString(v, STORAGE)));
        builder.discardPolicy(DiscardPolicy.get(readString(v, DISCARD)));
        builder.name(readString(v, NAME));
        builder.description(readString(v, DESCRIPTION));
        builder.maxConsumers(readLong(v, MAX_CONSUMERS, -1));
        builder.maxMessages(readLong(v, MAX_MSGS, -1));
        builder.maxMessagesPerSubject(readLong(v, MAX_MSGS_PER_SUB, -1));
        builder.maxBytes(readLong(v, MAX_BYTES, -1));
        builder.maxAge(readNanos(v, MAX_AGE));
        builder.maximumMessageSize(readInteger(v, MAX_MSG_SIZE, -1));
        builder.replicas(readInteger(v, NUM_REPLICAS, 1));
        builder.noAck(readBoolean(v, NO_ACK));
        builder.templateOwner(readString(v, TEMPLATE_OWNER));
        builder.duplicateWindow(readNanos(v, DUPLICATE_WINDOW));
        builder.subjects(readStringList(v, SUBJECTS));
        builder.placement(Placement.optionalInstance(readValue(v, PLACEMENT)));
        builder.republish(Republish.optionalInstance(readValue(v, REPUBLISH)));
        builder.subjectTransform(SubjectTransform.optionalInstance(readValue(v, SUBJECT_TRANSFORM)));
        builder.consumerLimits(ConsumerLimits.optionalInstance(readValue(v, CONSUMER_LIMITS)));
        builder.mirror(Mirror.optionalInstance(readValue(v, MIRROR)));
        builder.sources(Source.optionalListOf(readValue(v, SOURCES)));
        builder.sealed(readBoolean(v, SEALED));
        builder.allowRollup(readBoolean(v, ALLOW_ROLLUP_HDRS));
        builder.allowDirect(readBoolean(v, ALLOW_DIRECT));
        builder.mirrorDirect(readBoolean(v, MIRROR_DIRECT));
        builder.denyDelete(readBoolean(v, DENY_DELETE));
        builder.denyPurge(readBoolean(v, DENY_PURGE));
        builder.discardNewPerSubject(readBoolean(v, DISCARD_NEW_PER_SUBJECT));
        builder.metadata(readStringStringMap(v, METADATA));
        builder.firstSequence(readLong(v, FIRST_SEQ, 1));
        builder.allowMessageTtl(readBoolean(v, ALLOW_MSG_TTL));
        builder.subjectDeleteMarkerTtl(readNanos(v, SUBJECT_DELETE_MARKER_TTL));
        return builder.build();
    }

    // For the builder, assumes all validations are already done in builder
    StreamConfiguration(Builder b) {
        this.name = b.name;
        this.description = b.description;
        this.subjects = b.subjects;
        this.retentionPolicy = b.retentionPolicy;
        this.compressionOption = b.compressionOption;
        this.maxConsumers = b.maxConsumers;
        this.maxMsgs = b.maxMsgs;
        this.maxMsgsPerSubject = b.maxMsgsPerSubject;
        this.maxBytes = b.maxBytes;
        this.maxAge = b.maxAge;
        this.maxMsgSize = b.maxMsgSize;
        this.storageType = b.storageType;
        this.replicas = b.replicas;
        this.noAck = b.noAck;
        this.templateOwner = b.templateOwner;
        this.discardPolicy = b.discardPolicy;
        this.duplicateWindow = b.duplicateWindow;
        this.placement = b.placement;
        this.republish = b.republish;
        this.subjectTransform = b.subjectTransform;
        this.consumerLimits = b.consumerLimits;
        this.mirror = b.mirror;
        this.sources = b.sources;
        this.sealed = b.sealed;
        this.allowRollup = b.allowRollup;
        this.allowDirect = b.allowDirect;
        this.mirrorDirect = b.mirrorDirect;
        this.denyDelete = b.denyDelete;
        this.denyPurge = b.denyPurge;
        this.discardNewPerSubject = b.discardNewPerSubject;
        this.metadata = b.metadata;
        this.firstSequence = b.firstSequence;
        this.allowMessageTtl = b.allowMessageTtl;
        this.subjectDeleteMarkerTtl = b.subjectDeleteMarkerTtl;
    }

    /**
     * Returns a StreamConfiguration deserialized from its JSON form.
     *
     * @see #toJson()
     * @param json the json representing the Stream Configuration
     * @return StreamConfiguration for the given json
     * @throws JsonParseException thrown if the parsing fails for invalid json
     */
    public static StreamConfiguration instance(String json) throws JsonParseException {
        return instance(JsonParser.parse(json));
    }

    /**
     * Returns a JSON representation of this consumer configuration.
     *
     * @return json consumer configuration to send to the server.
     */
    public String toJson() {

        StringBuilder sb = beginJson();

        addField(sb, NAME, name);
        JsonUtils.addField(sb, DESCRIPTION, description);
        addStrings(sb, SUBJECTS, subjects);
        addField(sb, RETENTION, retentionPolicy.toString());
        addEnumWhenNot(sb, COMPRESSION, compressionOption, CompressionOption.None);
        addField(sb, MAX_CONSUMERS, maxConsumers);
        addField(sb, MAX_MSGS, maxMsgs);
        addField(sb, MAX_MSGS_PER_SUB, maxMsgsPerSubject);
        addField(sb, MAX_BYTES, maxBytes);
        addFieldAsNanos(sb, MAX_AGE, maxAge);
        addField(sb, MAX_MSG_SIZE, maxMsgSize);
        addField(sb, STORAGE, storageType.toString());
        addField(sb, NUM_REPLICAS, replicas);
        addFldWhenTrue(sb, NO_ACK, noAck);
        addField(sb, TEMPLATE_OWNER, templateOwner);
        addField(sb, DISCARD, discardPolicy.toString());
        addFieldAsNanos(sb, DUPLICATE_WINDOW, duplicateWindow);
        if (placement != null && placement.hasData()) {
            addField(sb, PLACEMENT, placement);
        }
        addField(sb, REPUBLISH, republish);
        addField(sb, SUBJECT_TRANSFORM, subjectTransform);
        addField(sb, CONSUMER_LIMITS, consumerLimits);
        addField(sb, MIRROR, mirror);
        addJsons(sb, SOURCES, sources);
        addFldWhenTrue(sb, SEALED, sealed);
        addFldWhenTrue(sb, ALLOW_ROLLUP_HDRS, allowRollup);
        addFldWhenTrue(sb, ALLOW_DIRECT, allowDirect);
        addFldWhenTrue(sb, MIRROR_DIRECT, mirrorDirect);
        addFldWhenTrue(sb, DENY_DELETE, denyDelete);
        addFldWhenTrue(sb, DENY_PURGE, denyPurge);
        addFldWhenTrue(sb, DISCARD_NEW_PER_SUBJECT, discardNewPerSubject);
        addField(sb, METADATA, metadata);
        addFieldWhenGreaterThan(sb, FIRST_SEQ, firstSequence, 1);
        addFldWhenTrue(sb, ALLOW_MSG_TTL, allowMessageTtl);
        addFieldAsNanos(sb, SUBJECT_DELETE_MARKER_TTL, subjectDeleteMarkerTtl);

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
     * Gets the description of this stream configuration.
     * @return the description of the stream.
     */
    public String getDescription() {
        return description;
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
     * Gets the compression option for this stream configuration.
     * @return the compression option for this stream.
     */
    public CompressionOption getCompressionOption() {
        return compressionOption;
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
     * Gets the maximum messages for this stream configuration.
     * @return the maximum number of messages for this stream.
     */
    public long getMaxMsgsPerSubject() {
        return maxMsgsPerSubject;
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
     * @deprecated the server value is a 32-bit signed value. Use {@link #getMaximumMessageSize()} instead.
     * @return the maximum message size for this stream.
     */
    @Deprecated
    public long getMaxMsgSize() {
        return maxMsgSize;
    }

    /**
     * Gets the maximum message size for this stream configuration.
     * @return the maximum message size for this stream.
     */
    public int getMaximumMessageSize() {
        return maxMsgSize;
    }

    /**
     * Gets the storage type for this stream configuration.
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
     * Gets whether acknowledgements are required in this stream configuration.
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
     * Get the placement directives to consider when placing replicas of this stream,
     * random placement when unset. May be null.
     * @return the placement object
     */
    public Placement getPlacement() {
        return placement;
    }

    /**
     * Get the republish configuration. May be null.
     * @return the republish object
     */
    public Republish getRepublish() {
        return republish;
    }

    /**
     * Get the subjectTransform configuration. May be null.
     * @return the subjectTransform object
     */
    public SubjectTransform getSubjectTransform() {
        return subjectTransform;
    }

    /**
     * Get the consumerLimits configuration. May be null.
     * @return the consumerLimits object
     */
    public ConsumerLimits getConsumerLimits() {
        return consumerLimits;
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

    /**
     * Get the flag indicating if the stream is sealed.
     * @return the sealed flag
     */
    public boolean getSealed() {
        return sealed;
    }

    /**
     * Get the flag indicating if the stream allows rollup.
     * @return the allows rollup flag
     */
    public boolean getAllowRollup() {
        return allowRollup;
    }

    /**
     * Get the flag indicating if the stream allows direct message access.
     * @return the allows direct flag
     */
    public boolean getAllowDirect() {
        return allowDirect;
    }

    /**
     * Get the flag indicating if the stream allows
     * higher performance and unified direct access for mirrors as well.
     * @return the allows direct flag
     */
    public boolean getMirrorDirect() {
        return mirrorDirect;
    }

    /**
     * Get the flag indicating if deny delete is set for the stream
     * @return the deny delete flag
     */
    public boolean getDenyDelete() {
        return denyDelete;
    }

    /**
     * Get the flag indicating if deny purge is set for the stream
     * @return the deny purge flag
     */
    public boolean getDenyPurge() {
        return denyPurge;
    }

    /**
     * Whether discard policy with max message per subject is applied per subject.
     * @return the discard new per subject flag
     */
    public boolean isDiscardNewPerSubject() {
        return discardNewPerSubject;
    }

    /**
     * Metadata for the stream
     * @return the metadata map. Might be null.
     */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    /**
     * The first sequence used in the stream.
     * @return the first sequence
     */
    public long getFirstSequence() {
        return firstSequence;
    }

    /**
     * Whether Allow Message TTL is set
     * @return the flag
     */
    public boolean isAllowMessageTtl() {
        return allowMessageTtl;
    }

    /**
     * Get the Subject Delete Marker TTL duration. May be null.
     * @return The duration
     */
    public Duration getSubjectDeleteMarkerTtl() {
        return subjectDeleteMarkerTtl;
    }

    @Override
    public String toString() {
        return toJson();
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
     * <p>{@code new StreamConfiguration.Builder().build()} will create a new StreamConfiguration.
     * 
     */
    public static class Builder {

        private String name = null;
        private String description = null;
        private final List<String> subjects = new ArrayList<>();
        private RetentionPolicy retentionPolicy = RetentionPolicy.Limits;
        private CompressionOption compressionOption = CompressionOption.None;
        private long maxConsumers = -1;
        private long maxMsgs = -1;
        private long maxMsgsPerSubject = -1;
        private long maxBytes = -1;
        private Duration maxAge = Duration.ZERO;
        private int maxMsgSize = -1;
        private StorageType storageType = StorageType.File;
        private int replicas = 1;
        private boolean noAck = false;
        private String templateOwner = null;
        private DiscardPolicy discardPolicy = DiscardPolicy.Old;
        private Duration duplicateWindow = Duration.ZERO;
        private Placement placement = null;
        private Republish republish = null;
        private SubjectTransform subjectTransform = null;
        private ConsumerLimits consumerLimits = null;
        private Mirror mirror = null;
        private final List<Source> sources = new ArrayList<>();
        private boolean sealed = false;
        private boolean allowRollup = false;
        private boolean allowDirect = false;
        private boolean mirrorDirect = false;
        private boolean denyDelete = false;
        private boolean denyPurge = false;
        private boolean discardNewPerSubject = false;
        private Map<String, String> metadata;
        private long firstSequence = 1;
        private boolean allowMessageTtl = false;
        private Duration subjectDeleteMarkerTtl = Duration.ZERO;

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
                this.description = sc.description;
                subjects(sc.subjects);
                this.retentionPolicy = sc.retentionPolicy;
                this.compressionOption = sc.compressionOption;
                this.maxConsumers = sc.maxConsumers;
                this.maxMsgs = sc.maxMsgs;
                this.maxMsgsPerSubject = sc.maxMsgsPerSubject;
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
                this.republish = sc.republish;
                this.subjectTransform = sc.subjectTransform;
                this.consumerLimits = sc.consumerLimits;
                this.mirror = sc.mirror;
                sources(sc.sources);
                this.sealed = sc.sealed;
                this.allowRollup = sc.allowRollup;
                this.allowDirect = sc.allowDirect;
                this.mirrorDirect = sc.mirrorDirect;
                this.denyDelete = sc.denyDelete;
                this.denyPurge = sc.denyPurge;
                this.discardNewPerSubject = sc.discardNewPerSubject;
                if (sc.metadata != null) {
                    this.metadata = new HashMap<>(sc.metadata);
                }
                this.firstSequence = sc.firstSequence;
                this.allowMessageTtl = sc.allowMessageTtl;
                this.subjectDeleteMarkerTtl = sc.subjectDeleteMarkerTtl;
            }
        }

        /**
         * Sets the name of the stream.
         * @param name name of the stream.
         * @return the builder
         */
        public Builder name(String name) {
            this.name =  validateStreamName(name, false);
            return this;
        }

        /**
         * Sets the description
         * @param description the description
         * @return the builder
         */
        public Builder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the subjects in the StreamConfiguration.
         * @param subjects the stream's subjects
         * @return The Builder
         */
        public Builder subjects(String... subjects) {
            this.subjects.clear();
            return addSubjects(subjects);
        }

        /**
         * Sets the subjects in the StreamConfiguration.
         * @param subjects the stream's subjects
         * @return The Builder
         */
        public Builder subjects(Collection<String> subjects) {
            this.subjects.clear();
            return addSubjects(subjects);
        }

        /**
         * Adds unique subjects into the StreamConfiguration.
         * @param subjects the stream's subjects to add
         * @return The Builder
         */
        public Builder addSubjects(String... subjects) {
            if (subjects != null) {
                return addSubjects(Arrays.asList(subjects));
            }
            return this;
        }

        /**
         * Adds unique subjects into the StreamConfiguration.
         * @param subjects the stream's subjects to add
         * @return The Builder
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
         * @return The Builder
         */
        public Builder retentionPolicy(RetentionPolicy policy) {
            this.retentionPolicy = policy == null ? RetentionPolicy.Limits : policy;
            return this;
        }

        /**
         * Sets the compression option in the StreamConfiguration.
         * @param compressionOption the compression option of the StreamConfiguration
         * @return The Builder
         */
        public Builder compressionOption(CompressionOption compressionOption) {
            this.compressionOption = compressionOption == null ? CompressionOption.None : compressionOption;
            return this;
        }

        /**
         * Sets the maximum number of consumers in the StreamConfiguration.
         * @param maxConsumers the maximum number of consumers
         * @return The Builder
         */        
        public Builder maxConsumers(long maxConsumers) {
            this.maxConsumers = validateMaxConsumers(maxConsumers);
            return this;
        }

        /**
         * Sets the maximum number of messages in the StreamConfiguration.
         * @param maxMsgs the maximum number of messages
         * @return The Builder
         */
        public Builder maxMessages(long maxMsgs) {
            this.maxMsgs = validateMaxMessages(maxMsgs);
            return this;
        }

        /**
         * Sets the maximum number of message per subject in the StreamConfiguration.
         * @param maxMsgsPerSubject the maximum number of messages
         * @return The Builder
         */
        public Builder maxMessagesPerSubject(long maxMsgsPerSubject) {
            this.maxMsgsPerSubject = validateMaxMessagesPerSubject(maxMsgsPerSubject);
            return this;
        }

        /**
         * Sets the maximum number of bytes in the StreamConfiguration.
         * @param maxBytes the maximum number of bytes
         * @return The Builder
         */
        public Builder maxBytes(long maxBytes) {
            this.maxBytes = validateMaxBytes(maxBytes);
            return this;
        }

        /**
         * Sets the maximum age in the StreamConfiguration.
         * @param maxAge the maximum message age
         * @return The Builder
         */
        public Builder maxAge(Duration maxAge) {
            this.maxAge = validateDurationNotRequiredGtOrEqZero(maxAge, Duration.ZERO);
            return this;
        }

        /**
         * Sets the maximum age in the StreamConfiguration.
         * @param maxAgeMillis the maximum message age
         * @return The Builder
         */
        public Builder maxAge(long maxAgeMillis) {
            this.maxAge = validateDurationNotRequiredGtOrEqZero(maxAgeMillis);
            return this;
        }

        /**
         * Sets the maximum message size in the StreamConfiguration.
         * @deprecated the server value is a 32-bit signed value. Use {@link #maximumMessageSize(int)} instead.
         * @param maxMsgSize the maximum message size
         * @return The Builder
         */
        @Deprecated
        public Builder maxMsgSize(long maxMsgSize) {
            this.maxMsgSize = (int)validateMaxMessageSize(maxMsgSize);
            return this;
        }

        /**
         * Sets the maximum message size in the StreamConfiguration.
         * @param maxMsgSize the maximum message size
         * @return The Builder
         */
        public Builder maximumMessageSize(int maxMsgSize) {
            this.maxMsgSize = (int)validateMaxMessageSize(maxMsgSize);
            return this;
        }

        /**
         * Sets the storage type in the StreamConfiguration.
         * @param storageType the storage type
         * @return The Builder
         */        
        public Builder storageType(StorageType storageType) {
            this.storageType = storageType == null ? StorageType.File : storageType;
            return this;
        }

        /**
         * Sets the number of replicas a message must be stored on in the StreamConfiguration.
         * Must be 1 to 5 inclusive
         * @param replicas the number of replicas to store this message on
         * @return The Builder
         */
        public Builder replicas(int replicas) {
            this.replicas = validateNumberOfReplicas(replicas);
            return this;
        }

        /**
         * Sets the acknowledgement mode of the StreamConfiguration.  if no acknowledgements are
         * set, then acknowledgements are not sent back to the client.  The default is false.
         * @param noAck true to disable acknowledgements.
         * @return The Builder
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
         * @return The Builder
         */
        public Builder discardPolicy(DiscardPolicy policy) {
            this.discardPolicy = policy == null ? DiscardPolicy.Old : policy;
            return this;
        }

        /**
         * Sets the duplicate checking window in the StreamConfiguration.  A Duration.Zero
         * disables duplicate checking.  Duplicate checking is disabled by default.
         * @param window duration to hold message ids for duplicate checking.
         * @return The Builder
         */
        public Builder duplicateWindow(Duration window) {
            this.duplicateWindow = validateDurationNotRequiredGtOrEqZero(window, Duration.ZERO);
            return this;
        }

        /**
         * Sets the duplicate checking window in the StreamConfiguration.  A Duration.Zero
         * disables duplicate checking.  Duplicate checking is disabled by default.
         * @param windowMillis duration to hold message ids for duplicate checking.
         * @return The Builder
         */
        public Builder duplicateWindow(long windowMillis) {
            this.duplicateWindow = validateDurationNotRequiredGtOrEqZero(windowMillis);
            return this;
        }

        /**
         * Sets the placement directive object
         * @param placement the placement directive object
         * @return The Builder
         */
        public Builder placement(Placement placement) {
            this.placement = placement;
            return this;
        }

        /**
         * Sets the republish config object
         * @param republish the republish config object
         * @return The Builder
         */
        public Builder republish(Republish republish) {
            this.republish = republish;
            return this;
        }

        /**
         * Sets the subjectTransform config object
         * @param subjectTransform the subjectTransform config object
         * @return The Builder
         */
        public Builder subjectTransform(SubjectTransform subjectTransform) {
            this.subjectTransform = subjectTransform;
            return this;
        }

        /**
         * Sets the consumerLimits config object
         * @param consumerLimits the consumerLimits config object
         * @return The Builder
         */
        public Builder consumerLimits(ConsumerLimits consumerLimits) {
            this.consumerLimits = consumerLimits;
            return this;
        }

        /**
         * Sets the mirror  object
         * @param mirror the mirror object
         * @return The Builder
         */
        public Builder mirror(Mirror mirror) {
            this.mirror = mirror;
            return this;
        }

        /**
         * Sets the sources in the StreamConfiguration.
         * @param sources the stream's sources
         * @return The Builder
         */
        public Builder sources(Source... sources) {
            this.sources.clear();
            return addSources(sources);
        }

        /**
         * Add the sources into the StreamConfiguration.
         * @param sources the stream's sources
         * @return The Builder
         */
        public Builder sources(Collection<Source> sources) {
            this.sources.clear();
            return addSources(sources);
        }

        /**
         * Add the sources into the StreamConfiguration.
         * @param sources the stream's sources
         * @return The Builder
         */
        public Builder addSources(Source... sources) {
            return addSources(Arrays.asList(sources));
        }

        /**
         * Sets the sources in the StreamConfiguration.
         * @param sources the stream's sources
         * @return The Builder
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
         * Add a source into the StreamConfiguration.
         * @param source a stream source
         * @return The Builder
         */
        public Builder addSource(Source source) {
            if (source != null && !this.sources.contains(source)) {
                this.sources.add(source);
            }
            return this;
        }

        /**
         * Set whether to seal the stream.
         * INTERNAL USE ONLY. Scoped protected for test purposes.
         * @param sealed the sealed setting
         * @return The Builder
         */
        protected Builder sealed(boolean sealed) {
            this.sealed = sealed;
            return this;
        }

        /**
         * Set whether to allow the rollup feature for a stream
         * @param allowRollup the allow rollup setting
         * @return The Builder
         */
        public Builder allowRollup(boolean allowRollup) {
            this.allowRollup = allowRollup;
            return this;
        }

        /**
         * Set whether to allow direct message access for a stream
         * @param allowDirect the allow direct setting
         * @return The Builder
         */
        public Builder allowDirect(boolean allowDirect) {
            this.allowDirect = allowDirect;
            return this;
        }

        /**
         * Set whether to allow unified direct access for mirrors
         * @param mirrorDirect the allow direct setting
         * @return The Builder
         */
        public Builder mirrorDirect(boolean mirrorDirect) {
            this.mirrorDirect = mirrorDirect;
            return this;
        }

        /**
         * Set whether to deny deleting messages from the stream
         * @param denyDelete the deny delete setting
         * @return The Builder
         */
        public Builder denyDelete(boolean denyDelete) {
            this.denyDelete = denyDelete;
            return this;
        }

        /**
         * Set whether to deny purging messages from the stream
         * @param denyPurge the deny purge setting
         * @return The Builder
         */
        public Builder denyPurge(boolean denyPurge) {
            this.denyPurge = denyPurge;
            return this;
        }

        /**
         * Set whether discard policy new with max message per subject applies to existing subjects, not just new subjects.
         * @param discardNewPerSubject the setting
         * @return The Builder
         */
        public Builder discardNewPerSubject(boolean discardNewPerSubject) {
            this.discardNewPerSubject = discardNewPerSubject;
            return this;
        }

        /**
         * Set this stream to be sealed. This is irreversible.
         * @return The Builder
         */
        public Builder seal() {
            this.sealed = true;
            return this;
        }

        /**
         * Sets the metadata for the configuration
         * @param metadata the metadata map
         * @return The Builder
         */
        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        /**
         * Sets the first sequence to be used. 1 is the default. All values less than 2 are treated as 1.
         * @param firstSeq specify the first_seq in the stream config when creating the stream.
         * @return The Builder
         */
        public Builder firstSequence(long firstSeq) {
            this.firstSequence = firstSeq > 1 ? firstSeq : 1;
            return this;
        }

        /**
         * Set to allow per message TTL to true
         * @return The Builder
         */
        public Builder allowMessageTtl() {
            this.allowMessageTtl = true;
            return this;
        }

        /**
         * Set allow per message TTL flag
         * @param allowMessageTtl the flag
         * @return The Builder
         */
        public Builder allowMessageTtl(boolean allowMessageTtl) {
            this.allowMessageTtl = allowMessageTtl;
            return this;
        }

        /**
         * The time delete marker TTL duration. Server accepts 1 second or more.
         * CLIENT DOES NOT VALIDATE
         * @param subjectDeleteMarkerTtl the TTL duration
         * @return The Builder
         */
        public Builder subjectDeleteMarkerTtl(Duration subjectDeleteMarkerTtl) {
            this.subjectDeleteMarkerTtl = subjectDeleteMarkerTtl;
            return this;
        }

        /**
         * Builds the StreamConfiguration
         * @return a stream configuration.
         */
        public StreamConfiguration build() {
            return new StreamConfiguration(this);
        }
    }
}
