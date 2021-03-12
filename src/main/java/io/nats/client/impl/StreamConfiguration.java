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

package io.nats.client.impl;

import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;

import static io.nats.client.impl.Validator.*;
import static io.nats.client.support.ApiConstants.*;

/**
 * The StreamConfiguration class specifies the configuration for creating a JetStream stream on the server.
 * Options are created using a {@link StreamConfiguration.Builder Builder}.
 */
public class StreamConfiguration {

    /**
     * Stream retention policies.
     */
    public enum RetentionPolicy {
        Limits("limits"),
        Interest("interest"),
        WorkQueue("workqueue");

        private final String policy;

        RetentionPolicy(String p) {
            policy = p;
        }

        @Override
        public String toString() {
            return policy;
        }        

        private static final Map<String, RetentionPolicy> strEnumHash = new HashMap<>();
        static {
            for(RetentionPolicy env : RetentionPolicy.values()){
                strEnumHash.put(env.toString(), env);
            }
        }

        public static RetentionPolicy get(String value) {
            return strEnumHash.get(value);
        }        
    }

    /**
     * Stream discard policies
     */
    public enum DiscardPolicy {
        New("new"),
        Old("old");

        private final String policy;

        DiscardPolicy(String p) {
            policy = p;
        }

        @Override
        public String toString() {
            return policy;
        }
        
        private static final Map<String, DiscardPolicy> strEnumHash = new HashMap<>();
        static {
            for(DiscardPolicy env : DiscardPolicy.values()) {
                strEnumHash.put(env.toString(), env);
            }
        }

        public static DiscardPolicy get(String value) {
            return strEnumHash.get(value);
        }
    }

    /**
     * Stream storage types.
     */
    public enum StorageType {
        File("file"),
        Memory("memory");

        private final String policy;

        StorageType(String p) {
            policy = p;
        }

        @Override
        public String toString() {
            return policy;
        }
        
        private static final Map<String, StorageType> strEnumHash = new HashMap<>();
        static {
            for(StorageType env : StorageType.values()) {
                strEnumHash.put(env.toString(), env);
            }
        }

        public static StorageType get(String value) {
            return strEnumHash.get(value);
        }
    }

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

    // for the response from the server
    static StreamConfiguration fromJson(String json) {
        Builder builder = new Builder();

        Matcher m = RETENTION_RE.matcher(json);
        if (m.find()) {
            builder.retentionPolicy = RetentionPolicy.get(m.group(1));
        }

        m = STORAGE_TYPE_RE.matcher(json);
        if (m.find()) {
            builder.storageType = StorageType.get(m.group(1));
        }

        m = DISCARD_RE.matcher(json);
        if (m.find()) {
            builder.discardPolicy = DiscardPolicy.get(m.group(1));
        }

        builder.name = JsonUtils.readString(json, NAME_RE);
        JsonUtils.readLong(json, MAX_CONSUMERS_RE, l -> builder.maxConsumers = l);
        JsonUtils.readLong(json, MAX_MSGS_RE, l -> builder.maxMsgs = l);
        JsonUtils.readLong(json, MAX_BYTES_RE, l -> builder.maxBytes = l);
        JsonUtils.readNanos(json, MAX_AGE_RE, dur -> builder.maxAge = dur);
        JsonUtils.readLong(json, MAX_MSG_SIZE_RE, l -> builder.maxMsgSize = l);
        JsonUtils.readInt(json, REPLICAS_RE, i -> builder.replicas = i);
        builder.noAck = JsonUtils.readBoolean(json, NO_ACK_RE);
        builder.templateOwner = JsonUtils.readString(json, TEMPLATE_RE);
        JsonUtils.readNanos(json, DUPLICATE_WINDOW_RE, dur -> builder.duplicateWindow = dur);

        builder.subjects(JsonUtils.getStringArray(SUBJECTS, json));

        return builder.build();
    }

    // For the builder, assumes all validations are already done in builder
    StreamConfiguration(
            String name, List<String> subjects, RetentionPolicy retentionPolicy,
            long maxConsumers, long maxMsgs, long maxBytes,
            Duration maxAge, long maxMsgSize, StorageType storageType,
            int replicas, boolean noAck, String templateOwner,
            DiscardPolicy discardPolicy, Duration duplicateWindow)
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
    }

    /**
     * Returns a JSON representation of this consumer configuration.
     * 
     * @return json consumer configuration to send to the server.
     */
    public String toJSON() {
        
        StringBuilder sb = JsonUtils.beginJson();
        
        JsonUtils.addFld(sb, NAME, name);
        JsonUtils.addFld(sb, SUBJECTS, subjects);
        JsonUtils.addFld(sb, RETENTION, retentionPolicy.toString());
        JsonUtils.addFld(sb, MAX_CONSUMERS, maxConsumers);
        JsonUtils.addFld(sb, MAX_MSGS, maxMsgs);
        JsonUtils.addFld(sb, MAX_BYTES, maxBytes);
        JsonUtils.addNanoFld(sb, MAX_AGE, maxAge);
        JsonUtils.addFld(sb, MAX_MSG_SIZE, maxMsgSize);
        JsonUtils.addFld(sb, STORAGE, storageType.toString());
        JsonUtils.addFld(sb, NUM_REPLICAS, replicas);
        JsonUtils.addFld(sb, NO_ACK, noAck);
        JsonUtils.addFld(sb, TEMPLATE, templateOwner);
        JsonUtils.addFld(sb, DISCARD, discardPolicy.toString());
        JsonUtils.addNanoFld(sb, DUPLICATE_WINDOW, duplicateWindow);

        return JsonUtils.endJson(sb).toString();
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
    public long getReplicas() {
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
     * Creates a builder for the stream configuration.
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

        /**
         * Default Builder
         */
        public Builder() {}

        /**
         * Update Builder, useful if you need to update a configuration
         * @param sc the configuration to copy
         */
        public Builder(StreamConfiguration sc) {
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
         * @param policy the retention policy of the StreamConfguration
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
         * @param template the stream template of the stream.
         * @return the builder
         */
        public Builder template(String template) {
            this.templateOwner = emptyAsNull(template);
            return this;
        }

        /**
         * Sets the discard policy in the StreamConfiguration.
         * @param policy the discard policy of the StreamConfguration
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
                    duplicateWindow
            );
        }

    }
}
