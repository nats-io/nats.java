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

package io.nats.client;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.nats.client.impl.JsonUtils;
import io.nats.client.impl.JsonUtils.FieldType;


// TODO Add properties

// JSApiStreamCreateT   = "$JS.API.STREAM.CREATE.%s"

/**
 * The StreamConfiguration class specifies the configuration for creating a jetstream stream on the server.
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

        private String policy;

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

        private String policy;

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

        private String policy;

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

	private String name = null;
    private String[] subjects = null;
    private RetentionPolicy retentionPolicy = RetentionPolicy.Limits;

    private long maxConsumers = -1;
    private long maxBytes = -1;
    private long maxMsgSize = -1;
    private Duration maxAge = null;
    private StorageType storageType = StorageType.File;
    private DiscardPolicy discardPolicy = DiscardPolicy.Old;
    private long replicas = -1;
    private boolean noAck = false;
    private Duration duplicateWindow = Duration.ZERO;
    private String template = null;

    private static String nameField = "name";
    private static String subjectsField = "subjects";
    private static String retentionField = "retention";
    private static String maxConsumersField =  "max_consumers";
    private static String maxBytesField =  "max_bytes";

    private static String maxAgeField =  "max_age";
    private static String maxMsgSizeField =  "max_msg_size";
    private static String storageTypeField =  "storage";
    private static String discardPolicyField = "discard";
    private static String replicasField =  "num_replicas";
    private static String noAckField =  "no_ack";
    private static String templateField =  "template";
    private static String duplicatesField =  "duplicates";

    private static final Pattern nameRE = JsonUtils.buildPattern(nameField, FieldType.jsonString);
    private static final Pattern maxConsumersRE = JsonUtils.buildPattern(maxConsumersField, FieldType.jsonNumber);
    private static final Pattern retentionRE = JsonUtils.buildPattern(retentionField, FieldType.jsonString);
    private static final Pattern maxBytesRE = JsonUtils.buildPattern(maxBytesField, FieldType.jsonNumber);
    private static final Pattern maxAgeRE = JsonUtils.buildPattern(maxAgeField, FieldType.jsonNumber);
    private static final Pattern maxMsgSizeRE = JsonUtils.buildPattern(maxMsgSizeField, FieldType.jsonNumber);
    private static final Pattern storageTypeRE = JsonUtils.buildPattern(storageTypeField, FieldType.jsonString);
    private static final Pattern discardPolicyRE = JsonUtils.buildPattern(discardPolicyField, FieldType.jsonString);
    private static final Pattern replicasRE = JsonUtils.buildPattern(replicasField, FieldType.jsonNumber);
    private static final Pattern noAckRE = JsonUtils.buildPattern(noAckField, FieldType.jsonBoolean);
    private static final Pattern templateRE = JsonUtils.buildPattern(templateField, FieldType.jsonString);
    private static final Pattern duplicatesRE = JsonUtils.buildPattern(duplicatesField, FieldType.jsonNumber);
    
    // for the response from the server
    StreamConfiguration(String json) {
        Matcher m = nameRE.matcher(json);
        if (m.find()) {
            this.name = m.group(1);
        }

        m = maxConsumersRE.matcher(json);
        if (m.find()) {
            this.maxConsumers = Long.parseLong(m.group(1));
        }
        
        m = retentionRE.matcher(json);
        if (m.find()) {
            this.retentionPolicy = RetentionPolicy.get(m.group(1));
        }

        m = maxBytesRE.matcher(json);
        if (m.find()) {
            this.maxBytes = Long.parseLong(m.group(1));
        }

        m = maxAgeRE.matcher(json);
        if (m.find()) {
            this.maxAge = Duration.ofNanos(Long.parseLong(m.group(1)));
        }        

        m = maxMsgSizeRE.matcher(json);
        if (m.find()) {
            this.maxMsgSize = Long.parseLong(m.group(1));
        } 

        m = storageTypeRE.matcher(json);
        if (m.find()) {
            this.storageType = StorageType.get(m.group(1));
        }

        m = discardPolicyRE.matcher(json);
        if (m.find()) {
            this.discardPolicy = DiscardPolicy.get(m.group(1));
        }

        m = replicasRE.matcher(json);
        if (m.find()) {
            this.replicas = Long.parseLong(m.group(1));
        }

        m = noAckRE.matcher(json);
        if (m.find()) {
            this.noAck = Boolean.parseBoolean(m.group(1));
        }

        m = templateRE.matcher(json);
        if (m.find()) {
            this.template = m.group(1);
        }

        m = duplicatesRE.matcher(json);
        if (m.find()) {
            this.duplicateWindow = Duration.ofNanos(Long.parseLong(m.group(1)));
        }
        
        this.subjects = JsonUtils.parseStringArray(subjectsField, json);
    }

    // For the builder
    StreamConfiguration(
        String name,
        String[] subjects,
        RetentionPolicy retentionPolicy,
        long maxConsumers,
        long maxBytes,
        long maxMsgSize,
        Duration maxAge,
        StorageType storageType,
        DiscardPolicy discardPolicy,
        long replicas,
        boolean noAck,
        Duration duplicateWindow,
        String template) {
            this.name = name;
            this.subjects = subjects;
            this.retentionPolicy = retentionPolicy;
            this.maxConsumers = maxConsumers;
            this.maxBytes = maxBytes;
            this.maxMsgSize = maxMsgSize;
            this.maxAge = maxAge;
            this.storageType = storageType;
            this.discardPolicy = discardPolicy;
            this.replicas = replicas;
            this.noAck = noAck;
            this.duplicateWindow = duplicateWindow;
            this.template = template;
    }

    /**
     * Returns a JSON representation of this consumer configuration.
     * 
     * @return json consumer configuration to send to the server.
     */
    public String toJSON() {
        
        StringBuilder sb = new StringBuilder("{");
        
        JsonUtils.addFld(sb, nameField, name);
        JsonUtils.addFld(sb, subjectsField, subjects);
        JsonUtils.addFld(sb, retentionField, retentionPolicy.toString());
        JsonUtils.addFld(sb, maxConsumersField, maxConsumers);
        JsonUtils.addFld(sb, maxBytesField, maxBytes);
        JsonUtils.addFld(sb, maxMsgSizeField, maxMsgSize);
        JsonUtils.addFld(sb, maxAgeField, maxAge);
        JsonUtils.addFld(sb, storageTypeField , storageType.toString());
        JsonUtils.addFld(sb, discardPolicyField, discardPolicy.toString());
        JsonUtils.addFld(sb, replicasField, replicas);
        JsonUtils.addFld(sb, noAckField, noAck);
        JsonUtils.addFld(sb, templateField, template);
        JsonUtils.addFld(sb, duplicatesField, duplicateWindow);

        // remove the trailing ','
        sb.setLength(sb.length()-1);

        sb.append("}");

        return sb.toString();
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
    public String[] getSubjects() {
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
     * Gets whether or not acknowedgements are required in this stream configuration.
     * @return true if acknowedgments are not required.
     */
    public boolean getNoAck() {
        return noAck;
    }

    /**
     * Gets the template json for this stream configuration.
     * @return the template for this stream.
     */    
    public String getTemplate() {
        return template;
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
     * Creates a builder for the stream configuration.
     * @return a stream configuration builder
     */
    public static Builder builder() {
        return new Builder();
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
        private String[] subjects = null;
        private RetentionPolicy retentionPolicy = RetentionPolicy.Limits;
        private long maxConsumers = -1;
        private long maxBytes = -1;
        private long maxMsgSize = -1;
        private Duration maxAge = Duration.ZERO;
        private StorageType storageType = StorageType.File;
        private DiscardPolicy discardPolicy = DiscardPolicy.Old;
        private long replicas = 1;
        private boolean noAck = false;
        private Duration duplicateWindow = Duration.ZERO;
        private String template = null;
    
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
        public Builder subjects(String[] subjects) {
            if (subjects == null || subjects.length == 0) {
                throw new IllegalArgumentException("Subjects cannot be null or empty");
            }
            this.subjects = subjects;
            return this;
        }

        /**
         * Sets the retention policy in the StreamConfiguration.
         * @param policy the retention policy of the StreamConfguration
         * @return Builder
         */
        public Builder retentionPolicy(RetentionPolicy policy) {
            this.retentionPolicy = policy;
            return this;
        }

        /**
         * Sets the maximum number of consumers in the StreamConfiguration.
         * @param maxConsumers the maximum number of consumers
         * @return Builder
         */        
        public Builder maxConsumers(long maxConsumers) {
            this.maxConsumers = maxConsumers;
            return this;
        }


        /**
         * Sets the maximum number of bytes in the StreamConfiguration.
         * @param maxBytes the maximum number of bytes
         * @return Builder
         */        
        public Builder maxBytes(long maxBytes) {
            this.maxBytes = maxBytes;
            return this;
        }

        /**
         * Sets the maximum age in the StreamConfiguration.
         * @param maxAge the maximum message age
         * @return Builder
         */        
        public Builder maxAge(Duration maxAge) {
            this.maxAge = maxAge;
            return this;
        }
        
        /**
         * Sets the maximum message size in the StreamConfiguration.
         * @param maxMsgSize the maximum message size
         * @return Builder
         */        
        public Builder maxMsgSize(long maxMsgSize) {
            if (maxMsgSize <= 0) {
                throw new IllegalArgumentException("value must be greater than zero");
            }
            this.maxMsgSize = maxMsgSize;
            return this;
        }  

        /**
         * Sets the storage type in the StreamConfiguration.
         * @param storageType the storage type
         * @return Builder
         */        
        public Builder storageType(StorageType storageType) {
            this.storageType = storageType;
            return this;
        }       
        
        
        /**
         * Sets the discard policy in the StreamConfiguration.
         * @param policy the discard policy of the StreamConfguration
         * @return Builder
         */
        public Builder discardPolicy(DiscardPolicy policy) {
            this.discardPolicy = policy;
            return this;
        }

        /**
         * Sets the number of replicas a message must be stored on in the StreamConfiguration.
         * @param replicas the number of replicas to store this message on
         * @return Builder
         */        
        public Builder replicas(long replicas) {
            this.replicas = replicas;
            return this;
        } 

        /**
         * Sets the acknowedgement mode of the StreamConfiguration.  if no acknowedgements are
         * set, then acknowedgements are not sent back to the client.  The default is false.
         * @param noAck true to disable acknowedgements.
         * @return Builder
         */        
        public Builder noAck(boolean noAck) {
            this.noAck = noAck;
            return this;
        }        

        /**
         * Sets the duplicate checking window in the the StreamConfiguration.  A Duration.Zero
         * disables duplicate checking.  Duplicate checking is disabled by default.
         * @param window duration to hold message ids for duplicate checking.
         * @return Builder
         */        
        public Builder duplicateWindow(Duration  window) {
            this.duplicateWindow = window;
            return this;
        } 

        /**
         * Sets the template a stream in the form of raw JSON.
         * @param template the stream template of the stream.
         * @return the builder
         */
        public Builder template(String template) {
            this.template = template;
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
                maxBytes,
                maxMsgSize,
                maxAge,
                storageType,
                discardPolicy,
                replicas,
                noAck,
                duplicateWindow,
                template
            );
        }

    }
}
