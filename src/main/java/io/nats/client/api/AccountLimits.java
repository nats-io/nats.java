package io.nats.client.api;

import io.nats.client.support.JsonUtils;

import static io.nats.client.support.ApiConstants.*;

/**
 * Represents the JetStream Account Limits
 */
public class AccountLimits {

    private final long maxMemory;
    private final long maxStorage;
    private final long maxStreams;
    private final long maxConsumers;

    public AccountLimits(String json) {
        this.maxMemory = JsonUtils.readLong(json, MAX_MEMORY_RE, -1);
        this.maxStorage = JsonUtils.readLong(json, MAX_STORAGE_RE, -1);
        this.maxStreams = JsonUtils.readLong(json, MAX_STREAMS_RE, -1);
        this.maxConsumers = JsonUtils.readLong(json, MAX_CONSUMERS_RE, -1);
    }

    /**
     * Gets the maximum amount of memory in the JetStream deployment.
     *
     * @return bytes
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * Gets the maximum amount of storage in the JetStream deployment.
     *
     * @return bytes
     */
    public long getMaxStorage() {
        return maxStorage;
    }

    /**
     * Gets the maximum number of allowed streams in the JetStream deployment.
     *
     * @return stream maximum count
     */
    public long getMaxStreams() {
        return maxStreams;
    }

    /**
     * Gets the maximum number of allowed consumers in the JetStream deployment.
     *
     * @return consumer maximum count
     */
    public long getMaxConsumers() {
        return maxConsumers;
    }

    @Override
    public String toString() {
        return "AccountLimitImpl{" +
                "memory=" + maxMemory +
                ", storage=" + maxStorage +
                ", streams=" + maxStreams +
                ", consumers=" + maxConsumers +
                '}';
    }
}
