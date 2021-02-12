package io.nats.client.impl;

import io.nats.client.JetStreamAccountLimits;

import static io.nats.client.support.ApiConstants.*;

public class NatsJetStreamAccountLimits implements JetStreamAccountLimits {

    private final long maxMemory;
    private final long maxStorage;
    private final long maxStreams;
    private final long maxConsumers;

    public NatsJetStreamAccountLimits(String json) {
        this.maxMemory = JsonUtils.readLong(json, MAX_MEMORY_RE, -1);
        this.maxStorage = JsonUtils.readLong(json, MAX_STORAGE_RE, -1);
        this.maxStreams = JsonUtils.readLong(json, MAX_STREAMS_RE, -1);
        this.maxConsumers = JsonUtils.readLong(json, MAX_CONSUMERS_RE, -1);
    }

    @Override
    public long getMaxMemory() {
        return maxMemory;
    }

    @Override
    public long getMaxStorage() {
        return maxStorage;
    }

    @Override
    public long getMaxStreams() {
        return maxStreams;
    }

    @Override
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
