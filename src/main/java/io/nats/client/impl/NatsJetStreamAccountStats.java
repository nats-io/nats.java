package io.nats.client.impl;

import io.nats.client.JetStreamAccountStatistics;

import static io.nats.client.support.ApiConstants.*;

public class NatsJetStreamAccountStats implements JetStreamAccountStatistics {

    private final long memory;
    private final long storage;
    private final long streams;
    private final long consumers;

    public NatsJetStreamAccountStats(String json) {
        memory = JsonUtils.readLong(json, MEMORY_RE, 0);
        storage = JsonUtils.readLong(json, STORAGE_RE, 0);
        streams = JsonUtils.readLong(json, STREAMS_RE, 0);
        consumers = JsonUtils.readLong(json, CONSUMERS_RE, 0);
    }

    @Override
    public long getMemory() {
        return memory;
    }

    @Override
    public long getStorage() {
        return storage;
    }

    @Override
    public long getStreams() {
        return streams;
    }

    @Override
    public long getConsumers() {
        return consumers;
    }

    @Override
    public String toString() {
        return "AccountStatsImpl{" +
                "memory=" + memory +
                ", storage=" + storage +
                ", streams=" + streams +
                ", consumers=" + consumers +
                '}';
    }
}
