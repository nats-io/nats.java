package io.nats.client.impl;

import io.nats.client.JetStreamAccountStatistics;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.nats.client.impl.JsonUtils.buildNumberPattern;

public class NatsJetStreamAccountStats implements JetStreamAccountStatistics {

    private static final Pattern STATS_MEMORY_RE = buildNumberPattern("memory");
    private static final Pattern STATS_STORAGE_RE = buildNumberPattern("storage");
    private static final Pattern STATS_STREAMS_RE = buildNumberPattern("streams");
    private static final Pattern STATS_CONSUMERS_RE = buildNumberPattern("consumers");

    private long memory = -1;
    private long storage = -1;
    private long streams = -1;
    private long consumers = 1;

    public NatsJetStreamAccountStats(String json) {
        Matcher m = STATS_MEMORY_RE.matcher(json);
        if (m.find()) {
            this.memory = Long.parseLong(m.group(1));
        }

        m = STATS_STORAGE_RE.matcher(json);
        if (m.find()) {
            this.storage = Long.parseLong(m.group(1));
        }

        m = STATS_STREAMS_RE.matcher(json);
        if (m.find()) {
            this.streams = Long.parseLong(m.group(1));
        }

        m = STATS_CONSUMERS_RE.matcher(json);
        if (m.find()) {
            this.consumers = Long.parseLong(m.group(1));
        }
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
