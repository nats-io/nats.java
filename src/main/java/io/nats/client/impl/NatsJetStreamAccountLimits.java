package io.nats.client.impl;

import io.nats.client.JetStreamAccountLimits;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.nats.client.impl.JsonUtils.buildNumberPattern;

public class NatsJetStreamAccountLimits implements JetStreamAccountLimits {

    private static final Pattern LIMITS_MEMORY_RE = buildNumberPattern("max_memory");
    private static final Pattern LIMITS_STORAGE_RE = buildNumberPattern("max_storage");
    private static final Pattern LIMIT_STREAMS_RE = buildNumberPattern("max_streams");
    private static final Pattern LIMIT_CONSUMERS_RE = buildNumberPattern("max_consumers");

    private long maxMemory = -1;
    private long maxStorage = -1;
    private long maxStreams = -1;
    private long maxConsumers = 1;

    public NatsJetStreamAccountLimits(String json) {
        Matcher m = LIMITS_MEMORY_RE.matcher(json);
        if (m.find()) {
            this.maxMemory = Long.parseLong(m.group(1));
        }

        m = LIMITS_STORAGE_RE.matcher(json);
        if (m.find()) {
            this.maxStorage = Long.parseLong(m.group(1));
        }

        m = LIMIT_STREAMS_RE.matcher(json);
        if (m.find()) {
            this.maxStreams = Long.parseLong(m.group(1));
        }

        m = LIMIT_CONSUMERS_RE.matcher(json);
        if (m.find()) {
            this.maxConsumers = Long.parseLong(m.group(1));
        }
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
