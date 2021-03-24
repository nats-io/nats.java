package io.nats.client.api;

import io.nats.client.Message;
import io.nats.client.support.JsonUtils;

import static io.nats.client.support.SchemaConstants.*;

/**
 * The JetStream Account Statistics
 */
public class AccountStatistics
        extends ApiResponse<AccountStatistics> {

    private final long memory;
    private final long storage;
    private final long streams;
    private final long consumers;

    public AccountStatistics(Message msg) {
        super(msg);
        memory = JsonUtils.readLong(json, MEMORY_RE, 0);
        storage = JsonUtils.readLong(json, STORAGE_RE, 0);
        streams = JsonUtils.readLong(json, STREAMS_RE, 0);
        consumers = JsonUtils.readLong(json, CONSUMERS_RE, 0);
    }

    /**
     * Gets the amount of memory used by the JetStream deployment.
     *
     * @return bytes
     */
    public long getMemory() {
        return memory;
    }

    /**
     * Gets the amount of storage used by  the JetStream deployment.
     *
     * @return bytes
     */
    public long getStorage() {
        return storage;
    }

    /**
     * Gets the number of streams used by the JetStream deployment.
     *
     * @return stream maximum count
     */
    public long getStreams() {
        return streams;
    }

    /**
     * Gets the number of consumers used by the JetStream deployment.
     *
     * @return consumer maximum count
     */
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
