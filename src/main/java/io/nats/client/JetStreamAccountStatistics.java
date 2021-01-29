package io.nats.client;

/**
 * The JetStream Account Statistics
 */
public interface JetStreamAccountStatistics {

    /**
     * Gets the amount of memory used by the JetStream deployment.
     *
     * @return bytes
     */
    long getMemory();

    /**
     * Gets the amount of storage used by  the JetStream deployment.
     *
     * @return bytes
     */
    long getStorage();

    /**
     * Gets the number of streams used by the JetStream deployment.
     *
     * @return stream maximum count
     */
    long getStreams();

    /**
     * Gets the number of consumers used by the JetStream deployment.
     *
     * @return consumer maximum count
     */
    long getConsumers();
}
