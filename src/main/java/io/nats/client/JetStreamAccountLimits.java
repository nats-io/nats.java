package io.nats.client;

/**
 * Represents the JetStream Account Limits
 */
public interface JetStreamAccountLimits {

    /**
     * Gets the maximum amount of memory in the JetStream deployment.
     *
     * @return bytes
     */
    long getMaxMemory();

    /**
     * Gets the maximum amount of storage in the JetStream deployment.
     *
     * @return bytes
     */
    long getMaxStorage();

    /**
     * Gets the maximum number of allowed streams in the JetStream deployment.
     *
     * @return stream maximum count
     */
    long getMaxStreams();

    /**
     * Gets the maximum number of allowed consumers in the JetStream deployment.
     *
     * @return consumer maximum count
     */
    long getMaxConsumers();
}
