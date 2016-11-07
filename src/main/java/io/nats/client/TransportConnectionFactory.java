package io.nats.client;

public interface TransportConnectionFactory {
    TransportConnection createConnection();
}
