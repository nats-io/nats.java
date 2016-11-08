package io.nats.client;

interface TransportConnectionFactory {
    TransportConnection createConnection();
}
