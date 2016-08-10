package io.nats.client;

class TCPConnectionFactory {

    protected TCPConnection createConnection() {
        return new TCPConnection();
    }
}
