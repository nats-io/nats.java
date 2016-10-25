package io.nats.client;

class TcpConnectionFactory {

    protected TcpConnection createConnection() {
        return new TcpConnection();
    }
}
