package io.nats.client;

class TcpConnectionFactory implements TransportConnectionFactory {

    public TcpConnection createConnection() {
        return new TcpConnection();
    }
}
