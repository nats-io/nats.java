package io.nats.service.context;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;

import static io.nats.service.ServiceUtil.toDiscoverySubject;

public class DiscoveryContext extends Context {

    private final byte[] response;

    public DiscoveryContext(Connection conn,
                            String name, String serviceName, String serviceId, byte[] response,
                            Dispatcher dispatcher, boolean internalDispatcher) {
        super(conn, toDiscoverySubject(name, serviceName, serviceId),
            dispatcher, internalDispatcher, null, false);
        this.response = response;
    }

    @Override
    protected void subOnMessage(Message msg) throws InterruptedException {
        conn.publish(msg.getReplyTo(), response);
    }
}
