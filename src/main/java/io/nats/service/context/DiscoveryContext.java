package io.nats.service.context;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.support.JsonSerializable;

import static io.nats.service.ServiceUtil.toDiscoverySubject;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class DiscoveryContext extends Context {

    private final byte[] response;

    public DiscoveryContext(Connection conn,
                            String name, String serviceName, String serviceId, JsonSerializable js,
                            Dispatcher dispatcher, boolean internalDispatcher) {
        super(conn, toDiscoverySubject(name, serviceName, serviceId),
            dispatcher, internalDispatcher, null, false);
        this.response = js.serialize();
    }

    @Override
    protected void subOnMessage(Message msg) throws InterruptedException {
        conn.publish(msg.getReplyTo(), response);
    }
}
