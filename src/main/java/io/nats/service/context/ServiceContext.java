package io.nats.service.context;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.service.StatsResponse;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class ServiceContext extends Context {

    private final MessageHandler messageHandler;

    public ServiceContext(Connection conn, String subject,
                          Dispatcher dispatcher, boolean internalDispatcher,
                          StatsResponse statsResponse, MessageHandler messageHandler) {
        super(conn, subject, dispatcher, internalDispatcher, statsResponse, true);
        this.messageHandler = messageHandler;
    }

    @Override
    protected void subOnMessage(Message msg) throws InterruptedException {
        messageHandler.onMessage(msg);
    }
}
