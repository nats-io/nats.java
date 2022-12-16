package io.nats.service.context;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.service.Stats;

public class ServiceContext extends Context {

    private final MessageHandler serviceMessageHandler;

    public ServiceContext(Connection conn, String subject,
                          Dispatcher dispatcher, boolean internalDispatcher,
                          Stats stats, MessageHandler serviceMessageHandler) {
        super(conn, subject, dispatcher, internalDispatcher, stats, true);
        this.serviceMessageHandler = serviceMessageHandler;
    }

    @Override
    protected void subOnMessage(Message msg) throws InterruptedException {
        serviceMessageHandler.onMessage(msg);
    }
}
