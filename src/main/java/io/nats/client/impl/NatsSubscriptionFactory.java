package io.nats.client.impl;

public interface NatsSubscriptionFactory {
    NatsSubscription createNatsSubscription(String sid, String subject, String queueName, NatsConnection connection, NatsDispatcher dispatcher);
}
