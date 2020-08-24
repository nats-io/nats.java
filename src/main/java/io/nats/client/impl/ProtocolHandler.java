package io.nats.client.impl;

public interface ProtocolHandler {

    void handleCommunicationIssue(Exception io);

    void deliverMessage(NatsMessage msg);

    void processOK();

    void processError(String errorText);

    void sendPong();

    void handlePong();

    void handleInfo(String infoJson);
}
