package io.nats.client.impl;

import java.io.IOException;

public interface NatsConnectionWriter {
    int writeMessages();
}
