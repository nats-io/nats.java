package io.nats.client.impl;

import io.nats.client.support.NatsUri;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("ClassEscapesDefinedScope") // NatsConnection
public class SimulateSocketDataPortException extends SocketDataPort {
    public static AtomicBoolean THROW_ON_CONNECT = new AtomicBoolean();

    @Override
    public void connect(@NonNull NatsConnection conn, @NonNull NatsUri nuri, long timeoutNanos) throws IOException {
        if (THROW_ON_CONNECT.get()) {
            SimulateSocketDataPortException.THROW_ON_CONNECT.set(false);
            throw new ConnectException("Simulated Exception");
        }
        super.connect(conn, nuri, timeoutNanos);
    }
}
