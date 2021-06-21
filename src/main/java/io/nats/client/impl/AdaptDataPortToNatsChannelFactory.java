package io.nats.client.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.nats.client.Options;

/**
 * Adapter for legacy implementations of DataPort.
 * 
 * <p><b>NOTES:</b>
 * <ul>
 *   <li>The NatsConnection passed into connect() is a completely disconnected instance, and thus only the
 *       {@link NatsConnection#getOptions() getOptions()} method may be relied upon.
 *   <li>{@link DataSocker#upgradeToSecure() upgradeToSecure()} will never be called, but if there is a
 *       need to upgrade to a secure connection, this adapted DataPort instance will be wrapped automatically,
 *       and thus the TLS upgrade should happen transparently.
 * </ul>
 */
@Deprecated
public class AdaptDataPortToNatsChannelFactory implements NatsChannelFactory {
    private Supplier<DataPort> dataPortSupplier;

    public AdaptDataPortToNatsChannelFactory(Supplier<DataPort> dataPortSupplier) {
        this.dataPortSupplier = dataPortSupplier;
        /*
            void connect(String serverURI, NatsConnection conn, long timeoutNanos) throws IOException;
    void upgradeToSecure() throws IOException;

    int read(byte[] dst, int off, int len) throws IOException;

    void write(byte[] src, int toWrite) throws IOException;

    void shutdownInput() throws IOException;

    void close() throws IOException;

    void flush() throws IOException;

        */
    }
    @Override
    public NatsChannel connect(
        URI serverURI,
        Options options,
        Consumer<Exception> handleCommunicationIssue,
        Duration timeout)
        throws IOException
    {
        DataPort dataPort = dataPortSupplier.get();
        dataPort.connect(serverURI.toString(), new NatsConnection(options), timeout.toNanos());
        return new NatsChannel() {
            private boolean isOpen = true;

            @Override
            public int read(ByteBuffer dst) throws IOException {
                // FIXME: Slow!
                byte[] tmp = new byte[dst.remaining()];
                int length = dataPort.read(tmp, 0, tmp.length);
                dst.put(tmp, 0, length);
                return length;
            }

            @Override
            public boolean isOpen() {
                return isOpen;
            }

            @Override
            public void close() throws IOException {
                dataPort.close();
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                // FIXME: Slow!
                byte[] tmp = new byte[src.remaining()];
                src.get(tmp);
                dataPort.write(tmp, tmp.length);
                return tmp.length;
            }

            @Override
            public boolean isSecure() {
                return false;
            }

            @Override
            public void shutdownInput() throws IOException {
                dataPort.shutdownInput();
            }

            @Override
            public void flushOutput() throws IOException {
                dataPort.flush();
            }

        };
    }
}
