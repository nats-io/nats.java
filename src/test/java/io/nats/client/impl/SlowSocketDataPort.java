package io.nats.client.impl;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("ClassEscapesDefinedScope")
public class SlowSocketDataPort extends SocketDataPort{
    public static final AtomicBoolean enabledPause = new AtomicBoolean(false);

    @Override
    public void write(byte[] src, int toWrite) throws IOException {
        if (enabledPause.get()) {
            try {
                System.out.println( "Slowing down write");
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        super.write(src, toWrite);
    }
}
