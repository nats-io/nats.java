package io.nats.client.channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.time.Duration;

public class BaseNatsChannel implements NatsChannel {
    private boolean isOpen = true;

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return -1;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return -1;
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public void shutdownInput() throws IOException {
    }

    @Override
    public String transformConnectUrl(String connectUrl) {
        return connectUrl;
    }

	@Override
	public void upgradeToSecure(Duration timeout) throws IOException, GeneralSecurityException {
	}
    
}
