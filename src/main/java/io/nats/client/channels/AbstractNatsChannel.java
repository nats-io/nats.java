// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.time.Duration;

/**
 * Delegates all operations to a wrapped {@link NatsChannel}.
 */
public abstract class AbstractNatsChannel implements NatsChannel {
    protected NatsChannel wrap;
    protected AbstractNatsChannel(NatsChannel wrap) {
        this.wrap = wrap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        return wrap.read(dst);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen() {
        return wrap.isOpen();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        wrap.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return wrap.write(srcs, offset, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSecure() {
        return wrap.isSecure();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdownInput() throws IOException {
        wrap.shutdownInput();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String transformConnectUrl(String connectUrl) {
        return wrap.transformConnectUrl(connectUrl);
    }

    /**
     * {@inheritDoc}
     */
	@Override
	public void upgradeToSecure(Duration timeout) throws IOException, GeneralSecurityException {
		wrap.upgradeToSecure(timeout);
	}
}
