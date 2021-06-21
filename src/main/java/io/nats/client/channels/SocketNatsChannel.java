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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.time.Duration;

import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;
import static io.nats.client.Options.DEFAULT_PORT;

/**
 * TCP socket subtransport for NATS.
 */
public class SocketNatsChannel implements NatsChannel {
    private static NatsChannelFactory.Chain FACTORY = new Factory();

    private static class Factory implements NatsChannelFactory.Chain {
        private Factory() {}
    
        @Override
        public NatsChannel connect(
            URI serverURI,
            Duration timeout) throws IOException
        {
            return SocketNatsChannel.connect(serverURI, timeout);
        }
    }

    /**
     * @return the NatsChannelFactory.Chain implementation
     *    for a TCP socket.
     */
    public static NatsChannelFactory.Chain factory() {
        return FACTORY;
    }

    private static NatsChannel connect(
        URI uri,
        Duration timeoutDuration)
        throws IOException
    {
        // Code copied from SocketDataPort.connect():
        try {
            String host = uri.getHost();
            int port = uri.getPort();
            if (port < 0) {
                port = DEFAULT_PORT;
            }

            SocketChannel socket = SocketChannel.open();
            socket.setOption(TCP_NODELAY, true);
            socket.setOption(SO_RCVBUF, 2 * 1024 * 1024);
            socket.setOption(SO_SNDBUF, 2 * 1024 * 1024);

            connectWithTimeout(socket, new InetSocketAddress(host, port), timeoutDuration);

            return new SocketNatsChannel(socket);
        } catch (ConnectTimeoutException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private static void connectWithTimeout(SocketChannel socket, SocketAddress address, Duration timeout) throws IOException {
        Selector selector = Selector.open();
        try {
            socket.configureBlocking(false);
            socket.register(selector, SelectionKey.OP_CONNECT);
            if (socket.connect(address)) {
                return;
            }
            if (0 == selector.select(timeout.toMillis())) {
                socket.close();
                throw new ConnectTimeoutException("Unable to connect within " + timeout);
            }
            boolean finished = socket.finishConnect();
            assert finished;
        } finally {
            selector.close();
            if (socket.isOpen()) {
                socket.configureBlocking(true);
            }
        }
    }

    private SocketChannel socket;

    private SocketNatsChannel(SocketChannel socket) throws IOException {
        this.socket = socket;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return  socket.read(dst);
    }

    @Override
    public boolean isOpen() {
        return socket.isOpen();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return socket.write(srcs, offset, length);
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public void shutdownInput() throws IOException {
        socket.shutdownInput();
    }

    @Override
    public String transformConnectUrl(String connectUrl) {
        return connectUrl;
    }

	@Override
	public void upgradeToSecure(Duration timeout) throws IOException {
        throw new UnsupportedOperationException("Attempt to upgradeToSecure when TLS is not configured in client");
	}
}
