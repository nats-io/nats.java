// Copyright 2015-2018 The NATS Authors
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

package io.nats.client.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import io.nats.client.Options;

class SocketChannelDataPort implements DataPort {

    private SocketChannel socketChannel;

    public void connect(String serverURI, NatsConnection conn) throws IOException
    {
        try {
            Options options = conn.getOptions();
            SocketChannel channel = SocketChannel.open();
            channel.configureBlocking(true);

            // TODO(sasbury): Bind to local address, set other options

            URI uri = new URI(serverURI);
            channel.socket().connect(new InetSocketAddress(uri.getHost(), uri.getPort()),
                    (int) options.getConnectionTimeout().toMillis());
            channel.finishConnect();

            this.socketChannel = channel;
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Upgrade the port to SSL. If it is already secured, this is a no-op.
     * If the data port type doesn't support SSL it should throw an exception.
     */
    public void upgradeToSecure() throws IOException
    {
        // TODO(sasbury)
    }


    public int read(ByteBuffer dst) throws IOException
    {
        return this.socketChannel.read(dst);
    }


    public int write(ByteBuffer src) throws IOException
    {
        return this.socketChannel.write(src);
    }


    public void close() throws IOException
    {
        this.socketChannel.close();
    }

}