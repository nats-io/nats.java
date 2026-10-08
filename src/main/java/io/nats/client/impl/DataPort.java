// Copyright 2015-2026 The NATS Authors
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

import io.nats.client.Options;
import io.nats.client.support.NatsUri;
import org.jspecify.annotations.NonNull;

import java.io.IOException;

/**
 * A data port represents the connection to the network. This could have been called
 * transport but that seemed too big a concept. This interface just allows a wrapper around
 * the core communication code.
 */
public interface DataPort {
    @Deprecated
    void connect(@NonNull String serverURI, @NonNull NatsConnection conn, long timeoutNanos) throws IOException;

    /**
     * Execute the connect
     * @param conn the NatsConnection object
     * @param uri the NatsUri to connect to
     * @param timeoutNanos the timeout
     * @throws IOException if the data port is unable to connect.
     */
    default void connect(@NonNull NatsConnection conn, @NonNull NatsUri uri, long timeoutNanos) throws IOException {
        connect(uri.toString(), conn, timeoutNanos);
    }

    /**
     * Execute the connect, also providing the uri before any hostname resolution.
     * The connection resolves hostnames to ip addresses in the resolving {@link Options.HostnameResolveMode} modes,
     * so the uri to connect to may have an ip address where the server was configured or discovered with a hostname.
     * A data port that needs the hostname, for instance to present it to the server during the TLS handshake
     * as the server name (SNI), gets it from the unresolved uri.
     * The default implementation ignores the unresolved uri and calls {@link #connect(NatsConnection, NatsUri, long)}.
     * @param conn the NatsConnection object
     * @param uri the NatsUri to connect to, possibly resolved to an ip address
     * @param unresolvedUri the NatsUri before any hostname resolution. The same object as uri when no resolution happened.
     * @param timeoutNanos the timeout
     * @throws IOException if the data port is unable to connect.
     */
    default void connect(@NonNull NatsConnection conn, @NonNull NatsUri uri, @NonNull NatsUri unresolvedUri, long timeoutNanos) throws IOException {
        connect(conn, uri, timeoutNanos);
    }

    default void afterConstruct(@NonNull Options options) {}

    /**
     * Upgrade the port to SSL. If it is already secured, this is a no-op.
     * If the data port type doesn't support SSL it should throw an exception.
     *
     * @throws IOException if the data port is unable to upgrade.
     */
    void upgradeToSecure() throws IOException;

    int read(byte[] dst, int off, int len) throws IOException;

    /**
     * NOTE: the buffer will be modified if communicating over websockets and
     * the toWrite is greater than 1432.
     * 
     * @param src output byte[]
     * @param toWrite number of bytes to write
     * @throws IOException any IO error on the underlaying connection
     */
    void write(byte[] src, int toWrite) throws IOException;

    void shutdownInput() throws IOException;

    void close() throws IOException;

    default void forceClose() throws IOException {
        close();
    }

    void flush() throws IOException;
}
