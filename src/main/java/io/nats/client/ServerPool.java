// Copyright 2023 The NATS Authors
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

package io.nats.client;

import io.nats.client.support.NatsUri;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.List;

/**
 * Allows the developer to provide the list of servers to try for connecting/reconnecting
 */
public interface ServerPool {

    /**
     * Initialize the pool.
     * @param opts the options that was used to make the connection is supplied
     */
    void initialize(@NonNull Options opts);

    /**
     * When the connection received discovered servers (ServerInfo.getConnectURLs)
     * it passes them on to the provider for later use
     * @param discoveredServers the list of discovered servers.
     * @return true if there were any unknown servers provided
     */
    boolean acceptDiscoveredUrls(@NonNull List<@NonNull String> discoveredServers);

    /**
     * Just take a peek at the next server without doing any processing.
     * @return the next server Nuri or null if the pool is empty.
     */
    @Nullable
    NatsUri peekNextServer();

    /**
     * Get the next server to try to connect to.
     * @return the next server Nuri or null if the pool is empty.
     */
    @Nullable
    NatsUri nextServer();

    /**
     * Resolve a host name to an ip address
     * @param host the host to resolve
     * @return a list of resolved hosts. Can be null.
     */
    @Nullable
    List<String> resolveHostToIps(@NonNull String host);

    /**
     * Indicate that the connection to this NatsUri succeeded.
     * @param nuri should match the NatsUri given by nextServer
     */
    void connectSucceeded(@NonNull NatsUri nuri);

    /**
     * Indicate that the connection to this NatsUri failed.
     * @param nuri should match the NatsUri given by nextServer
     */
    void connectFailed(@NonNull NatsUri nuri);

    /**
     * Get the list of servers known to the pool. Purely informational
     * @return the list of servers
     */
    @NonNull
    List<String> getServerList();

    /**
     * Whether the pool has any server with a secure scheme
     * @return the flag
     */
    boolean hasSecureServer();
}
