// Copyright 2020 The NATS Authors
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

import java.util.List;

/**
 * Allows the developer to provide the list of servers to try for connecting/reconnecting
 */
public interface ServerListProvider {
    /**
     * Get the ordered server list to try for connecting/reconnecting
     * @param currentServer the server that connection is currently connected to. May be null.
     * @param optionsServersRefined the list of normalized server URIs given to the options
     * @param optionsServersUnprocessed the list of server urls exactly how they were given to the options
     * @param discoveredServersRefined the list of normalized servers from the server info
     * @param discoveredServersUnprocessed the entire list of servers exactly as returned in the server info
     * @return the ordered server list
     */
    List<String> getServerList(String currentServer,
                               List<String> optionsServersRefined,
                               List<String> optionsServersUnprocessed,
                               List<String> discoveredServersRefined,
                               List<String> discoveredServersUnprocessed);
}
