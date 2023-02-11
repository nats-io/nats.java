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

import io.nats.client.support.NatsUri;

import java.util.List;

/**
 * Allows the developer to provide the list of servers to try for connecting/reconnecting
 * IMPORTANT! ServerListProvider IS CURRENTLY EXPERIMENTAL AND SUBJECT TO CHANGE.
 */
public interface ServerListProvider {
    /**
     * Get the server list to try for connecting/reconnecting
     * @param lastConnectedServer the server that connection last connected to. Can be null.
     * @param optionsNatsUris the list of nats server uris from options. Will never be null or empty.
     * @param serverInfoConnectUrls the entire list of servers exactly as returned in the server info. Will never be null but might be empty.
     * @return the ordered server list
     */
    List<NatsUri> getServerList(NatsUri lastConnectedServer,
                            List<NatsUri> optionsNatsUris,
                            List<String> serverInfoConnectUrls);
}
