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

import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class NatsServerListProvider implements ServerListProvider {

    // behavior is to
    // 1. add the configured servers on construction
    // 2. optionally add the discovered servers (options default is to include them)
    // 3. optionally randomize the servers (options default is to randomize)
    // 4. on randomize, move the current server to the bottom

    private Options options;
    private final List<NatsUri> list;

    public NatsServerListProvider() {
        list = new ArrayList<>();
    }

    public void initialize(Options opts) {
        this.options = opts;
        for (NatsUri nuri : options.getNatsServerUris()) {
            add(nuri);
        }
    }

    @Override
    public boolean acceptDiscoveredUrls(List<String> discoveredServers) {
        boolean anyAdded = false;
        if (!options.isIgnoreDiscoveredServers()) {
            // TODO PRUNE ???
            for (String discovered : discoveredServers) {
                try {
                    anyAdded |= add(new NatsUri(discovered));
                }
                catch (URISyntaxException ignore) {
                    // should never actually happen
                }
            }
        }
        return anyAdded;
    }

    @Override
    public List<NatsUri> getServerList(NatsUri lastConnectedServer) {
        if (list.size() > 1) {
            boolean removed = false;
            if (lastConnectedServer != null) {
                removed = list.remove(lastConnectedServer);
            }
            if (!options.isNoRandomize() && list.size() > 1) {
                Collections.shuffle(list, ThreadLocalRandom.current());
            }
            if (removed) {
                list.add(lastConnectedServer);
            }
        }
        return new ArrayList<>(list); // return a copy
    }

    @Override
    public List<String> resolveHostToIps(String host) {
        List<String> resolved = new ArrayList<>();
        if (options.resolveHostnames()) {
            try {
                InetAddress[] addresses = InetAddress.getAllByName(host);
                for (InetAddress a : addresses) {
                    resolved.add(a.getHostAddress());
                }
            }
            catch (UnknownHostException ignore) {
                // A user might have supplied a bad host, but the server shouldn't.
                // Either way, nothing much we can do.
            }
        }

        if (resolved.size() == 0){
            resolved.add(host);
        }
        return resolved;
    }

    private boolean add(NatsUri nuri) {
        if (list.contains(nuri)) {
            return false;
        }
        list.add(nuri);
        return true;
    }
}
