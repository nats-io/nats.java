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
    // 1. add the configured servers
    // 2. optionally add the discovered servers (options default is to get them)
    // 3. optionally randomize the servers (options default is to randomize)
    // 4. on randomize, move the current server to the bottom

    private final Options options;

    public NatsServerListProvider(Options opts) {
        this.options = opts;
    }

    @Override
    public List<NatsUri> getServerList(NatsUri lastConnectedServer, List<NatsUri> optionsNatsUris, List<String> serverInfoConnectUrls) {
        List<NatsUri> list = new ArrayList<>();

        for (NatsUri nuri : optionsNatsUris) {
            addUnique(list, nuri);
        }

        if (!options.isIgnoreDiscoveredServers()) {
            for (String discovered : serverInfoConnectUrls) {
                try {
                    NatsUri nuri = new NatsUri(discovered);
                    addUnique(list, nuri);
                }
                catch (URISyntaxException ignore) {}
            }
        }

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

        return list;
    }

    private void addUnique(List<NatsUri> list, NatsUri nuri) {
        if (options.resolveHostnames()) {
            if (!nuri.hostIsIpAddress()) {
                try {
                    InetAddress[] addrs = InetAddress.getAllByName(nuri.getHost());
                    for (InetAddress a : addrs) {
                        try {
                            NatsUri rehost = nuri.reHost(a.getHostAddress());
                            if (!list.contains(rehost)) {
                                list.add(rehost);
                            }
                        }
                        catch (URISyntaxException e) {
                            // just don't fail here
                        }
                    }
                }
                catch (UnknownHostException ignore) {
                    // just don't fail here
                }
            }
        }
        else if (!list.contains(nuri)) {
            list.add(nuri);
        }
    }
}
