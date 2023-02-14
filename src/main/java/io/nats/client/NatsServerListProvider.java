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

    // SERVER LOGIC
    // 1. On creation, call initialize
    // 2. Connect calls getServerList and tries the whole list once
    //    2.1 Just before trying an entry, call resolveHostToIps and actually loop through that list
    //    2.2. on a successful connection, call acceptDiscoveredUrls
    // 3. ReConnect calls getServerList and tries the whole list options.maxRetries times
    //    3.1 Just before trying an entry, call resolveHostToIps and actually loop through that list
    //    3.2. on a successful connection, call acceptDiscoveredUrls

    // PROVIDER LOGIC
    // initialize - is only called once to make sure the provider has the Options
    // 1. load explicit (options) servers, no dupes

    // acceptDiscoveredUrls - is called every time the connection has new server info
    // 1. copy the internal list (lastList), so we can see if there are any new ones added
    // 2. reset list to the explicit, no dupes
    // 3. add discovered (implicit), no dupes, checking if a non-dupe was not in lastList
    // 4. return true if any discovered where not in the lastList

    // getServerList - is called in both connect and reconnect
    // 1. if there is a lastConnectedServer, remove it from the internal list
    // 2. if there are still more than 1 in the internal list and NOT options.noRandomize
    //    2.1 shuffle list
    // 3. if there is a lastConnectedServer, add it to the end of the list
    // 4. return list

    // resolveHostToIps - is called by connect/reconnect looping through the getServerList
    // 1. make a list to hold resolved (rlist)
    // 1. if options.resolveHostnames
    //    1.1 try to resolve the hostname, adding to rlist
    // 2. if rlist is empty (either since NOT options.resolveHostnames or resolution didn't resolve anything)
    //    2.1 put the original hostname into rlist
    // 3. return rlist

    private Options options;
    private final List<NatsUri> list;

    public NatsServerListProvider() {
        list = new ArrayList<>();
    }

    public void initialize(Options opts) {
        this.options = opts;
        reset();
    }

    private void reset() {
        list.clear();
        for (NatsUri nuri : options.getNatsServerUris()) {
            addNoDupes(nuri);
        }
    }

    @Override
    public boolean acceptDiscoveredUrls(List<String> discoveredServers) {
        if (options.isIgnoreDiscoveredServers()) {
            // never want anything but the explicit, which is already loaded.
            return false; // there are no new servers
        }

        List<NatsUri> lastList = new ArrayList<>(list); // copy to test for changes

        reset();

        boolean anyAdded = false;
        if (!options.isIgnoreDiscoveredServers()) {
            for (String discovered : discoveredServers) {
                try {
                    NatsUri nuri = new NatsUri(discovered);
                    if (addNoDupes(nuri) && !anyAdded) {
                        anyAdded = !lastList.contains(nuri);
                    }
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
        if (lastConnectedServer != null) {
            list.remove(lastConnectedServer);
        }
        if (list.size() > 1 && !options.isNoRandomize()) {
            Collections.shuffle(list, ThreadLocalRandom.current());
        }
        if (lastConnectedServer != null) {
            list.add(lastConnectedServer);
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

    private boolean addNoDupes(NatsUri nuri) {
        if (list.contains(nuri)) {
            return false;
        }
        list.add(nuri);
        return true;
    }
}
