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

package io.nats.client.impl;

import io.nats.client.Options;
import io.nats.client.ServerPool;
import io.nats.client.support.NatsUri;

import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class NatsServerPool implements ServerPool {

    static class Srv {
        NatsUri nuri;
        boolean isGossiped;
        int failedAttempts;
        long lastAttempt;

        public Srv(NatsUri nuri, boolean isGossiped) {
            this.nuri = nuri;
            this.isGossiped = isGossiped;
        }
    }

    private final Object poolLock;
    private List<Srv> srvList;
    private Options options;
    private int maxConnectAttempts;
    private NatsUri lastConnected;

    public NatsServerPool() {
        poolLock = new Object();
    }

    /**
     * {@inheritDoc}
     */
    public void initialize(Options opts) {
        // 1. Hold on to options as we need them for settings
        this.options = opts;

        // 2. maxConnectAttempts accounts for the first connect attempt and also reconnect attempts
        maxConnectAttempts = options.getMaxReconnect() < 0 ? Integer.MAX_VALUE : options.getMaxReconnect() + 1;

        // 3. Add all the bootstrap to the server list and prepare list for next
        //    FYI bootstrap will always have at least the default url
        synchronized (poolLock) {
            srvList = new ArrayList<>();
            for (NatsUri nuri : options.getNatsServerUris()) {
                addNoDupes(srvList, nuri, false);
            }
            prepareListForNext();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acceptDiscoveredUrls(List<String> discoveredServers) {
        // 1. If ignored discovered servers, don't do anything, return false for no new servers found
        if (options.isIgnoreDiscoveredServers()) {
            // never want anything but the explicit, which is already loaded.
            return false; // there are no new servers
        }

        synchronized (poolLock) {
            // 2. Build a list for discovered
            //    - since we'll need the NatsUris later
            //    - so we have a list to check the old list against in order to prune old gossiped servers
            List<NatsUri> discovered = new ArrayList<>();
            for (String d : discoveredServers) {
                try {
                    discovered.add(new NatsUri(d));
                } catch (URISyntaxException ignore) {
                    // should never actually happen
                }
            }

            // 3. start a new server list keeping all non-gossiped and any found in the new discovered list
            List<Srv> newSrvList = new ArrayList<>();
            for (Srv srv : srvList) {
                if (!srv.isGossiped || discovered.contains(srv.nuri)) {
                    newSrvList.add(srv);
                }
            }

            // 4. Add all non-dupes from the new discovered list
            boolean anyAdded = false;
            for (NatsUri d : discovered) {
                anyAdded |= addNoDupes(newSrvList, d, true);
            }

            // 5. replace the list with the new one
            srvList = newSrvList;

            // 6. prepare list for next
            prepareListForNext();

            // 7. even if there was not any added, the isGossiped flag may have been updated
            return anyAdded;
        }
    }

    private static boolean addNoDupes(List<Srv> list, NatsUri nuri, boolean gossiped) {
        // 1. Look up the item in the list.
        // 2. If it is found, return false, this was a dupe
        // 3. else add to the list and return true, this was not a dupe
        for (Srv srv : list) {
            if (nuri.equivalent(srv.nuri)) {
                return false;
            }
        }
        list.add(new Srv(nuri, gossiped));
        return true;
    }

    private void prepareListForNext() {
        // This is about randomization and putting the last connected server at the end.
        // 1. If there is only one server there is nothing to do
        // 2. else
        //    2.1. If we are allowed to randomize, do so
        //    2.2. Find the last connected server and move it to the end.
        if (srvList.size() > 1) {
            if (!options.isNoRandomize()) {
                Collections.shuffle(srvList, ThreadLocalRandom.current());
            }
            if (lastConnected != null) {
                int lastIx = srvList.size() - 1;
                for (int x = lastIx; x >= 0 ; x--) {
                    if (srvList.get(x).nuri.equals(lastConnected)) {
                        if (x != lastIx) {
                            srvList.add(srvList.remove(x));
                        }
                        break;
                    }
                }
            }
        }
    }

    @Override
    public NatsUri nextServer() {
        // 1. Loop until you find an acceptable server or run out
        // 2. Remove the first server from the list, it will be disqualified or moved to ebd
        // 3. If qualified (failed attempts < maxReconnectAttempts)
        //    - maxConnectAttempts accounts for the first connect attempt and also reconnect attempts
        //    so if max reconnect = 5, connect can fail, then you can try 5 more times
        //    so if max reconnect = 0, connect can fail, then you can try 0 more times
        synchronized (poolLock) {
            while (srvList.size() > 0) {
                Srv srv = srvList.remove(0);
                if (srv.failedAttempts < maxConnectAttempts) {
                    srv.lastAttempt = System.currentTimeMillis();
                    srvList.add(srv);
                    return srv.nuri;
                }
            }
            return null;
        }
    }

    @Override
    public List<String> resolveHostToIps(String host) {
        // 1. if NOT options.resolveHostnames(), return empty list
        if (!options.resolveHostnames()) {
            return null;
        }

        // 2. else, try to resolve the hostname, adding results to list
        List<String> results = new ArrayList<>();
        try {
            InetAddress[] addresses = InetAddress.getAllByName(host);
            for (InetAddress a : addresses) {
                results.add(a.getHostAddress());
            }
        }
        catch (UnknownHostException ignore) {
            // A user might have supplied a bad host, but the server shouldn't.
            // Either way, nothing much we can do.
        }

        // 3. no results, return null.
        if (results.size() == 0) {
            return null;
        }

        // 4. if results has more than 1 and allowed to randomize, shuffle the list
        if (results.size() > 1 && !options.isNoRandomize()) {
            Collections.shuffle(results, ThreadLocalRandom.current());
        }
        return results;
    }

    @Override
    public void connectSucceeded(NatsUri nuri) {
        // 1. Work from the end because nextServer moved the one being tried to the end
        // 2. If we find the server in the list...
        //    2.1. remember it and
        //    2.2. reset failed attempts
        synchronized (poolLock) {
            for (int x = srvList.size() - 1; x >= 0 ; x--) {
                Srv srv = srvList.get(x);
                if (srv.nuri.equals(nuri)) {
                    lastConnected = nuri;
                    srv.failedAttempts = 0;
                    return;
                }
            }
        }
    }

    @Override
    public void connectFailed(NatsUri nuri) {
        // 1. Work from the end because nextServer moved the one being tried to the end
        // 2. If we find the server in the list...
        //    2.1. increment failed attempts
        //    2.2. if failed attempts reaches max, remove it from the list
        synchronized (poolLock) {
            for (int x = srvList.size() - 1; x >= 0 ; x--) {
                Srv srv = srvList.get(x);
                if (srv.nuri.equals(nuri)) {
                    ++srv.failedAttempts;
                    return;
                }
            }
        }
    }

    @Override
    public List<String> getServerList() {
        synchronized (poolLock) {
            List<String> list = new ArrayList<>();
            for (Srv srv : srvList) {
                list.add(srv.nuri.toString());
            }
            return list;
        }
    }
}
