// Copyright 2023-2026 The NATS Authors
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
import io.nats.client.support.NatsConstants;
import io.nats.client.support.NatsUri;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

public class NatsServerPool implements ServerPool {

    protected final ReentrantLock listLock;
    protected List<ServerPoolEntry> entryList;
    protected Options options;
    protected int maxConnectAttempts;
    protected boolean hasSecureServer;
    protected NatsUri lastConnected;
    protected String defaultScheme;
    protected final List<NatsUri> rejected;
    // ADR-40, max reconnects with a per-server count: a server that leaves the list keeps its count, for when it is
    // named again, and a server that enters the list starts at the higher of that kept count and the highest count
    // in the list; at the limit it has no attempt left and does not enter. A successful connect resets the count of
    // the server it connected to only. Keyed by host and port as NatsUri.equivalent compares them; bounded, since
    // a cluster can name a new address in every INFO
    protected final Map<String, Integer> keptCounts;

    public NatsServerPool() {
        listLock = new ReentrantLock();
        entryList = new ArrayList<>(); // this gets updated occasionally
        rejected = new ArrayList<>();
        keptCounts = new LinkedHashMap<String, Integer>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
                return size() > 1024;
            }
        };
        options = Options.builder().build(); // this will get updated when initialize is called
    }

    /**
     * {@inheritDoc}
     */
    public void initialize(@NonNull Options opts) {
        // 1. Hold on to options as we need them for settings
        options = opts;

        // 2. maxConnectAttempts accounts for the first connect attempt and also reconnect attempts
        maxConnectAttempts = options.getMaxReconnect() < 0 ? Integer.MAX_VALUE : options.getMaxReconnect() + 1;

        // 3. Add all the bootstrap to the server list and prepare list for next
        //    FYI bootstrap will always have at least the default url
        listLock.lock();
        try {
            for (NatsUri optionNuri : options.getNatsServerUris()) {
                // If optionNuri is not found in the list being built, add to the list
                // If optionNuri equivalent is found in the list and is secure, just use it
                boolean notAlreadyInList = true;
                for (ServerPoolEntry entry : entryList) {
                    if (optionNuri.equivalent(entry.nuri)) {
                        if (optionNuri.isSecure()) {
                            entryList.remove(entry);
                        }
                        else {
                            notAlreadyInList = false;
                        }
                        break;
                    }
                }
                if (notAlreadyInList) {
                    if (defaultScheme == null && !optionNuri.getScheme().equals(NatsConstants.NATS_PROTOCOL)) {
                        defaultScheme = optionNuri.getScheme();
                    }
                    entryList.add(new ServerPoolEntry(optionNuri, false));
                }
            }

            // prepare list for next, intentionally within lock
            afterListChanged();
        }
        finally {
            listLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acceptDiscoveredUrls(@NonNull List<@NonNull String> discoveredServers) {
        return acceptDiscoveredUrls(discoveredServers, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acceptDiscoveredUrls(@NonNull List<@NonNull String> discoveredServers, @Nullable NatsUri origin) {
        // 1. If ignored discovered servers, don't do anything b/c never want
        //    anything but the explicit, which is already loaded.
        // 2. return false == no new servers discovered
        if (options.isIgnoreDiscoveredServers()) {
            return false;
        }

        listLock.lock();
        try {
            // 2. Build a list for discovered
            //    - since we'll need the NatsUris later
            //    - and to have a list to use to prune removed gossiped servers
            //    - an entry is host:port, as nats-server writes it, and its user info is dropped. An entry that carries
            //      a scheme, that does not parse as host:port, or whose port is out of range is left out: the connection
            //      reported it before handing the list over, and it never becomes a server
            //    - a discovered server takes the scheme of the server that reported it, so it is dialed with that
            //      server's transport and TLS. Without an origin it takes the pool's default scheme
            //    - a server the connection rejected is never supplied again, so it is left out here.
            //      The match is the exact uri: the scheme is what made it unusable, so the same
            //      host and port under another scheme is still a candidate
            String scheme = origin != null ? origin.getScheme() : (defaultScheme == null ? NatsConstants.NATS_PROTOCOL : defaultScheme);
            List<NatsUri> discovered = new ArrayList<>();
            for (String d : discoveredServers) {
                try {
                    NatsUri nuri = new NatsUri(scheme + "://" + NatsUri.discoveredHostPort(d));
                    if (!rejected.contains(nuri)) {
                        discovered.add(nuri);
                    }
                } catch (URISyntaxException ignore) {
                    // the entry is not used
                }
            }

            // 3. Start a new server list, loading in current order from the current list, and keeping
            //    - the last connected
            //    - the origin, the server whose INFO this is, which is being connected to and is not yet the last
            //      connected when its own INFO arrives (ADR-40, updating the server list: the configured servers
            //      and the server the client is connected to are kept)
            //    - all non-gossiped
            //    - any found in the new discovered list
            //      - for any new discovered, we also remove them from
            //        that list so step there are no dupes for step #4
            //      - This also maintains the Srv state of an already known discovered
            //    A gossiped server that left the list keeps its count, for when it comes back
            List<ServerPoolEntry> newEntryList = new ArrayList<>();
            for (ServerPoolEntry entry : entryList) {
                int ix = findEquivalent(discovered, entry.nuri);
                if (ix != -1 || entry.nuri.equals(lastConnected) || entry.nuri.equals(origin) || !entry.isGossiped) {
                    newEntryList.add(entry);
                    if (ix != -1) {
                        discovered.remove(ix);
                    }
                }
                else {
                    keptCounts.put(entry.nuri.hostPortKey(), entry.failedAttempts);
                }
            }

            // 4. Add all left over from the new discovered list
            //    - a discovered server given as a bare ip address gets the hostname of the origin, to be verified
            //      against and to present during the TLS handshake: the hostname the origin was configured with, or
            //      the one the origin inherited itself, so the name travels over any number of hops (ADR-40, the TLS
            //      name of a discovered server). An origin that is an ip address with no name supplies nothing.
            //      With tlsVerifyDiscoveredByIp a discovered ip address is verified against its address instead and
            //      inherits nothing. Under TLS, a discovered ip address that would get no name is skipped by the
            //      connection before this call, see NatsConnection.handleInfo
            //    - a discovered server is sent the credentials used for the server that reported it:
            //      the origin's own url user info, or the user info the origin itself inherited.
            //      The options are the fallback, so there is nothing to carry when the origin has neither.
            //      User info the entry itself carried was dropped in step 2.
            //    - ADR-40, max reconnects with a per-server count: a server that enters the list starts at the
            //      higher of the count it kept when it left and the highest count of any server in the list. At the
            //      limit it has no attempt left: it does not enter, and keeps that count. A server leaves the list
            //      only at its limit, so one that left stays out while the connection lasts
            boolean discoveryContainedUnknowns = false;
            if (!discovered.isEmpty()) {
                discoveryContainedUnknowns = true;
                String originHost = origin == null || options.isTlsVerifyDiscoveredByIp() ? null : origin.tlsHostToInherit();
                String originUserInfo = origin == null ? null : origin.getConnectUserInfo();
                int highest = 0;
                for (ServerPoolEntry entry : newEntryList) {
                    if (entry.failedAttempts > highest) {
                        highest = entry.failedAttempts;
                    }
                }
                for (NatsUri d : discovered) {
                    String key = d.hostPortKey();
                    Integer kept = keptCounts.remove(key);
                    int start = kept == null || kept < highest ? highest : kept;
                    if (start >= maxConnectAttempts) {
                        keptCounts.put(key, start);
                        continue;
                    }
                    if (originHost != null && d.hostIsIpAddress()) {
                        d = d.withTlsHost(originHost);
                    }
                    if (originUserInfo != null) {
                        d = d.withInheritedUserInfo(originUserInfo);
                    }
                    ServerPoolEntry entry = new ServerPoolEntry(d, true);
                    entry.failedAttempts = start;
                    newEntryList.add(entry);
                }
            }

            // 5. replace the list with the new one
            entryList = newEntryList;

            // 6. prepare list for next
            afterListChanged();

            // 7.
            return discoveryContainedUnknowns;
        }
        finally {
            listLock.unlock();
        }
    }

    protected void afterListChanged() {
        // 1. randomize if needed and allowed
        if (entryList.size() > 1 && !options.isNoRandomize()) {
            Collections.shuffle(entryList, ThreadLocalRandom.current());
        }

        // 2. calculate hasSecureServer and find the index of lastConnected
        hasSecureServer = false;
        int lastConnectedIx = -1;
        for (int ix = 0; ix < entryList.size(); ix++) {
            NatsUri nuri = entryList.get(ix).nuri;
            hasSecureServer |= nuri.isSecure();
            if (nuri.equals(lastConnected)) {
                lastConnectedIx = ix;
            }
        }

        // C. put the last connected server at the end of the list
        if (lastConnectedIx != -1) {
            entryList.add(entryList.remove(lastConnectedIx));
        }
    }

    @Override
    @Nullable
    public NatsUri peekNextServer() {
        listLock.lock();
        try {
            return entryList.isEmpty() ? null : entryList.get(0).nuri;
        }
        finally {
            listLock.unlock();
        }
    }

    @Override
    @Nullable
    public NatsUri nextServer() {
        // 0. The list is already managed for qualified by connectFailed
        // 1. Get the first item in the list, update it's time, add back to the end of list
        listLock.lock();
        try {
            if (!entryList.isEmpty()) {
                ServerPoolEntry entry = entryList.remove(0);
                entry.lastAttempt = System.currentTimeMillis();
                entryList.add(entry);
                return entry.nuri;
            }
            return null;
        }
        finally {
            listLock.unlock();
        }
    }

    @Deprecated // this implementation has been deprecated but implemented for completeness
    @Override
    @Nullable
    public List<String> resolveHostToIps(@NonNull String host) {
        return NatsHostResolver.resolveHostToIps(host, false, false);
    }

    @Override
    @Nullable
    public List<String> resolveHostToIps(@NonNull String host, boolean maxOneResult, boolean includeIPV6) {
        return NatsHostResolver.resolveHostToIps(host, maxOneResult, includeIPV6);
    }

    @Override
    public void connectSucceeded(@NonNull NatsUri nuri) {
        // 1. Work from the end because nextServer moved the one being tried to the end
        // 2. If we find the server in the list...
        //    2.1. remember it and
        //    2.2. reset failed attempts, of this server only (ADR-40, max reconnects); the counts kept for
        //         servers that left the list stay
        listLock.lock();
        try {
            keptCounts.remove(nuri.hostPortKey());
            for (int x = entryList.size() - 1; x >= 0 ; x--) {
                ServerPoolEntry entry = entryList.get(x);
                if (entry.nuri.equals(nuri)) {
                    lastConnected = nuri;
                    entry.failedAttempts = 0;
                    return;
                }
            }
        }
        finally {
            listLock.unlock();
        }
    }

    @Override
    public void connectFailed(@NonNull NatsUri nuri) {
        // 1. Work from the end because nextServer moved the one being tried to the end
        // 2. If we find the server in the list...
        //    2.1. increment failed attempts
        //    2.2. if failed attempts reaches max, remove it from the list, keeping its count for when it is
        //         named again; otherwise just move it to the end of the list
        listLock.lock();
        try {
            for (int x = entryList.size() - 1; x >= 0 ; x--) {
                ServerPoolEntry entry = entryList.get(x);
                if (entry.nuri.equals(nuri)) {
                    entryList.remove(x);
                    if (++entry.failedAttempts < maxConnectAttempts) {
                        entryList.add(entry);
                    }
                    else {
                        keptCounts.put(entry.nuri.hostPortKey(), entry.failedAttempts);
                    }
                    return;
                }
            }
        }
        finally {
            listLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void serverRejected(@NonNull NatsUri nuri) {
        // 1. Remember the exact uri so a later discovery does not bring it back
        // 2. Remove it from the list. Work from the end because nextServer moved the one being tried to the end
        listLock.lock();
        try {
            if (!rejected.contains(nuri)) {
                rejected.add(nuri);
            }
            for (int x = entryList.size() - 1; x >= 0; x--) {
                if (entryList.get(x).nuri.equals(nuri)) {
                    entryList.remove(x);
                    return;
                }
            }
        }
        finally {
            listLock.unlock();
        }
    }

    @Override
    @NonNull
    public List<String> getServerList() {
        listLock.lock();
        try {
            List<String> list = new ArrayList<>();
            for (ServerPoolEntry entry : entryList) {
                list.add(entry.nuri.toString());
            }
            return list;
        }
        finally {
            listLock.unlock();
        }
    }

    @Override
    public boolean hasSecureServer() {
        return hasSecureServer;
    }

    protected int findEquivalent(List<NatsUri> list, NatsUri toFind) {
        for (int i = 0; i < list.size(); i++) {
            NatsUri nuri = list.get(i);
            if (nuri.equivalent(toFind)) {
                return i;
            }
        }
        return -1;
    }
}
