// Copyright 2026 The NATS Authors
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

import io.nats.client.support.NatsInetAddress;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class for resolving a host to IP addresses
 */
public final class NatsHostResolver {
    private NatsHostResolver() {}  /* ensures cannot be constructed */

    /**
     * Resolve a host to ip addresses
     * @param host the host
     * @param maxOneResult whether to return at max one result
     * @return the list of ips addresses or null if there were no ip addresses for the host.
     */
    public static @Nullable List<String> resolveHostToIps(@NonNull String host, boolean maxOneResult, boolean includeIPV6) {
        // 1. try to resolve the hostname, adding results to list
        List<String> results = new ArrayList<>();
        try {
            InetAddress[] addresses = NatsInetAddress.getAllByName(host);
            for (InetAddress a : addresses) {
                if (includeIPV6 || a instanceof Inet4Address) {
                    results.add(a.getHostAddress());
                }
            }
        }
        catch (UnknownHostException ignore) {
            // A user might have supplied a bad host, but the server shouldn't.
            // Either way, nothing much we can do.
            return null;
        }

        // 2. no results, return null.
        if (results.isEmpty()) {
            return null;
        }

        // 3. If results size == 1, just return
        if (results.size() == 1) {
            return results;
        }

        // 4. if maxOneResult, return the sublist
        if (maxOneResult) {
            return results.subList(0, 1);
        }

        // 5. return all results
        return results;
    }
}
