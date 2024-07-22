// Copyright 2022 The NATS Authors
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

package io.nats.client.utils;

import io.nats.client.Options;
import io.nats.client.ServerPool;
import io.nats.client.support.NatsUri;

import java.util.List;

/**
 * This class is simply to have a concrete implementation to test setting properties and calling the builder in Options
 */
public class CoverageServerPool implements ServerPool {
    @Override
    public void initialize(Options opts) {
    }

    @Override
    public boolean acceptDiscoveredUrls(List<String> discoveredServers) {
        return false;
    }

    @Override
    public NatsUri peekNextServer() {
        return null;
    }

    @Override
    public NatsUri nextServer() {
        return null;
    }

    @Override
    public List<String> resolveHostToIps(String host) {
        return null;
    }

    @Override
    public void connectSucceeded(NatsUri nuri) {
    }

    @Override
    public void connectFailed(NatsUri nuri) {
    }

    @Override
    public List<String> getServerList() {
        return null;
    }

    @Override
    public boolean hasSecureServer() {
        return false;
    }

    @Override
    public void shuffle() {
    }
}
