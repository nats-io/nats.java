// Copyright 2015-2018 The NATS Authors
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

import java.io.IOException;

import io.nats.client.impl.NatsImpl;

public class Nats {

    public static final String CLIENT_VERSION = "2.0.0";

    // TODO(sasbury): can we support TLS just with URL protocol
    public static Connection connect() throws IOException {
        Options options = new Options.Builder().server(Options.DEFAULT_URL).build();
        return createConnection(options);
    }

    public static Connection connect(String url) throws IOException {
        Options options = new Options.Builder().server(url).build();
        return createConnection(options);
    }

    /**
     * 
     * Options can be used to set the server URL, or multiple URLS,
     * callback handlers for various errors, and connection events.
     */
    public static Connection connect(Options options) throws IOException {
        return createConnection(options);
    }

    private static Connection createConnection(Options options) throws IOException {
        return NatsImpl.createConnection(options);
    }

    private Nats() {
        throw new UnsupportedOperationException("Nats is a static class");
    }
}