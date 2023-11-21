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

package io.nats.examples.testapp;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;

public class UiConnectionListener implements ConnectionListener {

    String id;

    public UiConnectionListener(String id) {
        this.id = id;
    }

    @Override
    public void connectionEvent(Connection conn, Events type) {
        Ui.controlMessage(id, "Connection: " + conn.getServerInfo().getPort() + " " + type.name().toLowerCase());

        switch (type) {
            case CONNECTED: connected(conn); break;
            case CLOSED: closed(conn); break;
            case DISCONNECTED: disconnected(conn); break;
            case RECONNECTED: reconnected(conn); break;
            case RESUBSCRIBED: resubscribed(conn); break;
            case DISCOVERED_SERVERS: discoveredServers(conn); break;
            case LAME_DUCK: lameDuck(conn); break;
        }
    }

    public void connected(Connection conn) {}
    public void closed(Connection conn) {}
    public void disconnected(Connection conn) {}
    public void reconnected(Connection conn) {}
    public void resubscribed(Connection conn) {}
    public void discoveredServers(Connection conn) {}
    public void lameDuck(Connection conn) {}
}
