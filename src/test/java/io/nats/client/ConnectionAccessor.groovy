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

package io.nats.client

import io.nats.client.Nats.ConnState
import io.nats.client.ConnectionImpl.Srv;
import groovy.transform.CompileStatic

import java.util.concurrent.ExecutorService

@CompileStatic
public class ConnectionAccessor {
    public static void setState(ConnectionImpl conn, ConnState newState) {
        conn.@status = newState;
    }

    public static void setSrvPool(ConnectionImpl conn, List<Srv> pool) {
        conn.@srvPool = pool;
    }

    public static List<Srv> getSrvPool(ConnectionImpl conn) {
        return conn.@srvPool;
    }

    public static Map<String, URI> getUrls(ConnectionImpl conn) {
        return conn.@urls;
    }

    public static void setParser(ConnectionImpl conn, Parser parser) {
        conn.@parser = parser;
    }

    public static Parser getParser(ConnectionImpl conn) {
        return conn.@parser;
    }

    public static ExecutorService getExec(ConnectionImpl conn) {
        return conn.@exec;
    }

    public static ExecutorService getSubExec(ConnectionImpl conn) {
        return conn.@subexec;
    }

    public static ExecutorService getCbExec(ConnectionImpl conn) {
        return conn.@cbexec;
    }

    public static void setOptions(ConnectionImpl conn, Options options) {
        conn.@opts = options;
    }

}
