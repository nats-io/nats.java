/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying materials are made available under the terms of the MIT License (MIT) which accompanies this distribution, and is available at http://opensource.org/licenses/MIT
 */

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
