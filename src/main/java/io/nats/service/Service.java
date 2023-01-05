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

package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.support.JsonSerializable;
import io.nats.service.context.Context;
import io.nats.service.context.DiscoveryContext;
import io.nats.service.context.ServiceContext;
import io.nats.service.context.StatsContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.service.ServiceUtil.*;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class Service {
    private final Connection conn;
    private final String id;
    private final Function<String, StatsData> statsDataDecoder;
    private final Duration drainTimeout;

    private final InfoResponse infoResponse;
    private final SchemaResponse schemaResponse;
    private final StatsResponse statsResponse;
    private final List<Context> discoveryContexts;
    private final List<Context> serviceContexts;

    private final Object stopLock;
    private CompletableFuture<Boolean> doneFuture;

    Service(ServiceBuilder builder) {
        id = new io.nats.client.NUID().next();
        conn = builder.conn;
        statsDataDecoder = builder.statsDataDecoder;
        drainTimeout = builder.drainTimeout;
        infoResponse = new InfoResponse(id, builder.name, builder.version, builder.description, builder.rootSubject);
        schemaResponse = new SchemaResponse(id, builder.name, builder.version, builder.schemaRequest, builder.schemaResponse);

        // User may provide 0 or more dispatchers, just use theirs when provided else use one we make
        boolean internalDiscovery = builder.dUserDiscovery == null;
        boolean internalService = builder.dUserService == null;
        Dispatcher dDiscovery = internalDiscovery ? conn.createDispatcher() : builder.dUserDiscovery;
        Dispatcher dService = internalService ? conn.createDispatcher() : builder.dUserService;

        // do the service first in case the server feels like rejecting the subject
        serviceContexts = new ArrayList<>();
        statsResponse = new StatsResponse(id, builder.name, builder.version);
        if (builder.endpointMap.size() == 0) {
            serviceContexts.add(new ServiceContext(conn, infoResponse.getSubject(), dService, internalService, statsResponse, builder.rootMessageHandler));
        }
        else {
            for (String endpoint : builder.endpointMap.keySet()) {
                String eSubject = infoResponse.getSubject() + DOT + endpoint;
                serviceContexts.add(new ServiceContext(conn, eSubject, dService, internalService, statsResponse, builder.endpointMap.get(endpoint)));
            }
        }

        discoveryContexts = new ArrayList<>();
        addDiscoveryContexts(PING, new PingResponse(id, builder.name, builder.version), dDiscovery, internalDiscovery);
        addDiscoveryContexts(INFO, infoResponse, dDiscovery, internalDiscovery);
        addDiscoveryContexts(SCHEMA, schemaResponse, dDiscovery, internalDiscovery);
        addStatsContexts(dDiscovery, internalDiscovery, statsResponse, builder.statsDataSupplier);

        stopLock = new Object();
    }

    public CompletableFuture<Boolean> startService() {
        doneFuture = new CompletableFuture<>();
        for (Context ctx : serviceContexts) {
            ctx.start();
        }
        for (Context ctx : discoveryContexts) {
            ctx.start();
        }
        statsResponse.start();
        return doneFuture;
    }

    @Override
    public String toString() {
        return "Service" + infoResponse.toJson();
    }

    public void stop() {
        stop(true, null);
    }

    public void stop(Throwable t) {
        stop(true, t);
    }

    public void stop(boolean drain) {
        stop(drain, null);
    }

    public void stop(boolean drain, Throwable t) {
        synchronized (stopLock) {
            if (!doneFuture.isDone()) {
                List<Dispatcher> internals = new ArrayList<>();

                if (drain) {
                    List<CompletableFuture<Boolean>> futures = new ArrayList<>();

                    for (Context c : serviceContexts) {
                        drain(c, internals, futures);
                    }

                    for (Context c : discoveryContexts) {
                        drain(c, internals, futures);
                    }

                    // make sure drain is done before closing dispatcher
                    long drainTimeoutMillis = drainTimeout.toMillis();
                    for (CompletableFuture<Boolean> f : futures) {
                        try {
                            f.get(drainTimeoutMillis, TimeUnit.MILLISECONDS);
                        }
                        catch (Exception ignore) {
                            // don't care if it completes successfully or not, just that it's done.
                        }
                    }
                }

                // close all internal dispatchers
                for (Dispatcher d : internals) {
                    conn.closeDispatcher(d);
                }

                // ok we are done
                if (t == null) {
                    doneFuture.complete(true);
                }
                else {
                    doneFuture.completeExceptionally(t);
                }
            }
        }
    }

    private void drain(Context c, List<Dispatcher> internals, List<CompletableFuture<Boolean>> futures) {
        if (c.isInternalDispatcher()) {
            internals.add(c.getDispatcher());
            try {
                futures.add(c.getDispatcher().drain(drainTimeout));
            }
            catch (Exception e) { /* nothing I can really do, we are stopping anyway */ }
        }
        else {
            try {
                futures.add(c.getSub().drain(drainTimeout));
            }
            catch (Exception e) { /* nothing I can really do, we are stopping anyway */ }
        }
    }

    public void reset() {
        serviceContexts.get(0).getStats().reset();
    }

    public String getId() {
        return infoResponse.getServiceId();
    }

    public InfoResponse getInfo() {
        return infoResponse;
    }

    public SchemaResponse getSchemaResponse() {
        return schemaResponse;
    }

    public StatsResponse getStats() {
        return serviceContexts.get(0).getStats().copy(statsDataDecoder);
    }

    private void addDiscoveryContexts(String action, JsonSerializable js, Dispatcher dispatcher, boolean internalDispatcher) {
        discoveryContexts.add(new DiscoveryContext(conn, action, null, null, js, dispatcher, internalDispatcher));
        discoveryContexts.add(new DiscoveryContext(conn, action, infoResponse.getName(), null, js, dispatcher, internalDispatcher));
        discoveryContexts.add(new DiscoveryContext(conn, action, infoResponse.getName(), id, js, dispatcher, internalDispatcher));
    }

    private void addStatsContexts(Dispatcher dispatcher, boolean internalDispatcher, StatsResponse statsResponse, Supplier<StatsData> sds) {
        discoveryContexts.add(new StatsContext(conn, null, null, dispatcher, internalDispatcher, statsResponse, sds));
        discoveryContexts.add(new StatsContext(conn, infoResponse.getName(), null, dispatcher, internalDispatcher, statsResponse, sds));
        discoveryContexts.add(new StatsContext(conn, infoResponse.getName(), id, dispatcher, internalDispatcher, statsResponse, sds));
    }
}
