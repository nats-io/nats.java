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

import io.nats.client.*;
import io.nats.service.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Service {

    private static final String PING = "PING";
    private static final String INFO = "INFO";
    private static final String SCHEMA = "SCHEMA";
    private static final String STATS = "STATS";
    private static final String SRV_PREFIX = "$SRV.";
    public static final String QGROUP = "q";
    public static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(10);

    private final Connection conn;
    private final String id;
    private final ServiceDescriptor sd;
    private final List<Context> discoveryContexts;
    private final Context serviceContext;
    private final MessageHandler userMessageHandler;
    private final CompletableFuture<Boolean> doneFuture;

    public Service(Connection conn, ServiceDescriptor descriptor,
                   MessageHandler userMessageHandler) {
        this(conn, descriptor, userMessageHandler, null, null, null, null, null);
    }

    public Service(Connection conn, ServiceDescriptor descriptor, MessageHandler userMessageHandler,
                   Dispatcher dUserPing, Dispatcher dUserInfo, Dispatcher dUserSchema, Dispatcher dUserStats, Dispatcher dUserService) {
        id = io.nats.client.NUID.nextGlobal();
        this.conn = conn;
        this.sd = descriptor;
        this.userMessageHandler = userMessageHandler;

        // User may provide 0 or more dispatchers, just use theirs when provided else use one we make
        Dispatcher dInternal = null;
        if (dUserPing == null || dUserInfo == null || dUserSchema == null || dUserStats == null) {
            dInternal =  conn.createDispatcher();
        }

        Dispatcher dPing = getDispatcher(dUserPing, dInternal);
        Dispatcher dInfo = getDispatcher(dUserInfo, dInternal);
        Dispatcher dSchema = getDispatcher(dUserSchema, dInternal);
        Dispatcher dStats = getDispatcher(dUserStats, dInternal);
        Dispatcher dService = getDispatcher(dUserService, null);

        discoveryContexts = new ArrayList<>();
        addDiscoveryContexts(PING, new PingResponse(id, this.sd.name).serialize(), dPing, dUserPing == null);
        addDiscoveryContexts(INFO, new InfoResponse(id, this.sd).serialize(), dInfo, dUserInfo == null);
        addDiscoveryContexts(SCHEMA, new SchemaResponse(id, this.sd).serialize(), dSchema, dUserSchema == null);
        addStatsContexts(dStats, dUserStats == null);

        serviceContext = new ServiceContext(this.sd.subject, dService, dUserService == null);
        serviceContext.sub = dService.subscribe(this.sd.subject, QGROUP, serviceContext::onMessage);

        doneFuture = new CompletableFuture<>();
    }

    private Dispatcher getDispatcher(Dispatcher userDispatcher, Dispatcher dInternal) {
        if (userDispatcher == null) {
            return dInternal == null ? conn.createDispatcher() : dInternal;
        }
        return userDispatcher;
    }

    private static String toSubject(String action) {
        return SRV_PREFIX + action;
    }

    public void stop() {
        stop(null);
    }

    private final Object stopLock = new Object();

    public void stop(Throwable t) {
        synchronized (stopLock) {
            if (!doneFuture.isDone()) {
                // drain all dispatchers
                List<Dispatcher> internals = new ArrayList<>();
                List<CompletableFuture<Boolean>> futures = new ArrayList<>();

                drain(serviceContext, internals, futures);

                for (Context c : discoveryContexts) {
                    drain(c, internals, futures);
                }

                // make sure drain is done before closing dispatcher
                for (CompletableFuture<Boolean> f : futures) {
                    f.join(); // don't care if it completes successfully or not, just that it's done.
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

    private static void drain(Context c, List<Dispatcher> internals, List<CompletableFuture<Boolean>> futures) {
        if (c.internalDispatcher) {
            internals.add(c.dispatcher);
            try {
                futures.add(c.dispatcher.drain(DRAIN_TIMEOUT));
            }
            catch (Exception e) { /* nothing I can really do, we are stopping anyway */ }
        }
        else {
            try {
                futures.add(c.sub.drain(DRAIN_TIMEOUT));
            }
            catch (Exception e) { /* nothing I can really do, we are stopping anyway */ }
        }
    }

    public void reset() {
        serviceContext.stats.reset();
    }

    public void resetDiscovery() {
        for (Context c : discoveryContexts) {
            c.stats.reset();
        }
    }

    public EndpointStats stats() {
        return serviceContext.stats;
    }

    public List<EndpointStats> discoveryStats() {
        List<EndpointStats> list = new ArrayList<>();
        for (Context c : discoveryContexts) {
            list.add(c.stats);
        }
        return list;
    }

    public CompletableFuture<Boolean> done() {
        return doneFuture;
    }

    private void addDiscoveryContexts(String action, byte[] response, Dispatcher dispatcher, boolean internalDispatcher) {
        finishAddDiscoveryContext(dispatcher,
            new DiscoveryContext(action, response, dispatcher, internalDispatcher));

        finishAddDiscoveryContext(dispatcher,
            new DiscoveryContext(action + "." + sd.name, response, dispatcher, internalDispatcher));

        finishAddDiscoveryContext(dispatcher,
            new DiscoveryContext(action + "." + sd.name + "." + id, response, dispatcher, internalDispatcher));
    }

    private void addStatsContexts(Dispatcher dispatcher, boolean internalDispatcher) {
        finishAddDiscoveryContext(dispatcher,
            new StatsContext(STATS, dispatcher, internalDispatcher));

        finishAddDiscoveryContext(dispatcher,
            new StatsContext(STATS + "." + sd.name, dispatcher, internalDispatcher));

        finishAddDiscoveryContext(dispatcher,
            new StatsContext(STATS + "." + sd.name + "." + id, dispatcher, internalDispatcher));
    }

    private void finishAddDiscoveryContext(Dispatcher dispatcher, final Context ctx) {
        ctx.sub = dispatcher.subscribe(ctx.subject, ctx::onMessage);
        discoveryContexts.add(ctx);
    }

    abstract class Context {
        String subject;
        Subscription sub;
        EndpointStats stats;
        Dispatcher dispatcher;
        boolean internalDispatcher;

        public Context(String subject, Dispatcher dispatcher, boolean internalDispatcher) {
            this.subject = subject;
            this.dispatcher = dispatcher;
            this.internalDispatcher = internalDispatcher;
            this.stats = new EndpointStats(subject);
        }

        protected abstract void subOnMessage(Message msg) throws InterruptedException;

        public void onMessage(Message msg) throws InterruptedException {
            long requests = 1;
            long start = 0;
            try {
                requests = stats.numRequests.incrementAndGet();
                start = System.nanoTime();
                subOnMessage(msg);
            }
            catch (Throwable t) {
                stats.numErrors.incrementAndGet();
                stats.lastError = t.toString();
                Service.this.stop(t);
            }
            finally {
                long elapsed = System.nanoTime() - start;
                long total = stats.totalProcessingTime.addAndGet(elapsed);
                stats.averageProcessingTime.set(total / requests);
            }
        }

    }

    class ServiceContext extends Context {
        public ServiceContext(String subject, Dispatcher dispatcher, boolean internalDispatcher) {
            super(subject, dispatcher, internalDispatcher);
        }

        @Override
        protected void subOnMessage(Message msg) throws InterruptedException {
            userMessageHandler.onMessage(msg);
        }
    }

    class DiscoveryContext extends Context {
        byte[] response;

        public DiscoveryContext(String subject, byte[] response, Dispatcher dispatcher, boolean internalDispatcher) {
            super(toSubject(subject), dispatcher, internalDispatcher);
            this.response = response;
        }

        @Override
        protected void subOnMessage(Message msg) {
            conn.publish(msg.getReplyTo(), response);
        }
    }

    class StatsContext extends Context {
        public StatsContext(String subject, Dispatcher dispatcher, boolean internalDispatcher) {
            super(toSubject(subject), dispatcher, internalDispatcher);
        }

        @Override
        protected void subOnMessage(Message msg) {
            StatsRequest rq = new StatsRequest(msg.getData());
            if (rq.isInternal()) {
                List<EndpointStats> list = discoveryStats();
                list.add(stats);
                conn.publish(msg.getReplyTo(),
                    new StatsResponse(id, sd, list).serialize());
            }
            else {
                conn.publish(msg.getReplyTo(),
                    new StatsResponse(id, sd, stats).serialize());
            }
        }
    }
}
