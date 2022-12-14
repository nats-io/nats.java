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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.nats.service.ServiceUtil.*;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class Service {
    private final Connection conn;
    private final String id;
    private final ServiceDescriptor sd;
    private final List<Context> discoveryContexts;
    private final Context serviceContext;
    private final MessageHandler userMessageHandler;
    private final CompletableFuture<Boolean> doneFuture;
    private final Object stopLock = new Object();
    private Duration drainTimeout = DEFAULT_DRAIN_TIMEOUT;

    public Service(Connection conn, ServiceDescriptor descriptor,
                   MessageHandler userMessageHandler) {
        this(conn, descriptor, userMessageHandler, null, null, null, null, null);
    }
    public Service(Connection conn, ServiceDescriptor descriptor, MessageHandler userMessageHandler, Dispatcher dDiscoveryServices, Dispatcher dUserService) {
        this(conn, descriptor, userMessageHandler, dDiscoveryServices, dDiscoveryServices, dDiscoveryServices, dDiscoveryServices, dUserService);
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

        serviceContext = new ServiceContext(this.sd.name, this.sd.subject, dService, dUserService == null);
        serviceContext.sub = dService.subscribe(this.sd.subject, QGROUP, serviceContext::onMessage);

        doneFuture = new CompletableFuture<>();
    }

    public Service setDrainTimeout(Duration drainTimeout) {
        this.drainTimeout = drainTimeout;
        return this;
    }

    public String getId() {
        return id;
    }

    public ServiceDescriptor getServiceDescriptor() {
        return sd;
    }

    @Override
    public String toString() {
        return "Service{" +
            "id='" + id + '\'' +
            ", name='" + sd.name + '\'' +
            ", description='" + sd.description + '\'' +
            ", version='" + sd.version + '\'' +
            ", subject='" + sd.subject + '\'' +
            ", schemaRequest='" + sd.schemaRequest + '\'' +
            ", schemaResponse='" + sd.schemaResponse + '\'' +
            '}';
    }

    private Dispatcher getDispatcher(Dispatcher userDispatcher, Dispatcher dInternal) {
        if (userDispatcher == null) {
            return dInternal == null ? conn.createDispatcher() : dInternal;
        }
        return userDispatcher;
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

                    drain(serviceContext, internals, futures);

                    for (Context c : discoveryContexts) {
                        drain(c, internals, futures);
                    }

                    // make sure drain is done before closing dispatcher
                    for (CompletableFuture<Boolean> f : futures) {
                        f.join(); // don't care if it completes successfully or not, just that it's done.
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
        if (c.internalDispatcher) {
            internals.add(c.dispatcher);
            try {
                futures.add(c.dispatcher.drain(drainTimeout));
            }
            catch (Exception e) { /* nothing I can really do, we are stopping anyway */ }
        }
        else {
            try {
                futures.add(c.sub.drain(drainTimeout));
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
        Set<EndpointStats> set = new HashSet<>();
        for (Context c : discoveryContexts) {
            set.add(c.stats);
        }
        List<EndpointStats> list = new ArrayList<>(set);
        list.sort(ENDPOINT_STATS_COMPARATOR);
        return list;
    }

    public CompletableFuture<Boolean> done() {
        return doneFuture;
    }

    private EndpointStats addDiscoveryContexts(String action, byte[] response, Dispatcher dispatcher, boolean internalDispatcher) {
        EndpointStats es = new EndpointStats(action);
        finishAddDiscoveryContext(dispatcher,
            new DiscoveryContext(es, action, null, null, response, dispatcher, internalDispatcher));

        finishAddDiscoveryContext(dispatcher,
            new DiscoveryContext(es, action, sd.name, null, response, dispatcher, internalDispatcher));

        finishAddDiscoveryContext(dispatcher,
            new DiscoveryContext(es, action, sd.name, id, response, dispatcher, internalDispatcher));

        return es;
    }

    private EndpointStats addStatsContexts(Dispatcher dispatcher, boolean internalDispatcher) {
        EndpointStats es = new EndpointStats(STATS);
        finishAddDiscoveryContext(dispatcher,
            new StatsContext(es, null, null, dispatcher, internalDispatcher));

        finishAddDiscoveryContext(dispatcher,
            new StatsContext(es, sd.name, null, dispatcher, internalDispatcher));

        finishAddDiscoveryContext(dispatcher,
            new StatsContext(es, sd.name, id, dispatcher, internalDispatcher));

        return es;
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

        public Context(String subject, EndpointStats stats, Dispatcher dispatcher, boolean internalDispatcher) {
            this.subject = subject;
            this.stats = stats;
            this.dispatcher = dispatcher;
            this.internalDispatcher = internalDispatcher;
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
                stats.lastError.set(t.toString());
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
        public ServiceContext(String name, String subject, Dispatcher dispatcher, boolean internalDispatcher) {
            super(subject, new EndpointStats(name), dispatcher, internalDispatcher);
        }

        @Override
        protected void subOnMessage(Message msg) throws InterruptedException {
            userMessageHandler.onMessage(msg);
        }
    }

    class DiscoveryContext extends Context {
        byte[] response;

        public DiscoveryContext(EndpointStats es, String name, String serviceName, String serviceId, byte[] response, Dispatcher dispatcher, boolean internalDispatcher) {
            super(toDiscoverySubject(name, serviceName, serviceId), es, dispatcher, internalDispatcher);
            this.response = response;
        }

        @Override
        protected void subOnMessage(Message msg) {
            conn.publish(msg.getReplyTo(), response);
        }
    }

    class StatsContext extends Context {
        public StatsContext(EndpointStats es, String serviceName, String serviceId, Dispatcher dispatcher, boolean internalDispatcher) {
            super(toDiscoverySubject(STATS, serviceName, serviceId), es, dispatcher, internalDispatcher);
        }

        @Override
        protected void subOnMessage(Message msg) {
            StatsRequest rq = new StatsRequest(msg.getData());
            if (rq.isInternal()) {
                List<EndpointStats> list = discoveryStats();
                list.add(0, serviceContext.stats);
                conn.publish(msg.getReplyTo(),
                    new StatsResponse(id, sd, list).serialize());
            }
            else {
                conn.publish(msg.getReplyTo(),
                    new StatsResponse(id, sd, serviceContext.stats).serialize());
            }
        }
    }
}
