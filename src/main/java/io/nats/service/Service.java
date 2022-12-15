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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.nats.client.support.Validator.*;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class Service {

    static final String PING = "PING";
    static final String INFO = "INFO";
    static final String SCHEMA = "SCHEMA";
    static final String STATS = "STATS";
    static final String DEFAULT_SERVICE_PREFIX = "$SRV.";
    static final String QGROUP = "q";

    public static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(5);
    public static final long DEFAULT_DISCOVERY_MAX_TIME_MILLIS = 5000;
    public static final int DEFAULT_DISCOVERY_MAX_RESULTS = 10;

    private final Connection conn;
    private final String id;
    private final Info info;
    private final SchemaInfo schemaInfo;
    private final Stats stats;
    private final List<Context> discoveryContexts;
    private final Context serviceContext;
    private final MessageHandler serviceMessageHandler;
    private final StatsDataSupplier statsDataSupplier;
    private final StatsDataDecoder statsDataDecoder;
    private final CompletableFuture<Boolean> doneFuture;
    private final Object stopLock = new Object();
    private Duration drainTimeout = DEFAULT_DRAIN_TIMEOUT;

    private Service(ServiceCreator sc) {
        id = io.nats.client.NUID.nextGlobal();
        conn = sc.conn;
        serviceMessageHandler = sc.serviceMessageHandler;
        statsDataSupplier = sc.statsDataSupplier;
        statsDataDecoder = sc.statsDataDecoder;
        info = new Info(id, sc.name, sc.description, sc.version, sc.subject);
        schemaInfo = new SchemaInfo(id, sc.name, sc.version, sc.schemaRequest, sc.schemaResponse);

        // User may provide 0 or more dispatchers, just use theirs when provided else use one we make
        boolean internalDiscovery = sc.dUserDiscovery == null;
        boolean internalService = sc.dUserService == null;
        Dispatcher dDiscovery = internalDiscovery ? conn.createDispatcher() : sc.dUserDiscovery;
        Dispatcher dService = internalService ? conn.createDispatcher() : sc.dUserService;

        // do the service first in case the server feels like rejecting the subject
        stats = new Stats(id, sc.name, sc.version);
        serviceContext = new ServiceContext(info.getSubject(), dService, internalService);
        serviceContext.sub = dService.subscribe(info.getSubject(), QGROUP, serviceContext::onMessage);

        discoveryContexts = new ArrayList<>();
        addDiscoveryContexts(PING, new Ping(id, info.getName()).serialize(), dDiscovery, internalDiscovery);
        addDiscoveryContexts(INFO, info.serialize(), dDiscovery, internalDiscovery);
        addDiscoveryContexts(SCHEMA, schemaInfo.serialize(), dDiscovery, internalDiscovery);
        addStatsContexts(dDiscovery, internalDiscovery);

        doneFuture = new CompletableFuture<>();
    }

    public void setDrainTimeout(Duration drainTimeout) {
        this.drainTimeout = drainTimeout;
    }

    @Override
    public String toString() {
        return "Service" + info.toJson();
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
        if (c.isInternalDispatcher) {
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
        stats.reset();
    }

    public String getId() {
        return info.getServiceId();
    }

    public Info getInfo() {
        return info;
    }

    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    public Stats getStats() {
        return stats.copy(statsDataDecoder);
    }

    public CompletableFuture<Boolean> done() {
        return doneFuture;
    }

    private void addDiscoveryContexts(String action, byte[] response, Dispatcher dispatcher, boolean internalDispatcher) {
        finishAddDiscoveryContext(dispatcher,
            new DiscoveryContext(action, null, null, response, dispatcher, internalDispatcher));

        finishAddDiscoveryContext(dispatcher,
            new DiscoveryContext(action, info.getName(), null, response, dispatcher, internalDispatcher));

        finishAddDiscoveryContext(dispatcher,
            new DiscoveryContext(action, info.getName(), id, response, dispatcher, internalDispatcher));
    }

    private void addStatsContexts(Dispatcher dispatcher, boolean internalDispatcher) {
        finishAddDiscoveryContext(dispatcher,
            new StatsContext(null, null, dispatcher, internalDispatcher));

        finishAddDiscoveryContext(dispatcher,
            new StatsContext(info.getName(), null, dispatcher, internalDispatcher));

        finishAddDiscoveryContext(dispatcher,
            new StatsContext(info.getName(), id, dispatcher, internalDispatcher));
    }

    private void finishAddDiscoveryContext(Dispatcher dispatcher, final Context ctx) {
        ctx.sub = dispatcher.subscribe(ctx.subject, ctx::onMessage);
        discoveryContexts.add(ctx);
    }

    abstract class Context {
        String subject;
        Subscription sub;
        Dispatcher dispatcher;
        boolean isInternalDispatcher;
        boolean isServiceContext;

        public Context(String subject, Dispatcher dispatcher, boolean isInternalDispatcher) {
            this.subject = subject;
            this.dispatcher = dispatcher;
            this.isInternalDispatcher = isInternalDispatcher;
        }

        protected abstract void subOnMessage(Message msg) throws InterruptedException;

        public void onMessage(Message msg) throws InterruptedException {
            long requestNo = isServiceContext ? stats.incrementNumRequests() : -1;
            long start = 0;
            try {
                start = System.nanoTime();
                subOnMessage(msg);
            }
            catch (Throwable t) {
                if (isServiceContext) {
                    stats.incrementNumErrors();
                    stats.setLastError(t.toString());
                }
                try {
                    ServiceMessage.replyStandardError(conn, msg, t.getMessage(), 500);
                }
                catch (Exception ignore) {}
            }
            finally {
                if (isServiceContext) {
                    long total = stats.addTotalProcessingTime(System.nanoTime() - start);
                    stats.setAverageProcessingTime(total / requestNo);
                }
            }
        }
    }

    class ServiceContext extends Context {
        public ServiceContext(String subject, Dispatcher dispatcher, boolean internalDispatcher) {
            super(subject, dispatcher, internalDispatcher);
            isServiceContext = true;
        }

        @Override
        protected void subOnMessage(Message msg) throws InterruptedException {
            serviceMessageHandler.onMessage(msg);
        }
    }

    class DiscoveryContext extends Context {
        byte[] response;

        public DiscoveryContext(String name, String serviceName, String serviceId, byte[] response, Dispatcher dispatcher, boolean internalDispatcher) {
            super(toDiscoverySubject(name, serviceName, serviceId), dispatcher, internalDispatcher);
            this.response = response;
        }

        @Override
        protected void subOnMessage(Message msg) {
            conn.publish(msg.getReplyTo(), response);
        }
    }

    class StatsContext extends Context {
        public StatsContext(String serviceName, String serviceId, Dispatcher dispatcher, boolean internalDispatcher) {
            super(toDiscoverySubject(STATS, serviceName, serviceId), dispatcher, internalDispatcher);
        }

        @Override
        protected void subOnMessage(Message msg) {
            if (statsDataSupplier != null) {
                stats.setData(statsDataSupplier.get());
            }
            conn.publish(msg.getReplyTo(), stats.serialize());
        }
    }

    public static String toDiscoverySubject(String baseSubject, String optionalServiceNameSegment, String optionalServiceIdSegment) {
        if (nullOrEmpty(optionalServiceIdSegment)) {
            if (nullOrEmpty(optionalServiceNameSegment)) {
                return DEFAULT_SERVICE_PREFIX + baseSubject;
            }
            return DEFAULT_SERVICE_PREFIX + baseSubject + "." + optionalServiceNameSegment;
        }
        return DEFAULT_SERVICE_PREFIX + baseSubject + "." + optionalServiceNameSegment + "." + optionalServiceIdSegment;
    }

    // ----------------------------------------------------------------------------------------------------
    // Service Creator (Builder)
    // ----------------------------------------------------------------------------------------------------
    public static ServiceCreator creator() {
        return new ServiceCreator();
    }

    public static class ServiceCreator {
        Connection conn;
        String name;
        String description;
        String version;
        String subject;
        String schemaRequest;
        String schemaResponse;
        MessageHandler serviceMessageHandler;
        Dispatcher dUserDiscovery;
        Dispatcher dUserService;
        StatsDataSupplier statsDataSupplier;
        StatsDataDecoder statsDataDecoder;

        public ServiceCreator connection(Connection conn) {
            this.conn = conn;
            return this;
        }

        public ServiceCreator name(String name) {
            this.name = name;
            return this;
        }

        public ServiceCreator description(String description) {
            this.description = description;
            return this;
        }

        public ServiceCreator version(String version) {
            this.version = version;
            return this;
        }

        public ServiceCreator subject(String subject) {
            this.subject = subject;
            return this;
        }

        public ServiceCreator schemaRequest(String schemaRequest) {
            this.schemaRequest = schemaRequest;
            return this;
        }

        public ServiceCreator schemaResponse(String schemaResponse) {
            this.schemaResponse = schemaResponse;
            return this;
        }

        public ServiceCreator serviceMessageHandler(MessageHandler userMessageHandler) {
            this.serviceMessageHandler = userMessageHandler;
            return this;
        }

        public ServiceCreator userDiscoveryDispatcher(Dispatcher dUserDiscovery) {
            this.dUserDiscovery = dUserDiscovery;
            return this;
        }

        public ServiceCreator userServiceDispatcher(Dispatcher dUserService) {
            this.dUserService = dUserService;
            return this;
        }

        public ServiceCreator statsDataHandlers(StatsDataSupplier statsDataSupplier, StatsDataDecoder statsDataDecoder) {
            this.statsDataSupplier = statsDataSupplier;
            this.statsDataDecoder = statsDataDecoder;
            return this;
        }

        public Service startService() {
            required(conn, "Connection");
            required(serviceMessageHandler, "Service Message Handler");
            validateIsRestrictedTerm(name, "Name", true);
            required(version, "Version");
            if ((statsDataSupplier != null && statsDataDecoder == null)
                || (statsDataSupplier == null && statsDataDecoder != null)) {
                throw new IllegalArgumentException("You must provide neither or both the stats data supplier and decoder");
            }

            return new Service(this);
        }
    }
}
