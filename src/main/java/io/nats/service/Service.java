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

public class Service implements Runnable {

    private final Connection conn;
    private final String id;
    private final String name;
    private final String subject;
    private final String version;
    private final List<Context> verbs;
    private final Context service;
    private final Dispatcher discoveryDispatcher;
    private final Dispatcher subjectDispatcher;
    private final MessageHandler userMessageHandler;
    private final List<EndpointStats> allEndpointStats;

    public Service(Connection conn,
                   String name,
                   String description,
                   String version,
                   String subject,
                   Schema schema,
                   MessageHandler userMessageHandler
    ) {
        this.conn = conn;
        this.userMessageHandler = userMessageHandler;

        id = io.nats.client.NUID.nextGlobal();
        this.name = name;
        this.subject = subject;
        this.version = version;

        discoveryDispatcher = conn.createDispatcher();
        subjectDispatcher = conn.createDispatcher();

        allEndpointStats = new ArrayList<>();

        verbs = new ArrayList<>();
        addDiscoveryContexts(verbs, "PING", new PingResponse(name, id).serialize());
        addDiscoveryContexts(verbs, "INFO", new InfoResponse(name, id, description, version, subject).serialize());
        addDiscoveryContexts(verbs, "SCHEMA", new SchemaResponse(name, id, version, schema).serialize());

        addStatsContexts(verbs);

        service = new ServiceContext(subject);
        allEndpointStats.add(service.stats);
        service.sub = subjectDispatcher.subscribe(subject, "q", service::onMessage);
    }

    //    stop(error?) function that allows user code to stop the service. Optionally this function should allow for an optional error. Stop should always drain its service subscriptions.
    public void stop(Throwable t) throws InterruptedException {
        discoveryDispatcher.drain(Duration.ofSeconds(3));
        subjectDispatcher.drain(Duration.ofSeconds(3));
        if (t == null) {
            done.complete(true);
        }
        else {
            done.completeExceptionally(t);
        }
    }

    public void reset() {
        for (EndpointStats e : allEndpointStats) {
            e.reset();
        }
    }

    public EndpointStats stats() {
        return service.stats;
    }

    CompletableFuture<Boolean> done = new CompletableFuture<>();
    public CompletableFuture<Boolean> doneFuture() {
        return done;
    }

    private void addDiscoveryContexts(List<Context> verbs, String action, byte[] response) {
        final DiscoveryContext ctx0 = new DiscoveryContext(getPrefix() + action, response);
        ctx0.sub = discoveryDispatcher.subscribe(ctx0.subject, ctx0::onMessage);
        verbs.add(ctx0);
        allEndpointStats.add(ctx0.stats);

        final DiscoveryContext ctx1 = new DiscoveryContext(getPrefix() + action + "." + name, response);
        ctx1.sub = discoveryDispatcher.subscribe(ctx1.subject, ctx1::onMessage);
        verbs.add(ctx1);
        allEndpointStats.add(ctx1.stats);

        final DiscoveryContext ctx2 = new DiscoveryContext(getPrefix() + action + "." + name + "." + id, response);
        ctx2.sub = discoveryDispatcher.subscribe(ctx2.subject, ctx2::onMessage);
        verbs.add(ctx2);
        allEndpointStats.add(ctx2.stats);
    }

    private void addStatsContexts(List<Context> verbs) {
        final StatsContext ctx0 = new StatsContext(getPrefix() + "STATS");
        ctx0.sub = discoveryDispatcher.subscribe(ctx0.subject, ctx0::onMessage);
        verbs.add(ctx0);
        allEndpointStats.add(ctx0.stats);

        final StatsContext ctx1 = new StatsContext(getPrefix() + "STATS." + name);
        ctx1.sub = discoveryDispatcher.subscribe(ctx1.subject, ctx1::onMessage);
        verbs.add(ctx1);
        allEndpointStats.add(ctx1.stats);

        final StatsContext ctx2 = new StatsContext(getPrefix() + "STATS." + name + "." + id);
        ctx2.sub = discoveryDispatcher.subscribe(ctx2.subject, ctx2::onMessage);
        verbs.add(ctx2);
        allEndpointStats.add(ctx2.stats);
    }

    private static String getPrefix() {
        return "$SRV.";
    }

    static class Context {
        String subject;
        Subscription sub;
        EndpointStats stats;
        MessageHandler requestHandler;

        public Context(String subject) {
            this.stats = new EndpointStats(subject);
        }

        public Context(String subject, MessageHandler requestHandler) {
            this.stats = new EndpointStats(subject);
            this.requestHandler = requestHandler;
        }

        public void onMessage(Message msg) throws InterruptedException {
            long requests = 1;
            long start = 0;
            try {
                requests = stats.numRequests.incrementAndGet();
                start = System.nanoTime();
                requestHandler.onMessage(msg);
            }
            catch (Throwable t) {
                stats.numErrors.incrementAndGet();
                stats.lastError = t.toString();
            }
            finally {
                long elapsed = System.nanoTime() - start;
                long total = stats.totalProcessingTime.addAndGet(elapsed);
                stats.averageProcessingTime.set(total / requests);
            }
        }

    }

    class ServiceContext extends Context {
        public ServiceContext(String subject) {
            super(subject, userMessageHandler);
        }
    }

    class DiscoveryContext extends Context {
        public DiscoveryContext(String subject, byte[] response) {
            super(subject, msg -> conn.publish(msg.getReplyTo(), response));
        }
    }

    class StatsContext extends Context {
        public StatsContext(String subject) {
            super(subject);
            requestHandler =
                msg -> {
                    if (new StatsRequest(msg.getData()).isInternal()) {
                        conn.publish(msg.getReplyTo(),
                            new StatsResponse(name, id, version, stats).serialize());
                    }
                    else {
                        conn.publish(msg.getReplyTo(),
                            new StatsResponse(name, id, version, allEndpointStats).serialize());
                    }
                };
        }
    }

    @Override
    public void run() {
        while (true) {

        }
    }
}
