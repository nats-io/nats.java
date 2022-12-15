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
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.service.api.Info;
import io.nats.service.api.Ping;
import io.nats.service.api.SchemaInfo;
import io.nats.service.api.Stats;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.nats.client.NUID.nextGlobal;
import static io.nats.service.Service.*;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class Discovery {

    private final Connection conn;
    private final long maxTimeMillis;
    private final int maxResults;

    public Discovery(Connection conn) {
        this(conn, -1, -1);
    }

    public Discovery(Connection conn, long maxTimeMillis, int maxResults) {
        this.conn = conn;
        this.maxTimeMillis = maxTimeMillis < 1 ? DEFAULT_DISCOVERY_MAX_TIME_MILLIS : maxTimeMillis;
        this.maxResults = maxResults < 1 ? DEFAULT_DISCOVERY_MAX_RESULTS : maxResults;
    }

    // ----------------------------------------------------------------------------------------------------
    // ping
    // ----------------------------------------------------------------------------------------------------
    public List<Ping> ping() {
        return ping(null);
    }

    public List<Ping> ping(String serviceName) {
        List<Ping> list = new ArrayList<>();
        discoverMany(PING, serviceName, json -> {
            list.add(new Ping(json));
        });
        return list;
    }

    public Ping ping(String serviceName, String serviceId) {
        String json = discoverOne(PING, serviceName, serviceId);
        return json == null ? null : new Ping(json);
    }

    // ----------------------------------------------------------------------------------------------------
    // info
    // ----------------------------------------------------------------------------------------------------
    public List<Info> info() {
        return info(null);
    }

    public List<Info> info(String serviceName) {
        List<Info> list = new ArrayList<>();
        discoverMany(INFO, serviceName, json -> {
            list.add(new Info(json));
        });
        return list;
    }

    public Info info(String serviceName, String serviceId) {
        String json = discoverOne(INFO, serviceName, serviceId);
        return json == null ? null : new Info(json);
    }

    // ----------------------------------------------------------------------------------------------------
    // schema
    // ----------------------------------------------------------------------------------------------------
    public List<SchemaInfo> schema() {
        return schema(null);
    }

    public List<SchemaInfo> schema(String serviceName) {
        List<SchemaInfo> list = new ArrayList<>();
        discoverMany(SCHEMA, serviceName, json -> {
            list.add(new SchemaInfo(json));
        });
        return list;
    }

    public SchemaInfo schema(String serviceName, String serviceId) {
        String json = discoverOne(SCHEMA, serviceName, serviceId);
        return json == null ? null : new SchemaInfo(json);
    }

    // ----------------------------------------------------------------------------------------------------
    // stats
    // ----------------------------------------------------------------------------------------------------
    public List<Stats> stats() {
        return stats(null, (StatsDataHandler)null);
    }

    public List<Stats> stats(StatsDataHandler statsDataHandler) {
        return stats(null, statsDataHandler);
    }

    public List<Stats> stats(String serviceName) {
        return stats(serviceName, (StatsDataHandler)null);
    }

    public List<Stats> stats(String serviceName, StatsDataHandler statsDataHandler) {
        List<Stats> list = new ArrayList<>();
        discoverMany(STATS, serviceName, json -> {
            list.add(new Stats(json, statsDataHandler));
        });
        return list;
    }

    public Stats stats(String serviceName, String serviceId) {
        return stats(serviceName, serviceId, null);
    }

    public Stats stats(String serviceName, String serviceId, StatsDataHandler statsDataHandler) {
        String json = discoverOne(STATS, serviceName, serviceId);
        return json == null ? null : new Stats(json, statsDataHandler);
    }

    // ----------------------------------------------------------------------------------------------------
    // workers
    // ----------------------------------------------------------------------------------------------------
    private String discoverOne(String action, String serviceName, String serviceId) {
        String subject = toDiscoverySubject(action, serviceName, serviceId);
        try {
            Message m = conn.request(subject, null, Duration.ofMillis(maxTimeMillis));
            if (m != null) {
                return new String(m.getData());
            }
        }
        catch (InterruptedException ignore) {}
        return null;
    }

    private void discoverMany(String action, String serviceName, Consumer<String> stringConsumer) {
        Subscription sub = null;
        try {
            StringBuilder sb = new StringBuilder(nextGlobal()).append('-').append(action);
            if (serviceName != null) {
                sb.append('-').append(serviceName);
            }
            String replyTo = sb.toString();

            sub = conn.subscribe(replyTo);

            String subject = toDiscoverySubject(action, serviceName, null);
            conn.publish(subject, replyTo, null);

            int resultsLeft = maxResults;
            long start = System.currentTimeMillis();
            long timeLeft = maxTimeMillis;
            while (resultsLeft > 0 && timeLeft > 0) {
                Message msg = sub.nextMessage(timeLeft);
                if (msg == null) {
                    return;
                }
                stringConsumer.accept(new String(msg.getData()));
                resultsLeft--;
                // try again while we have time
                timeLeft = maxTimeMillis - (System.currentTimeMillis() - start);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            try {
                if (sub != null) {
                    sub.unsubscribe();
                }
            }
            catch (Exception ignore) {}
        }
    }
}
