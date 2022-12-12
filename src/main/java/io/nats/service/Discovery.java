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
import io.nats.service.api.InfoResponse;
import io.nats.service.api.PingResponse;
import io.nats.service.api.SchemaResponse;
import io.nats.service.api.StatsResponse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.nats.service.ServiceUtil.*;
import static io.nats.service.api.StatsRequest.INTERNAL_STATS_REQUEST_BYTES;

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
    public List<PingResponse> ping() {
        return ping(null);
    }

    public List<PingResponse> ping(String serviceName) {
        List<PingResponse> list = new ArrayList<>();
        discover(PING, serviceName, null, null, json -> {
            list.add(new PingResponse(json));
        });
        return list;
    }

    public PingResponse ping(String serviceName, String serviceId) {
        String json = discoverOne(PING, serviceName, serviceId, null);
        return json == null ? null : new PingResponse(json);
    }

    // ----------------------------------------------------------------------------------------------------
    // info
    // ----------------------------------------------------------------------------------------------------
    public List<InfoResponse> info() {
        return info(null);
    }

    public List<InfoResponse> info(String serviceName) {
        List<InfoResponse> list = new ArrayList<>();
        discover(INFO, serviceName, null, null, json -> {
            list.add(new InfoResponse(json));
        });
        return list;
    }

    public InfoResponse info(String serviceName, String serviceId) {
        String json = discoverOne(INFO, serviceName, serviceId, null);
        return json == null ? null : new InfoResponse(json);
    }

    // ----------------------------------------------------------------------------------------------------
    // schema
    // ----------------------------------------------------------------------------------------------------
    public List<SchemaResponse> schema() {
        return schema(null);
    }

    public List<SchemaResponse> schema(String serviceName) {
        List<SchemaResponse> list = new ArrayList<>();
        discover(SCHEMA, serviceName, null, null, json -> {
            list.add(new SchemaResponse(json));
        });
        return list;
    }

    public SchemaResponse schema(String serviceName, String serviceId) {
        String json = discoverOne(SCHEMA, serviceName, serviceId, null);
        return json == null ? null : new SchemaResponse(json);
    }

    // ----------------------------------------------------------------------------------------------------
    // stats
    // ----------------------------------------------------------------------------------------------------
    public List<StatsResponse> stats() {
        return stats(null, false);
    }

    public List<StatsResponse> stats(boolean includeInternal) {
        return stats(null, includeInternal);
    }

    public List<StatsResponse> stats(String serviceName) {
        return stats(serviceName, false);
    }

    public List<StatsResponse> stats(String serviceName, boolean includeInternal) {
        List<StatsResponse> list = new ArrayList<>();
        byte[] body = includeInternal ? INTERNAL_STATS_REQUEST_BYTES : null;
        discover(STATS, serviceName, null, body, json -> {
            list.add(new StatsResponse(json));
        });
        return list;
    }

    public StatsResponse stats(String serviceName, String serviceId) {
        return stats(serviceName, serviceId, false);
    }

    public StatsResponse stats(String serviceName, String serviceId, boolean includeInternal) {
        byte[] body = includeInternal ? INTERNAL_STATS_REQUEST_BYTES : null;
        String json = discoverOne(STATS, serviceName, serviceId, body);
        return json == null ? null : new StatsResponse(json);
    }

    // ----------------------------------------------------------------------------------------------------
    // workers
    // ----------------------------------------------------------------------------------------------------
    private String discoverOne(String action, String serviceName, String serviceId, byte[] body) {
        String subject = toDiscoverySubject(action, serviceName, serviceId);
        try {
            Message m = conn.request(subject, body, Duration.ofMillis(maxTimeMillis));
            if (m != null) {
                return new String(m.getData());
            }
        }
        catch (InterruptedException ignore) {}
        return null;
    }

    private void discover(String action, String serviceName, String serviceId, byte[] body,
                          java.util.function.Consumer<String> stringConsumer) {
        Subscription sub = null;
        try {
            String replyTo = toReplyTo(action, serviceName, serviceId);
            sub = conn.subscribe(replyTo);

            String subject = toDiscoverySubject(action, serviceName, serviceId);
            conn.publish(subject, replyTo, body);

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

    private String toReplyTo(String action, String serviceName, String serviceId) {
        StringBuilder sb = new StringBuilder(io.nats.client.NUID.nextGlobal());
        sb.append('-').append(action);
        if (serviceName != null) {
            sb.append('-').append(serviceName);
        }
        if (serviceId != null) {
            sb.append('-').append(serviceId);
        }
        return sb.toString();
    }

}
