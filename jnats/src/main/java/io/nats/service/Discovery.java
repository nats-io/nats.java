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

package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.NatsSystemClock;
import io.nats.client.Subscription;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.nats.client.support.NatsConstants.NANOS_PER_MILLI;
import static io.nats.service.Service.*;

/**
 * Discovery is a utility class to help discover services by executing Ping, Info and Stats requests
 * You are required to provide a connection.
 * Optionally you can set 'maxTimeMillis' and 'maxResults'. When making a discovery request,
 * the discovery will wait until the first one of those thresholds is reached before returning the results.
 * <p>'maxTimeMillis' defaults to {@value DEFAULT_DISCOVERY_MAX_TIME_MILLIS}</p>
 * <p>'maxResults' defaults tp {@value DEFAULT_DISCOVERY_MAX_RESULTS}</p>
 */
public class Discovery {
    public static final long DEFAULT_DISCOVERY_MAX_TIME_MILLIS = 5000;
    public static final int DEFAULT_DISCOVERY_MAX_RESULTS = 10;

    private final Connection conn;
    private final long maxTimeNanos;
    private final int maxResults;

    private Supplier<String> inboxSupplier;

    /**
     * Construct a Discovery instance with a connection and default maxTimeMillis / maxResults
     * @param conn the NATS Connection
     */
    public Discovery(Connection conn) {
        this(conn, 0, 0);
    }

    /**
     * Construct a Discovery instance
     * @param conn the NATS Connection
     * @param maxTimeMillis the maximum time to wait for discovery requests to complete or any number less than 1 to use the default
     * @param maxResults the maximum number of results to wait for or any number less than 1 to use the default
     */
    public Discovery(Connection conn, long maxTimeMillis, int maxResults) {
        this.conn = conn;
        this.maxTimeNanos = (maxTimeMillis < 1 ? DEFAULT_DISCOVERY_MAX_TIME_MILLIS : maxTimeMillis) * NANOS_PER_MILLI;
        this.maxResults = maxResults < 1 ? DEFAULT_DISCOVERY_MAX_RESULTS : maxResults;
        setInboxSupplier(null);
    }

    /**
     * Override the normal inbox with a custom inbox to support you security model
     * @param inboxSupplier the supplier
     */
    public void setInboxSupplier(Supplier<String> inboxSupplier) {
        this.inboxSupplier = inboxSupplier == null ? conn::createInbox : inboxSupplier;
    }

    // ----------------------------------------------------------------------------------------------------
    // ping
    // ----------------------------------------------------------------------------------------------------

    /**
     * Make a ping request to all services running on the server.
     * @return the list of {@link PingResponse}
     */
    public List<PingResponse> ping() {
        return ping(null);
    }

    /**
     * Make a ping request only to services having the matching service name
     * @param serviceName the service name
     * @return the list of {@link PingResponse}
     */
    public List<PingResponse> ping(String serviceName) {
        List<PingResponse> list = new ArrayList<>();
        discoverMany(SRV_PING, serviceName, jsonBytes -> list.add(new PingResponse(jsonBytes)));
        return list;
    }

    /**
     * Make a ping request to a specific instance of a service having matching service name and id
     * @param serviceName the service name
     * @param serviceId the specific service id
     * @return the list of {@link PingResponse}
     */
    public PingResponse ping(String serviceName, String serviceId) {
        byte[] jsonBytes = discoverOne(SRV_PING, serviceName, serviceId);
        return jsonBytes == null ? null : new PingResponse(jsonBytes);
    }

    // ----------------------------------------------------------------------------------------------------
    // info
    // ----------------------------------------------------------------------------------------------------

    /**
     * Make an info request to all services running on the server.
     * @return the list of {@link InfoResponse}
     */
    public List<InfoResponse> info() {
        return info(null);
    }

    /**
     * Make an info request only to services having the matching service name
     * @param serviceName the service name
     * @return the list of {@link InfoResponse}
     */
    public List<InfoResponse> info(String serviceName) {
        List<InfoResponse> list = new ArrayList<>();
        discoverMany(SRV_INFO, serviceName, jsonBytes -> list.add(new InfoResponse(jsonBytes)));
        return list;
    }

    /**
     * Make an info request to a specific instance of a service having matching service name and id
     * @param serviceName the service name
     * @param serviceId the specific service id
     * @return the list of {@link InfoResponse}
     */
    public InfoResponse info(String serviceName, String serviceId) {
        byte[] jsonBytes = discoverOne(SRV_INFO, serviceName, serviceId);
        return jsonBytes == null ? null : new InfoResponse(jsonBytes);
    }

    // ----------------------------------------------------------------------------------------------------
    // stats
    // ----------------------------------------------------------------------------------------------------

    /**
     * Make a stats request to all services running on the server.
     * @return the list of {@link StatsResponse}
     */
    public List<StatsResponse> stats() {
        return stats(null);
    }

    /**
     * Make a stats request only to services having the matching service name
     * @param serviceName the service name
     * @return the list of {@link StatsResponse}
     */
    public List<StatsResponse> stats(String serviceName) {
        List<StatsResponse> list = new ArrayList<>();
        discoverMany(SRV_STATS, serviceName, jsonBytes -> list.add(new StatsResponse(jsonBytes)));
        return list;
    }

    /**
     * Make a stats request to a specific instance of a service having matching service name and id
     * @param serviceName the service name
     * @param serviceId the specific service id
     * @return the list of {@link StatsResponse}
     */
    public StatsResponse stats(String serviceName, String serviceId) {
        byte[] jsonBytes = discoverOne(SRV_STATS, serviceName, serviceId);
        return jsonBytes == null ? null : new StatsResponse(jsonBytes);
    }

    // ----------------------------------------------------------------------------------------------------
    // workers
    // ----------------------------------------------------------------------------------------------------
    private byte[] discoverOne(String action, String serviceName, String serviceId) {
        String subject = Service.toDiscoverySubject(action, serviceName, serviceId);
        try {
            Message m = conn.request(subject, null, Duration.ofNanos(maxTimeNanos));
            if (m != null) {
                return m.getData();
            }
        }
        catch (InterruptedException e) {
            // conn.request interrupted means data is not retrieved
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return null;
    }

    private void discoverMany(String action, String serviceName, Consumer<byte[]> dataConsumer) {
        Subscription sub = null;
        try {
            String replyTo = inboxSupplier.get();
            sub = conn.subscribe(replyTo);

            String subject = toDiscoverySubject(action, serviceName, null);
            conn.publish(subject, replyTo, null);

            int resultsLeft = maxResults;
            long start = NatsSystemClock.nanoTime();
            long timeLeft = maxTimeNanos;
            while (resultsLeft > 0 && timeLeft > 0) {
                Message msg = sub.nextMessage(Duration.ofNanos(timeLeft));
                if (msg == null) {
                    return;
                }
                dataConsumer.accept(msg.getData());
                resultsLeft--;
                // try again while we have time
                timeLeft = maxTimeNanos - (NatsSystemClock.nanoTime() - start);
            }
        }
        catch (InterruptedException e) {
            // sub.nextMessage was fetching one message
            // and data is not completely read
            // so it seems like this is an error condition
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        finally {
            try {
                //noinspection DataFlowIssue
                sub.unsubscribe();
            }
            catch (Exception ignore) {}
        }
    }
}
