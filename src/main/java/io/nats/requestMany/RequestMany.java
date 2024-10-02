// Copyright 2024 The NATS Authors
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

package io.nats.requestMany;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.impl.Headers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static io.nats.client.support.NatsConstants.NANOS_PER_MILLI;

/**
 * This class is EXPERIMENTAL, meaning it's api is subject to change.
 * The core Connection api provides the ability to make a request, but
 * only supports return ing the first response. The Request Many interface
 * provides the ability to receive multiple responses from a single request.
 * <p>This can support patterns like scatter-gather (getting responses from multiple
 * responders) or getting a multipart message from a single responder (without streaming)
 * <p>Responses generally all get to the client at about the same time.
 * Waiting until the first one lands will take most of the time.
 * Subsequent ones are generally coming at about the same time so queue up
 * in the client, and we don't want to wait to long after receiving the first
 * message, but also allow for some stragglers.
 */
public class RequestMany {
    private static final long MAX_MILLIS = Long.MAX_VALUE / NANOS_PER_MILLI; // so when I go to get millis it does not overflow
    private static final long MAX_NANOS = MAX_MILLIS * NANOS_PER_MILLI;      // "

    public static final long DEFAULT_SENTINEL_STRATEGY_TOTAL_WAIT = Duration.ofMinutes(10).toMillis();

    private final Connection conn;
    private final long totalWaitTimeNanos;
    private final long stallNanos;
    private final long maxResponses;
    private final boolean standardSentinel;

    @Override
    public String toString() {
        String ms = stallNanos == -1 ? ", <no stall>" : ", maxStall=" + stallNanos / NANOS_PER_MILLI;
        String mr = maxResponses == -1 ? ", <no max>" : ", maxResponses=" + maxResponses;
        return "RequestMany: totalWaitTime=" + totalWaitTimeNanos / NANOS_PER_MILLI + ms + mr;
    }

    // builder accepts millis then converts to nanos since we prefer to use nanos internally
    private RequestMany(Builder b) {
        this.conn = b.conn;
        this.totalWaitTimeNanos = b.totalWaitTimeNanos;
        this.stallNanos = b.stallNanos;
        this.maxResponses = b.maxResponses;
        this.standardSentinel = b.standardSentinel;
    }

    /**
     * The total amount of time to wait for messages regardless of other configuration
     * @return the total wait time in ms
     */
    public long getTotalWaitTime() {
        return totalWaitTimeNanos / NANOS_PER_MILLI;
    }

    /**
     * The amount of time to wait on messages other than the first, otherwise the request is stalled
     * @return the stall in ms or -1 if there is no stall
     */
    public long getStallTime() {
        return stallNanos == -1 ? -1 : stallNanos / NANOS_PER_MILLI;
    }

    /**
     * The maximum number of messages to wait for
     * @return then maximum number of responses or -1 if there is no max
     */
    public long getMaxResponses() {
        return maxResponses;
    }

    /**
     * Whether the configuration will automatically handle the standard sentinel
     * @return the flag
     */
    public boolean isStandardSentinel() {
        return standardSentinel;
    }

    /**
     * Helper to start a builder
     * @param conn the connection since a connection is required
     * @return the builder
     */
    public static Builder builder(Connection conn) {
        return new Builder(conn);
    }

    /**
     * Get a RequestMany that waits the entire default connection timeout for as many messages as it can
     * @param conn the connection since a connection is required
     * @return the builder
     */
    public static RequestMany wait(Connection conn) {
        return wait(conn, -1);
    }

    /**
     * Get a RequestMany that waits the entire time for as many messages as it can
     * @param conn the connection since a connection is required
     * @param totalWaitTimeMillis the total time to wait
     * @return the builder
     */
    public static RequestMany wait(Connection conn, long totalWaitTimeMillis) {
        return new Builder(conn).totalWaitTime(totalWaitTimeMillis).build();
    }

    /**
     * Get a RequestMany that waits a max total wait time of the default connection timeout
     * and automatically makes a stall time of 10 percent of the total wait time
     * @param conn the connection since a connection is required
     * @return the builder
     */
    public static RequestMany stall(Connection conn) {
        return stall(conn, -1);
    }

    /**
     * Get a RequestMany that waits a max total wait time
     * and automatically makes a stall time of 10 percent of the total wait time
     * @param conn the connection since a connection is required
     * @param totalWaitTimeMillis the total time to wait
     * @return the builder
     */
    public static RequestMany stall(Connection conn, long totalWaitTimeMillis) {
        if (totalWaitTimeMillis < 1) {
            totalWaitTimeMillis = getDefaultTimeout(conn);
        }
        return new Builder(conn).totalWaitTime(totalWaitTimeMillis)
            .stallTime(Math.min(getDefaultTimeout(conn), totalWaitTimeMillis / 10))
            .build();
    }

    /**
     * Get a RequestMany that waits a max total wait time of the default connection timeout
     * for the maximum number of responses
     * @param conn the connection since a connection is required
     * @param maxResponses the maximum number of responses
     * @return the builder
     */
    public static RequestMany maxResponses(Connection conn, long maxResponses) {
        return maxResponses(conn, -1, maxResponses);
    }

    /**
     * Get a RequestMany that waits a max total wait time
     * for the maximum number of responses
     * @param conn the connection since a connection is required
     * @param totalWaitTimeMillis the total time to wait
     * @param maxResponses the maximum number of responses
     * @return the builder
     */
    public static RequestMany maxResponses(Connection conn, long totalWaitTimeMillis, long maxResponses) {
        return new Builder(conn).totalWaitTime(totalWaitTimeMillis)
            .maxResponses(maxResponses)
            .build();
    }

    /**
     * Get a RequestMany that
     * <ul>
     * <li>Never waits more than 10 minutes for the entire process to complete. This is mostly for safety</li>
     * <li>Never waits more than the standard connection timeout for any message after the first.</li>
     * <li>Waits for the standard sentinel message to indicate the request is finished</li>
     * </ul>
     * @param conn the connection since a connection is required
     * @return the builder
     */
    public static RequestMany standardSentinel(Connection conn) {
        return standardSentinel(conn, DEFAULT_SENTINEL_STRATEGY_TOTAL_WAIT);
    }

    /**
     * Get a RequestMany that
     * <ul>
     * <li>Never waits more than the total wait time for the entire process to complete.</li>
     * <li>Never waits more than the the lessor of 1/10th of the total wait time or the standard connection timeout, for any message after the first.</li>
     * <li>Waits for the standard sentinel message to indicate the request is finished</li>
     * </ul>
     * @param conn the connection since a connection is required
     * @param totalWaitTimeMillis the total time to wait
     * @return the builder
     */
    public static RequestMany standardSentinel(Connection conn, long totalWaitTimeMillis) {
        return new Builder(conn)
            .totalWaitTime(totalWaitTimeMillis)
            .stallTime(Math.min(getDefaultTimeout(conn), totalWaitTimeMillis / 10))
            .standardSentinel()
            .build();
    }

    private static long getDefaultTimeout(Connection conn) {
        return conn.getOptions().getConnectionTimeout().toMillis();
    }

    private static long getDefaultTimeoutNanos(Connection conn) {
        return conn.getOptions().getConnectionTimeout().toNanos();
    }

    /**
     * Builder for Request Many
     * Default Behavior
     * <ul>
     * <li>the default total wait time is the connections options timeout</li>
     * <li>the default stall is "not used".</li>
     * <li>the default max responses is unlimited.</li>
     * </ul>
     */
    public static class Builder {
        private final Connection conn;
        private long totalWaitTimeNanos = -1;
        private long stallNanos = -1;
        private long maxResponses = -1;
        private boolean standardSentinel = false;

        public Builder(Connection conn) {
            this.conn = conn;
            maxResponses(-1);
        }

        /**
         * Set the total amount of time to wait for messages regardless of other configuration
         * Less than 1 clears it to default behavior, setting it to the connection request timeout
         * The actual maximum total wait time is 2.562048 hours since internally nanoseconds are used
         * @param totalWaitTimeMillis the total time to wait
         * @return the builder
         */
        public Builder totalWaitTime(long totalWaitTimeMillis) {
            totalWaitTimeNanos = toNanos(totalWaitTimeMillis);
            return this;
        }

        /**
         * The amount of time to wait on messages other than the first.
         * Less than 1 clears it to default behavior, setting it to "not used".
         * The actual maximum stall is 2.562048 hours since internally nanoseconds are used
         * @param stallMillis the maximum stall
         * @return the builder
         */
        public Builder stallTime(long stallMillis) {
            stallNanos = toNanos(stallMillis);
            return this;
        }

        private static long toNanos(long millis) {
            if (millis < 1) {
                return -1;
            }
            if (millis > MAX_MILLIS) {
                return MAX_NANOS;
            }
            return millis * NANOS_PER_MILLI;
        }

        /**
         * The maximum number of responses to get
         * Less than 1 clears it to default behavior, no max number of responses
         * @param maxResponses the max number of responses
         * @return the builder
         */
        public Builder maxResponses(long maxResponses) {
            this.maxResponses = maxResponses < 1 ? -1 : maxResponses;
            return this;
        }

        /**
         * Indicates to recgonize a null or zero byte payload as a sentinel
         * @return the builder
         */
        public Builder standardSentinel() {
            this.standardSentinel = true;
            return this;
        }

        public RequestMany build() {
            // fill in defaults.
            if (totalWaitTimeNanos == -1) {
                totalWaitTimeNanos = getDefaultTimeoutNanos(conn);
            }
            return new RequestMany(this);
        }
    }

    public List<RmMessage> fetch(String subject, byte[] payload) {
        return fetch(subject, null, payload);
    }

    public List<RmMessage> fetch(String subject, Headers headers, byte[] payload) {
        List<RmMessage> results = new ArrayList<>();
        request(subject, headers, payload, rmm -> {
            if (!rmm.isNormalEndOfData()) {
                results.add(rmm);
            }
            return true;
        });
        return results;
    }

    public LinkedBlockingQueue<RmMessage> queue(String subject, byte[] payload) {
        return queue(subject, null, payload);
    }

    public LinkedBlockingQueue<RmMessage> queue(String subject, Headers headers, byte[] payload) {
        final LinkedBlockingQueue<RmMessage> q = new LinkedBlockingQueue<>();
        conn.getOptions().getExecutor().submit(() -> {
            request(subject, headers, payload, rmm -> {
                q.add(rmm);
                return true;
            });
        });
        return q;
    }

    public void request(String subject, byte[] payload, RmHandler handler) {
        request(subject, null, payload, handler);
    }

    public void request(String subject, Headers headers, byte[] payload, RmHandler handler) {
        Subscription sub = null;

        // the default end of data will be a normal end of data (vs status or exception)
        RmMessage eod = RmMessage.NORMAL_EOD;
        try {
            String replyTo = conn.createInbox();
            sub = conn.subscribe(replyTo);
            conn.publish(subject, replyTo, headers, payload);

            long resultsLeft = maxResponses == -1 ? Long.MAX_VALUE : maxResponses; // Long.MAX_VALUE is a practical no limit
            long timeLeftNanos = totalWaitTimeNanos;
            long timeoutNanos = totalWaitTimeNanos; // first time we wait the whole timeout
            long timeoutStall = stallNanos == -1 ? totalWaitTimeNanos : stallNanos; // totalWaitTimeNanos is practical since leftover time will always be less

            long start = System.nanoTime();
            while (timeLeftNanos > 0) {
                // java sub next message returns null on timeout
                Message msg = sub.nextMessage(Duration.ofNanos(timeoutNanos));

                // we calculate this here so it does not consider any of our own or the handler's processing time.
                // I don't know if this is right. Maybe we should time including the handler.
                timeLeftNanos = totalWaitTimeNanos - (System.nanoTime() - start);

                if (msg == null) {
                    // timeout indicates we are done. uses the default EOD
                    return;
                }
                if (msg.isStatusMessage()) {
                    // status is terminal. Uses the status EOD so the user can see what happened.
                    eod = new RmMessage(msg);
                    return;
                }
                if (standardSentinel) {
                    if (msg.getData() == null || msg.getData().length == 0) {
                        // in standard sentinel, we have to give them eod
                        return;
                    }
                }
                if (!handler.handle(new RmMessage(msg))) {
                    // they already know it's the end, the prevents them from getting an EOD at all
                    // I'm pretty sure this is right.
                    eod = null;
                    return;
                }
                if (--resultsLeft < 1) {
                    // ee got the count, we are done. Uses the default EOD
                    return;
                }

                // subsequent times we wait the shortest of the time left vs the max stall
                timeoutNanos = Math.min(timeLeftNanos, timeoutStall);
            }

            // if it fell through, the last operation went over time. Fine, just use the default EOD
        }
        catch (RuntimeException r) {
            eod = new RmMessage(r);
            throw r;
        }
        catch (InterruptedException e) {
            eod = new RmMessage(e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        finally {
            try {
                if (eod != null) {
                    handler.handle(eod);
                }
            }
            catch (Exception ignore) {}
            try {
                //noinspection DataFlowIssue
                sub.unsubscribe();
            }
            catch (Exception ignore) {}
        }
    }
}
