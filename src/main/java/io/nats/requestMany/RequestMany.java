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
import io.nats.client.support.Debug;

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
    public static final long MAX_MILLIS = Long.MAX_VALUE / NANOS_PER_MILLI; // so when I go to get millis it does not overflow
    public static final long MAX_NANOS = MAX_MILLIS * NANOS_PER_MILLI;      // "

    private final Connection conn;
    private final long totalWaitTimeNanos;
    private final long maxStallNanos;
    private final long maxResponses;

    @Override
    public String toString() {
        String ms = maxStallNanos == MAX_NANOS ? ", <no stall>" : ", maxStall=" + maxStallNanos / NANOS_PER_MILLI;
        String mr = maxResponses == Long.MAX_VALUE ? ", <no max>" : ", maxResponses=" + maxResponses;
        return "RequestMany: totalWaitTime=" + totalWaitTimeNanos / NANOS_PER_MILLI + ms + mr;
    }

    // builder accepts millis then converts to nanos since we prefer to use nanos internally
    public RequestMany(Builder b) {
        this.conn = b.conn;
        this.totalWaitTimeNanos = b.totalWaitTimeNanos;
        this.maxStallNanos = b.maxStallNanos;
        this.maxResponses = b.maxResponses;
        Debug.info("RM", this);
    }

    /**
     * The total amount of time to wait for messages regardless of other configuration
     * @return the total wait time
     */
    public long getTotalWaitTime() {
        return totalWaitTimeNanos / NANOS_PER_MILLI;
    }

    /**
     * The amount of time to wait on messages other than the first
     * @return the max stall
     */
    public long getMaxStall() {
        return maxStallNanos / NANOS_PER_MILLI;
    }

    /**
     * The maximum number of messages to wait for
     * @return then maximum number of responses
     */
    public long getMaxResponses() {
        return maxResponses;
    }

    public static Builder builder(Connection conn) {
        return new Builder(conn);
    }

    /**
     * Builder for Request Many
     * Default Behavior
     * <ul>
     * <li>if you don't set total wait time or stall, both default.</li>
     * <li>the default total wait time is the connections options timeout</li>
     * <li>the default stall time is 1/10 of the default total wait time.</li>
     * </ul>
     * <p>Total Wait Time
     * <ul>
     * <li>if you set total wait time, but not stall, stall defaults don't use stall</li>
     * </ul>
     * <p>Max Stall
     * <ul>
     * <li>if you set max stall to a value between 1 and MAX_MILLIS inclusive, max stall is that value</li>
     * <li>if you set max stall to an invalid value, it's like clearing it to default behavior</li>
     * </ul>
     * <p>Max Responses
     * <ul>
     * <li>if you set max responses to a value greater than 0, max responses is that value</li>
     * <li>if you set max responses to an invalid value, max responses is Long.MAX_VALUE</li>
     * </ul>
     */
    public static class Builder {
        private final Connection conn;
        private Long totalWaitTimeNanos;
        private Long maxStallNanos;
        private long maxResponses;

        public Builder(Connection conn) {
            this.conn = conn;
            maxResponses(-1);
        }

        /**
         * Set the total amount of time to wait for messages regardless of other configuration
         * Less than 1 clears it to default behavior, setting it to the connection request timeout
         * @param totalWaitTimeMillis the total time to wait
         * @return the builder
         */
        public Builder totalWaitTime(long totalWaitTimeMillis) {
            totalWaitTimeNanos = toNanos(totalWaitTimeMillis);
            return this;
        }

        /**
         * The amount of time to wait on messages other than the first.
         * @param maxStallMillis the max stall
         * @return the builder
         */
        public Builder maxStall(long maxStallMillis) {
            maxStallNanos = toNanos(maxStallMillis);
            return this;
        }

        private static Long toNanos(long millis) {
            return millis < 1 || millis > MAX_MILLIS ? null : millis * NANOS_PER_MILLI;
        }

        /**
         * The maximum number of responses to get
         * @param maxResponses the max number of responses
         * @return the builder
         */
        public Builder maxResponses(long maxResponses) {
            this.maxResponses = maxResponses < 1 ? Long.MAX_VALUE : maxResponses;
            return this;
        }

        public RequestMany build() {
            // totalWaitTimeNanos must have a value
            if (totalWaitTimeNanos == null) {
                totalWaitTimeNanos = conn.getOptions().getConnectionTimeout().toNanos();
            }
            if (maxStallNanos == null) {
                maxStallNanos = totalWaitTimeNanos / 10;
            }
            return new RequestMany(this);
        }
    }

    public List<RmMessage> fetch(String subject, byte[] payload) {
        return fetch(subject, null, payload);
    }

    public List<RmMessage> fetch(String subject, Headers headers, byte[] payload) {
        List<RmMessage> results = new ArrayList<>();
        gather(subject, headers, payload, rmm -> {
            if (!rmm.isNormalEndOfData()) {
                results.add(rmm);
            }
            return true;
        });
        return results;
    }

    public LinkedBlockingQueue<RmMessage> iterate(String subject, byte[] payload) {
        return iterate(subject, null, payload);
    }

    public LinkedBlockingQueue<RmMessage> iterate(String subject, Headers headers, byte[] payload) {
        final LinkedBlockingQueue<RmMessage> q = new LinkedBlockingQueue<>();
        conn.getOptions().getExecutor().submit(() -> {
            gather(subject, headers, payload, rmm -> {
                q.add(rmm);
                return true;
            });
        });
        return q;
    }

    public void gather(String subject, byte[] payload, RmHandler handler) {
        gather(subject, null, payload, handler);
    }

    public void gather(String subject, Headers headers, byte[] payload, RmHandler handler) {
        Subscription sub = null;

        // the default end of data will be a normal end of data (vs status or exception)
        RmMessage eod = RmMessage.NORMAL_EOD;
        try {
            String replyTo = conn.createInbox();
            sub = conn.subscribe(replyTo);
            conn.publish(subject, replyTo, headers, payload);

            long resultsLeft = maxResponses;
            long timeLeftNanos = totalWaitTimeNanos;
            long timeoutNanos = totalWaitTimeNanos; // first time we wait the whole timeout

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
                if (!handler.gather(new RmMessage(msg))) {
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
                timeoutNanos = Math.min(timeLeftNanos, maxStallNanos);
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
                    handler.gather(eod);
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
