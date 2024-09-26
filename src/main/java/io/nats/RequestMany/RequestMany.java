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

package io.nats.RequestMany;

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
 * The RequestMany is
 */
public class RequestMany {
    public static final long DEFAULT_TOTAL_WAIT_TIME_MS = 1000;
    private static final long MAX_MILLIS = Long.MAX_VALUE / NANOS_PER_MILLI; // so went I go to get millis it does not overflow
    private static final long MAX_NANOS = MAX_MILLIS * NANOS_PER_MILLI; // so went I go to get millis it does not overflow

    private final Connection conn;
    private final long totalWaitTimeNanos;
    private final long maxStallNanos;
    private final long maxResponses;

    public RequestMany(Builder b) {
        this.conn = b.conn;
        this.totalWaitTimeNanos = b.totalWaitTimeNanos;
        this.maxStallNanos = b.maxStallNanos;
        this.maxResponses = b.maxResponses;
    }

    public static Builder builder(Connection conn) {
        return new Builder(conn);
    }

    public static class Builder {
        private final Connection conn;
        private long totalWaitTimeNanos = DEFAULT_TOTAL_WAIT_TIME_MS * NANOS_PER_MILLI;
        private long maxStallNanos = MAX_MILLIS * NANOS_PER_MILLI;
        private long maxResponses = Long.MAX_VALUE;

        public Builder(Connection conn) {
            this.conn = conn;
        }

        public Builder totalWaitTime(long totalWaitTimeMillis) {
            totalWaitTimeNanos = toNanos(totalWaitTimeMillis);
            return this;
        }

        public Builder maxStall(long maxStallMillis) {
            maxStallNanos = toNanos(maxStallMillis);
            return this;
        }

        private static long toNanos(long millis) {
            return millis < 1 || millis > MAX_MILLIS ? MAX_NANOS : millis * NANOS_PER_MILLI;
        }

        public Builder maxResponses(long maxResponses) {
            this.maxResponses = maxResponses;
            return this;
        }

        public RequestMany build() {
            return new RequestMany(this);
        }
    }

    public List<RequestManyMessage> fetch(String subject, byte[] payload) {
        return fetch(subject, null, payload);
    }

    public List<RequestManyMessage> fetch(String subject, Headers headers, byte[] payload) {
        List<RequestManyMessage> results = new ArrayList<>();
        gather(subject, headers, payload, rmm -> {
            results.add(rmm);
            return true;
        });
        return results;
    }

    public LinkedBlockingQueue<RequestManyMessage> iterate(String subject, byte[] payload) {
        return iterate(subject, null, payload);
    }

    public LinkedBlockingQueue<RequestManyMessage> iterate(String subject, Headers headers, byte[] payload) {
        final LinkedBlockingQueue<RequestManyMessage> q = new LinkedBlockingQueue<>();
        conn.getOptions().getExecutor().submit(() -> {
            gather(subject, headers, payload, rmm -> {
                q.add(rmm);
                return true;
            });
        });
        return q;
    }

    public void gather(String subject, byte[] payload, RequestManyHandler handler) {
        gather(subject, null, payload, handler);
    }

    public void gather(String subject, Headers headers, byte[] payload, RequestManyHandler handler) {
        RequestManyMessage eod = RequestManyMessage.EOD;

        Subscription sub = null;
        try {
            String replyTo = conn.createInbox();
            sub = conn.subscribe(replyTo);
            conn.publish(subject, replyTo, headers, payload);

            long resultsLeft = maxResponses;
            long start = System.nanoTime();
            long timeLeftNanos = totalWaitTimeNanos;
            long timeoutNanos = totalWaitTimeNanos; // first time we wait the whole timeout
            while (timeLeftNanos > 0) {
                Message msg = sub.nextMessage(Duration.ofNanos(timeoutNanos));
                timeLeftNanos = totalWaitTimeNanos - (System.nanoTime() - start);
                if (msg == null) {
                    return;
                }
                if (msg.isStatusMessage()) {
                    eod = new RequestManyMessage(msg);
                    return;
                }
                if (!handler.gather(new RequestManyMessage(msg)) || --resultsLeft < 1) {
                    return;
                }
                timeoutNanos = Math.min(timeLeftNanos, maxStallNanos); // subsequent times we wait the shortest of the time left vs the max stall
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        finally {
            handler.gather(eod);
            try {
                //noinspection DataFlowIssue
                sub.unsubscribe();
            }
            catch (Exception ignore) {}
        }
    }
}
