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
 * RequestMany is EXPERIMENTAL
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
    }

    public long getTotalWaitTime() {
        return totalWaitTimeNanos / NANOS_PER_MILLI;
    }

    public long getMaxStall() {
        return maxStallNanos / NANOS_PER_MILLI;
    }

    public long getMaxResponses() {
        return maxResponses;
    }

    public static Builder builder(Connection conn) {
        return new Builder(conn);
    }

    public static class Builder {
        private final Connection conn;
        private Long totalWaitTimeNanos;
        private Long maxStallNanos;
        private long maxResponses;

        public Builder(Connection conn) {
            this.conn = conn;
            maxResponses = Long.MAX_VALUE;
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
            // totalWaitTimeNanos must have a value
            if (totalWaitTimeNanos == null) {
                totalWaitTimeNanos = conn.getOptions().getConnectionTimeout().toNanos();

                // if they also didn't set this, default is 10 percent
                if (maxStallNanos == null) {
                    maxStallNanos = totalWaitTimeNanos / 10;
                }
            }
            else if (maxStallNanos == null) {
                // if they had set totalWaitTimeNanos but not maxStallNanos
                // treat it as not supplied
                maxStallNanos = MAX_NANOS;
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
