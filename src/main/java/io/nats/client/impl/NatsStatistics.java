// Copyright 2015-2018 The NATS Authors
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

package io.nats.client.impl;

import io.nats.client.Statistics;

import java.text.NumberFormat;
import java.util.LongSummaryStatistics;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

class NatsStatistics implements Statistics {
    private ReentrantLock lock;
    private LongSummaryStatistics accumulateStats;

    private AtomicLong flushCounter;
    private AtomicLong outstandingRequests;
    private AtomicLong requestsSent;
    private AtomicLong repliesReceived;
    private AtomicLong inMsgs;
    private AtomicLong outMsgs;
    private AtomicLong inBytes;
    private AtomicLong outBytes;

    public NatsStatistics() {
        this.lock = new ReentrantLock();
        this.accumulateStats = new LongSummaryStatistics();
        this.flushCounter = new AtomicLong();
        this.outstandingRequests = new AtomicLong();
        this.requestsSent = new AtomicLong();
        this.repliesReceived = new AtomicLong();
        this.inMsgs = new AtomicLong();
        this.outMsgs = new AtomicLong();
        this.inBytes = new AtomicLong();
        this.outBytes = new AtomicLong();
    }

    void incrementRequestsSent() {
        this.requestsSent.incrementAndGet();
    }

    void incrementRepliesReceived() {
        this.repliesReceived.incrementAndGet();
    }

    void incrementInMsgs() {
        this.inMsgs.incrementAndGet();
    }

    void incrementOutMsgs() {
        this.outMsgs.incrementAndGet();
    }

    void incrementInBytes(long bytes) {
        this.inBytes.addAndGet(bytes);
    }

    void incrementOutBytes(long bytes) {
        this.outBytes.addAndGet(bytes);
    }

    void incrementFlushCounter() {
        this.flushCounter.incrementAndGet();
    }

    void incrementOutstandingRequests() {
        this.outstandingRequests.incrementAndGet();
    }

    void decrementOutstandingRequests() {
        this.outstandingRequests.decrementAndGet();
    }

    void registerAccumulate(long msgCount) {
        lock.lock();
        try {
            accumulateStats.accept(msgCount);
        } finally {
            lock.unlock();
        }
    }

    public long getInMsgs() {
        return this.inMsgs.get();
    }

    public long getOutMsgs() {
        return this.outMsgs.get();
    }

    public long getInBytes() {
        return this.inBytes.get();
    }

    public long getOutBytes() {
        return this.outBytes.get();
    }

    public long getFlushCounter() {
        return flushCounter.get();
    }

    public long getOutstandingRequests() {
        return outstandingRequests.get();
    }

    public String buildHumanFriendlyString() {
        StringBuilder builder = new StringBuilder();

        lock.lock();
        try {
            builder.append("### Connection ###\n");
            builder.append("Successful Flush Calls:          ");
            builder.append(this.flushCounter.get());
            builder.append("\n");
            builder.append("Requests Sent:                   ");
            builder.append(this.requestsSent.get());
            builder.append("\n");
            builder.append("Replies Received:                ");
            builder.append(this.repliesReceived.get());
            builder.append("\n");
            builder.append("Outstanding Request Futures:     ");
            builder.append(this.outstandingRequests.get());
            builder.append("\n");
            builder.append("\n");
            builder.append("### Reader ###\n");
            builder.append("Messages in:     ");
            builder.append(this.inMsgs.get());
            builder.append("\n");
            builder.append("Bytes in:     ");
            builder.append(this.inBytes.get());
            builder.append("\n");
            builder.append("\n");
            builder.append("### Writer ###\n");
            builder.append("Messages out:     ");
            builder.append(this.outMsgs.get());
            builder.append("\n");
            builder.append("Bytes out:     ");
            builder.append(this.outBytes.get());
            builder.append("\n");
            builder.append("Accumulation Calls:              ");
            builder.append(String.valueOf(accumulateStats.getCount()));
            builder.append("\n");
            builder.append("Average Messages Per Accumulate: ");
            builder.append(NumberFormat.getNumberInstance().format(accumulateStats.getAverage()));
            builder.append("\n");
            builder.append("Min Messages Per Accumulate:     ");
            builder.append(String.valueOf(accumulateStats.getMin()));
            builder.append("\n");
            builder.append("Max Messages Per Accumulate:     ");
            builder.append(String.valueOf(accumulateStats.getMax()));
            builder.append("\n");
        } finally {
            lock.unlock();
        }

        return builder.toString();
    }
}