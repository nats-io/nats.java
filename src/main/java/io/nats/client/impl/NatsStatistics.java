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
import io.nats.client.StatisticsCollector;

import java.text.NumberFormat;
import java.util.LongSummaryStatistics;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

// TODO: Consider making public, and if so add javadoc

class NatsStatistics implements Statistics, StatisticsCollector {
    private ReentrantLock lock;
    private LongSummaryStatistics readStats;
    private LongSummaryStatistics writeStats;

    private AtomicLong flushCounter;
    private AtomicLong outstandingRequests;
    private AtomicLong requestsSent;
    private AtomicLong repliesReceived;
    private AtomicLong duplicateRepliesReceived;
    private AtomicLong orphanRepliesReceived;
    private AtomicLong reconnects;
    private AtomicLong inMsgs;
    private AtomicLong outMsgs;
    private AtomicLong inBytes;
    private AtomicLong outBytes;
    private AtomicLong pingCount;
    private AtomicLong okCount;
    private AtomicLong errCount;
    private AtomicLong exceptionCount;
    private AtomicLong droppedCount;

    private boolean trackAdvanced;

    public NatsStatistics() {
        this.readStats = new LongSummaryStatistics();
        this.writeStats = new LongSummaryStatistics();

        this.lock = new ReentrantLock();
        this.flushCounter = new AtomicLong();
        this.outstandingRequests = new AtomicLong();
        this.requestsSent = new AtomicLong();
        this.repliesReceived = new AtomicLong();
        this.duplicateRepliesReceived = new AtomicLong();
        this.orphanRepliesReceived = new AtomicLong();
        this.reconnects = new AtomicLong();
        this.inMsgs = new AtomicLong();
        this.outMsgs = new AtomicLong();
        this.inBytes = new AtomicLong();
        this.outBytes = new AtomicLong();
        this.pingCount = new AtomicLong();
        this.okCount = new AtomicLong();
        this.errCount = new AtomicLong();
        this.exceptionCount = new AtomicLong();
        this.droppedCount = new AtomicLong();
    }

    @Override
    public void setAdvancedTracking(boolean trackAdvanced) {
        this.trackAdvanced = trackAdvanced;
    }

    @Override
    public void incrementPingCount() {
        this.pingCount.incrementAndGet();
    }

    @Override
    public void incrementDroppedCount() {
        this.droppedCount.incrementAndGet();
    }

    @Override
    public void incrementOkCount() {
        this.okCount.incrementAndGet();
    }

    @Override
    public void incrementErrCount() {
        this.errCount.incrementAndGet();
    }

    @Override
    public void incrementExceptionCount() {
        this.exceptionCount.incrementAndGet();
    }

    @Override
    public void incrementRequestsSent() {
        this.requestsSent.incrementAndGet();
    }

    @Override
    public void incrementRepliesReceived() {
        this.repliesReceived.incrementAndGet();
    }

    @Override
    public void incrementDuplicateRepliesReceived() {
        this.duplicateRepliesReceived.incrementAndGet();
    }

    @Override
    public void incrementOrphanRepliesReceived() {
        this.orphanRepliesReceived.incrementAndGet();
    }

    @Override
    public void incrementReconnects() {
        this.reconnects.incrementAndGet();
    }

    @Override
    public void incrementInMsgs() {
        this.inMsgs.incrementAndGet();
    }

    @Override
    public void incrementOutMsgs() {
        this.outMsgs.incrementAndGet();
    }

    @Override
    public void incrementInBytes(long bytes) {
        this.inBytes.addAndGet(bytes);
    }

    @Override
    public void incrementOutBytes(long bytes) {
        this.outBytes.addAndGet(bytes);
    }

    @Override
    public void incrementFlushCounter() {
        this.flushCounter.incrementAndGet();
    }

    @Override
    public void incrementOutstandingRequests() {
        this.outstandingRequests.incrementAndGet();
    }

    @Override
    public void decrementOutstandingRequests() {
        this.outstandingRequests.decrementAndGet();
    }

    void registerSummaryStat(LongSummaryStatistics stats, long value) {
        if(!trackAdvanced) {
            return;
        }
        lock.lock();
        try {
            stats.accept(value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void registerRead(long bytes) {
        registerSummaryStat(readStats, bytes);
    }

    @Override
    public void registerWrite(long bytes) {
        registerSummaryStat(writeStats, bytes);
    }

    @Override
    public long getPings() {
        return this.pingCount.get();
    }

    @Override
    public long getDroppedCount() {
        return this.droppedCount.get();
    }

    @Override
    public long getOKs() {
        return this.okCount.get();
    }

    @Override
    public long getErrs() {
        return this.errCount.get();
    }

    @Override
    public long getExceptions() {
        return this.exceptionCount.get();
    }

    @Override
    public long getRequestsSent() {
        return this.requestsSent.get();
    }

    @Override
    public long getReconnects() {
        return this.reconnects.get();
    }

    @Override
    public long getInMsgs() {
        return this.inMsgs.get();
    }

    @Override
    public long getOutMsgs() {
        return this.outMsgs.get();
    }

    @Override
    public long getInBytes() {
        return this.inBytes.get();
    }

    @Override
    public long getOutBytes() {
        return this.outBytes.get();
    }

    @Override
    public long getFlushCounter() {
        return flushCounter.get();
    }

    @Override
    public long getOutstandingRequests() {
        return outstandingRequests.get();
    }

    @Override
    public long getRepliesReceived() { return repliesReceived.get(); }

    @Override
    public long getDuplicateRepliesReceived() {
        return duplicateRepliesReceived.get();
    }

    @Override
    public long getOrphanRepliesReceived() { return orphanRepliesReceived.get(); }

    void appendNumberStat(StringBuilder builder, String name, long value) {
        builder.append(name);
        builder.append(NumberFormat.getNumberInstance().format(value));
        builder.append("\n");
    }

    void appendNumberStat(StringBuilder builder, String name, double value) {
        builder.append(name);
        builder.append(NumberFormat.getNumberInstance().format(value));
        builder.append("\n");
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();

        lock.lock();
        try {
            builder.append("### Connection ###\n");
            appendNumberStat(builder, "Reconnects:                      ", this.reconnects.get());
            if (this.trackAdvanced) {
                appendNumberStat(builder, "Requests Sent:                   ", this.requestsSent.get());
                appendNumberStat(builder, "Replies Received:                ", this.repliesReceived.get());
                appendNumberStat(builder, "Duplicate Replies Received:      ", this.duplicateRepliesReceived.get());
                appendNumberStat(builder, "Orphan Replies Received:         ", this.orphanRepliesReceived.get());
                appendNumberStat(builder, "Pings Sent:                      ", this.pingCount.get());
                appendNumberStat(builder, "+OKs Received:                   ", this.okCount.get());
                appendNumberStat(builder, "-Errs Received:                  ", this.errCount.get());
                appendNumberStat(builder, "Handled Exceptions:              ", this.exceptionCount.get());
                appendNumberStat(builder, "Successful Flush Calls:          ", this.flushCounter.get());
                appendNumberStat(builder, "Outstanding Request Futures:     ", this.outstandingRequests.get());
                appendNumberStat(builder, "Dropped Messages:                ", this.droppedCount.get());
            }
            builder.append("\n");
            builder.append("### Reader ###\n");
            appendNumberStat(builder, "Messages in:                     ", this.inMsgs.get());
            appendNumberStat(builder, "Bytes in:                        ", this.inBytes.get());
            builder.append("\n");
            if (this.trackAdvanced) {
                appendNumberStat(builder, "Socket Reads:                    ", readStats.getCount());
                appendNumberStat(builder, "Average Bytes Per Read:          ", readStats.getAverage());
                appendNumberStat(builder, "Min Bytes Per Read:              ", readStats.getMin());
                appendNumberStat(builder, "Max Bytes Per Read:              ", readStats.getMax());
            }
            builder.append("\n");
            builder.append("### Writer ###\n");
            appendNumberStat(builder, "Messages out:                    ", this.outMsgs.get());
            appendNumberStat(builder, "Bytes out:                       ", this.outBytes.get());
            builder.append("\n");
            if (this.trackAdvanced) {
                appendNumberStat(builder, "Socket Writes:                   ", writeStats.getCount());
                appendNumberStat(builder, "Average Bytes Per Write:         ", writeStats.getAverage());
                appendNumberStat(builder, "Min Bytes Per Write:             ", writeStats.getMin());
                appendNumberStat(builder, "Max Bytes Per Write:             ", writeStats.getMax());
            }
        } finally {
            lock.unlock();
        }

        return builder.toString();
    }
}
