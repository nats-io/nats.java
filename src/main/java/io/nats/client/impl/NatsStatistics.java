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

    final private boolean trackAdvanced;

    public NatsStatistics(boolean trackAdvanced) {
        this.trackAdvanced = trackAdvanced;
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

    void incrementPingCount() {
        this.pingCount.incrementAndGet();
    }

    void incrementDroppedCount() {
        this.droppedCount.incrementAndGet();
    }

    void incrementOkCount() {
        this.okCount.incrementAndGet();
    }

    void incrementErrCount() {
        this.errCount.incrementAndGet();
    }

    void incrementExceptionCount() {
        this.exceptionCount.incrementAndGet();
    }

    void incrementRequestsSent() {
        this.requestsSent.incrementAndGet();
    }

    void incrementRepliesReceived() {
        this.repliesReceived.incrementAndGet();
    }

    void incrementDuplicateRepliesReceived() {
        this.duplicateRepliesReceived.incrementAndGet();
    }

    void incrementOrphanRepliesReceived() {
        this.orphanRepliesReceived.incrementAndGet();
    }

    void incrementReconnects() {
        this.reconnects.incrementAndGet();
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

    void incrementOutMsgsAndBytes(long bytes) {
        incrementOutMsgs();
        incrementOutBytes(bytes);
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

    void registerRead(long bytes) {
        registerSummaryStat(readStats, bytes);
    }

    void registerWrite(long bytes) {
        registerSummaryStat(writeStats, bytes);
    }

    public long getPings() {
        return this.pingCount.get();
    }

    public long getDroppedCount() {
        return this.droppedCount.get();
    }

    public long getOKs() {
        return this.okCount.get();
    }

    public long getErrs() {
        return this.errCount.get();
    }

    public long getExceptions() {
        return this.exceptionCount.get();
    }

    public long getReconnects() {
        return this.reconnects.get();
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

    long getFlushCounter() {
        return flushCounter.get();
    }

    long getOutstandingRequests() {
        return outstandingRequests.get();
    }

    public long getRepliesReceived() { return repliesReceived.get(); }

    public long getDuplicateRepliesReceived() {
        return duplicateRepliesReceived.get();
    }

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