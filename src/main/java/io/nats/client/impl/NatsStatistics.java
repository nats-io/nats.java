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

import io.nats.client.StatisticsCollector;

import java.text.NumberFormat;
import java.util.LongSummaryStatistics;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

class NatsStatistics implements StatisticsCollector {
    private final ReentrantLock readStatsLock;
    private final ReentrantLock writeStatsLock;

    private final LongSummaryStatistics readStats;
    private final LongSummaryStatistics writeStats;

    private final AtomicLong flushCounter;
    private final AtomicLong outstandingRequests;
    private final AtomicLong requestsSent;
    private final AtomicLong repliesReceived;
    private final AtomicLong duplicateRepliesReceived;
    private final AtomicLong orphanRepliesReceived;
    private final AtomicLong reconnects;
    private final AtomicLong inMsgs;
    private final AtomicLong outMsgs;
    private final AtomicLong inBytes;
    private final AtomicLong outBytes;
    private final AtomicLong pingCount;
    private final AtomicLong okCount;
    private final AtomicLong errCount;
    private final AtomicLong exceptionCount;
    private final AtomicLong droppedCount;

    private boolean trackAdvanced;

    public NatsStatistics() {
        this.readStatsLock = new ReentrantLock();
        this.writeStatsLock = new ReentrantLock();

        this.readStats = new LongSummaryStatistics();
        this.writeStats = new LongSummaryStatistics();

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

    @Override
    public void registerRead(long bytes) {
        if (!trackAdvanced) {
            return;
        }

        readStatsLock.lock();
        try {
            readStats.accept(bytes);
        } finally {
            readStatsLock.unlock();
        }
    }

    @Override
    public void registerWrite(long bytes) {
        if (!trackAdvanced) {
            return;
        }

        writeStatsLock.lock();
        try {
            writeStats.accept(bytes);
        } finally {
            writeStatsLock.unlock();
        }
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

        builder.append("### Connection ###\n");
        appendNumberStat(builder, "Reconnects:                      ", this.reconnects.get());
        appendNumberStat(builder, "Requests Sent:                   ", this.requestsSent.get());
        appendNumberStat(builder, "Replies Received:                ", this.repliesReceived.get());
        if (this.trackAdvanced) {
            appendNumberStat(builder, "Duplicate Replies Received:      ", this.duplicateRepliesReceived.get());
            appendNumberStat(builder, "Orphan Replies Received:         ", this.orphanRepliesReceived.get());
        }
        appendNumberStat(builder, "Pings Sent:                      ", this.pingCount.get());
        appendNumberStat(builder, "+OKs Received:                   ", this.okCount.get());
        appendNumberStat(builder, "-Errs Received:                  ", this.errCount.get());
        appendNumberStat(builder, "Handled Exceptions:              ", this.exceptionCount.get());
        appendNumberStat(builder, "Successful Flush Calls:          ", this.flushCounter.get());
        appendNumberStat(builder, "Outstanding Request Futures:     ", this.outstandingRequests.get());
        appendNumberStat(builder, "Dropped Messages:                ", this.droppedCount.get());
        builder.append("\n");
        builder.append("### Reader ###\n");
        appendNumberStat(builder, "Messages in:                     ", this.inMsgs.get());
        appendNumberStat(builder, "Bytes in:                        ", this.inBytes.get());
        builder.append("\n");
        if (this.trackAdvanced) {
            readStatsLock.lock();
            try {
                appendNumberStat(builder, "Socket Reads:                    ", readStats.getCount());
                appendNumberStat(builder, "Average Bytes Per Read:          ", readStats.getAverage());
                appendNumberStat(builder, "Min Bytes Per Read:              ", readStats.getMin());
                appendNumberStat(builder, "Max Bytes Per Read:              ", readStats.getMax());
            } finally {
                readStatsLock.unlock();
            }
        }
        builder.append("\n");
        builder.append("### Writer ###\n");
        appendNumberStat(builder, "Messages out:                    ", this.outMsgs.get());
        appendNumberStat(builder, "Bytes out:                       ", this.outBytes.get());
        builder.append("\n");
        if (this.trackAdvanced) {
            writeStatsLock.lock();
            try {
                appendNumberStat(builder, "Socket Writes:                   ", writeStats.getCount());
                appendNumberStat(builder, "Average Bytes Per Write:         ", writeStats.getAverage());
                appendNumberStat(builder, "Min Bytes Per Write:             ", writeStats.getMin());
                appendNumberStat(builder, "Max Bytes Per Write:             ", writeStats.getMax());
            } finally {
                writeStatsLock.unlock();
            }
        }

        return builder.toString();
    }
}
