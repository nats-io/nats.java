/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.benchmark;

import static io.nats.benchmark.Utils.humanBytes;

import io.nats.client.Connection;
import io.nats.client.Statistics;
import java.text.DecimalFormat;

public class Sample {
    int jobMsgCnt;
    long msgCnt;
    long msgBytes;
    long ioBytes;
    long start;
    long end;

    static final double BILLION = 1000000000.0;

    Sample() {
    }

    /**
     * Constructs a new benchmark sample.
     *
     * @param jobCount the number of messages that were sent or received
     * @param msgSize  the size in bytes of each messages
     * @param start    the start time in nanoseconds
     * @param end      the end time in nanoseconds
     * @param nc       the NATs connection
     */
    public Sample(int jobCount, int msgSize, long start, long end, Connection nc) {
        this.jobMsgCnt = jobCount;
        this.start = start;
        this.end = end;
        this.msgBytes = msgSize * jobCount;
        Statistics stats = nc.getStats();
        this.msgCnt = stats.getOutMsgs() + stats.getInMsgs();
        this.ioBytes = stats.getOutBytes() + stats.getInBytes();
    }

    /**
     * Duration that the sample was active (ns).
     *
     * @return Duration that the sample was active (ns)
     */
    public long duration() {
        return end - start;
    }

    /**
     * Throughput of bytes per second.
     *
     * @return throughput of bytes per second
     */
    public double throughput() {
        return (double) msgBytes / (duration() / BILLION);
    }

    /**
     * Rate of messages in the job per second.
     *
     * @return rate of messages in the job per second.
     */
    public long rate() {
        return (long) ((double) jobMsgCnt / (duration() / BILLION));
    }

    public double seconds() {
        return (double) duration() / BILLION;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public String toString() {
        DecimalFormat formatter = new DecimalFormat("#,###");
        String rate = formatter.format(rate());
        String throughput = humanBytes(throughput(), false);
        return String.format("%s msgs/sec ~ %s/sec", rate, throughput);
    }
}
