/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.benchmark;

import io.nats.client.NUID;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A utility class for collecting and calculating benchmark metrics.
 */
public class Benchmark extends Sample {
    String name = null;
    String runId = null;
    SampleGroup pubs = new SampleGroup();
    SampleGroup subs = new SampleGroup();
    BlockingQueue<Sample> pubChannel = new LinkedBlockingQueue<Sample>();
    BlockingQueue<Sample> subChannel;

    public Benchmark() {
        // TODO Auto-generated constructor stub
    }

    public Benchmark(String name, int subCnt, int pubCnt) {
        this(name, NUID.nextGlobal(), subCnt, pubCnt);
    }

    /**
     * Initializes a Benchmark. After creating a bench call addSubSample/addPubSample. When done
     * collecting samples, call endBenchmark.
     * 
     * @param name a descriptive name for this test run
     * @param runId a unique id for this test run (typically a guid)
     * @param subCnt the number of subscribers
     * @param pubCnt the number of publishers
     */
    public Benchmark(String name, String runId, int subCnt, int pubCnt) {
        this.name = name;
        this.runId = runId;
        this.subChannel = new LinkedBlockingQueue<Sample>();
        this.pubChannel = new LinkedBlockingQueue<Sample>();
    }

    public void addPubSample(Sample sample) {
        pubChannel.add(sample);
    }

    public void addSubSample(Sample sample) {
        subChannel.add(sample);
    }

    /**
     * Closes this benchmark and calculates totals and times.
     */
    public void close() {
        while (subChannel.size() > 0) {
            subs.addSample(subChannel.poll());
        }
        while (pubChannel.size() > 0) {
            pubs.addSample(pubChannel.poll());
        }

        if (subs.hasSamples()) {
            start = subs.start;
            end = subs.end;
        } else {
            start = pubs.start;
            end = pubs.end;
        }

        end = Math.min(end, subs.end);
        end = Math.min(end, pubs.end);

        msgBytes = pubs.msgBytes + subs.msgBytes;
        ioBytes = pubs.ioBytes + subs.ioBytes;
        msgCnt = pubs.msgCnt + subs.msgCnt;
        jobMsgCnt = pubs.jobMsgCnt + subs.jobMsgCnt;
    }

    /**
     * Creates the output report.
     * 
     * @return the report as a String.
     */
    public String report() {
        StringBuffer sb = new StringBuffer();
        String indent = "";
        if (pubs.hasSamples() && subs.hasSamples()) {
            sb.append(String.format("%s Pub/Sub stats: %s\n", name, this));
            indent += " ";
        }
        if (pubs.hasSamples()) {
            String maybeTitle = "";
            if (!subs.hasSamples()) {
                maybeTitle = name + " ";
            }
            sb.append(String.format("%s%sPub stats: %s\n", indent, maybeTitle, pubs));
            if (pubs.samples.size() > 1) {
                for (Sample stat : pubs.samples) {
                    sb.append(String.format("%s [%d] %s (%d msgs)\n", indent,
                            pubs.samples.indexOf(stat) + 1, stat, stat.jobMsgCnt));
                }
                sb.append(String.format("%s %s\n", indent, pubs.statistics()));
            }
        }
        if (subs.hasSamples()) {
            String maybeTitle = "";
            sb.append(String.format("%s%sSub stats: %s\n", indent, maybeTitle, subs));
            if (subs.samples.size() > 1) {
                for (Sample stat : subs.samples) {
                    sb.append(String.format("%s [%d] %s (%d msgs)\n", indent,
                            subs.samples.indexOf(stat) + 1, stat, stat.jobMsgCnt));
                }
                sb.append(String.format("%s %s\n", indent, subs.statistics()));
            }
        }
        return sb.toString();
    }

    /**
     * Returns a list of text lines for output to a CSV file.
     * 
     * @return a list of text lines for output to a CSV file
     */
    public List<String> csv() {
        List<String> lines = new ArrayList<String>();
        String header =
                "#RunID, ClientID, MsgCount, MsgBytes, MsgsPerSec, BytesPerSec, DurationSecs";
        lines.add(header);
        SampleGroup[] groups = new SampleGroup[] { subs, pubs };
        String pre = "S";
        int i = 0;
        for (SampleGroup grp : groups) {
            if (i++ == 1) {
                pre = "P";
            }
            int j = 0;
            for (Sample stat : grp.samples) {
                String line = String.format("%s,%s%d,%d,%d,%d,%f,%f", runId, pre, j, stat.msgCnt,
                        stat.msgBytes, stat.rate(), stat.throughput(),
                        (double) stat.duration() / 1000000000.0);
                lines.add(line);
            }
        }
        return lines;
    }
}
