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

package io.nats.benchmark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.nats.client.impl.NatsImpl;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class BenchmarkFunctionalTest {
    private static final int MSG_SIZE = 8;
    private static final int MILLION = 1000 * 1000;
    private static final int BILLION = MILLION * 1000;
    private static final double EPSILON = 1.0 / 1.0E18;

    private final long baseTime = System.nanoTime();

    /**
     * Returns a million message sample.
     *
     * @return a Sample for one million messages
     */
    private Sample millionMessagesSecondSample(int seconds) {
        int messages = MILLION * seconds;
        long start = baseTime;
        long end = start + TimeUnit.SECONDS.toNanos(seconds);

        Sample stat = new Sample(messages, MSG_SIZE, start, end, NatsImpl.createEmptyStats());
        stat.msgCnt = (long) messages;
        stat.msgBytes = (long) messages * MSG_SIZE;
        stat.ioBytes = stat.msgBytes;
        return stat;
    }

    @Test
    public void testDuration() {
        Sample stat = millionMessagesSecondSample(1);
        long duration = stat.end - stat.start;
        assertEquals(1L, TimeUnit.NANOSECONDS.toSeconds(stat.duration()));
        assertEquals(stat.duration(), duration);
    }

    @Test
    public void testHumanBytes() {
        double bytes = 999;
        assertEquals("999.00 B", Utils.humanBytes(bytes, true));

        bytes = 2099;
        assertEquals("2.10 kiB", Utils.humanBytes(bytes, true));
    }

    @Test
    public void testSeconds() {
        Sample stat = millionMessagesSecondSample(1);
        double seconds = TimeUnit.NANOSECONDS.toSeconds(stat.end - stat.start) * 1.0;
        assertEquals(1.0, seconds, EPSILON);
        assertEquals(seconds, stat.seconds(), EPSILON);
    }

    @Test
    public void testRate() {
        Sample stat = millionMessagesSecondSample(60);
        assertEquals(MILLION, stat.rate());
    }

    @Test
    public void testThroughput() {
        Sample stat = millionMessagesSecondSample(60);
        assertEquals(MILLION * MSG_SIZE, stat.throughput(), EPSILON);
    }

    @Test
    public void testToString() {
        Sample stat = millionMessagesSecondSample(60);
        assertNotNull(stat.toString());
        assertFalse(stat.toString().isEmpty());
    }

    @Test
    public void testGroupDuration() {
        SampleGroup group = new SampleGroup();
        group.addSample(millionMessagesSecondSample(1));
        group.addSample(millionMessagesSecondSample(2));
        double duration = group.end - group.start;
        assertEquals(group.duration(), duration, EPSILON);
        assertEquals(2.0, duration / BILLION, EPSILON);
    }

    @Test
    public void testGroupSeconds() {
        SampleGroup group = new SampleGroup();
        group.addSample(millionMessagesSecondSample(1));
        group.addSample(millionMessagesSecondSample(2));
        group.addSample(millionMessagesSecondSample(3));
        double seconds = (group.end - group.start) / BILLION * 1.0;
        assertEquals(group.seconds(), seconds, EPSILON);
        assertEquals(3.0, seconds, EPSILON);
    }

    @Test
    public void testGroupRate() {
        SampleGroup group = new SampleGroup();
        group.addSample(millionMessagesSecondSample(1));
        group.addSample(millionMessagesSecondSample(2));
        group.addSample(millionMessagesSecondSample(3));
        assertEquals(2 * MILLION, group.rate());
    }

    @Test
    public void testGroupThroughput() {
        SampleGroup group = new SampleGroup();
        group.addSample(millionMessagesSecondSample(1));
        group.addSample(millionMessagesSecondSample(2));
        group.addSample(millionMessagesSecondSample(3));
        assertEquals(2 * MILLION * MSG_SIZE, group.throughput(), EPSILON);
    }

    @Test
    public void testMinMaxRate() {
        SampleGroup group = new SampleGroup();
        group.addSample(millionMessagesSecondSample(1));
        group.addSample(millionMessagesSecondSample(2));
        group.addSample(millionMessagesSecondSample(3));
        assertEquals(group.minRate(), group.maxRate());
    }

    @Test
    public void testAvgRate() {
        SampleGroup group = new SampleGroup();
        group.addSample(millionMessagesSecondSample(1));
        group.addSample(millionMessagesSecondSample(2));
        group.addSample(millionMessagesSecondSample(3));
        assertEquals(group.minRate(), group.avgRate());
    }

    @Test
    public void testStdDev() {
        SampleGroup group = new SampleGroup();
        group.addSample(millionMessagesSecondSample(1));
        group.addSample(millionMessagesSecondSample(2));
        group.addSample(millionMessagesSecondSample(3));
        assertEquals(0.0, group.stdDev(), EPSILON);
    }

    @Test
    public void testBenchSetup() {
        Benchmark bench = new Benchmark("test");
        bench.addPubSample(millionMessagesSecondSample(1));
        bench.addSubSample(millionMessagesSecondSample(1));
        bench.close();
        assertNotNull(bench.getRunId());
        assertFalse(bench.getRunId().isEmpty());
        assertEquals(1, bench.getPubs().getSamples().size());
        assertEquals(1, bench.getSubs().getSamples().size());
        assertEquals(2 * MILLION, bench.msgCnt);
        assertEquals(2 * MILLION * MSG_SIZE, bench.ioBytes);
        assertEquals(1, TimeUnit.NANOSECONDS.toSeconds(bench.duration()));
    }

    /**
     * Creates a Benchmark object with test data.
     *
     * @param subs number of subscribers
     * @param pubs number of publishers
     * @return the created Benchmark
     */
    private Benchmark makeBench(int subs, int pubs) {
        Benchmark bench = new Benchmark("test");
        for (int i = 0; i < subs; i++) {
            bench.addSubSample(millionMessagesSecondSample(1));
        }
        for (int i = 0; i < pubs; i++) {
            bench.addPubSample(millionMessagesSecondSample(1));
        }
        bench.close();
        return bench;
    }

    @Test
    public void testCsv() {
        Benchmark bench = makeBench(1, 1);
        String csv = bench.csv();
        String[] lines = csv.split("\n");
        assertEquals("Expected 4 lines of output from csv()", 4, lines.length);
        String[] fields = lines[1].split(","); // First line is a title, get the header
        assertEquals("Expected 7 fields", 7, fields.length);
    }

    @Test
    public void testBenchStrings() {
        Benchmark bench = makeBench(1, 1);
        String report = bench.report();
        String[] lines = report.split("\n");
        assertEquals(3, lines.length);
        assertEquals("Expected 3 lines of output: header, pub, sub", 3, lines.length);

        bench = makeBench(2, 2);
        report = bench.report();
        lines = report.split("\n");
        String str = String
                .format("%s\nExpected 9 lines of output: header, pub header, pub x 2, stats, sub "
                        + "headers, sub x 2, stats", report);
        assertEquals(str, 9, lines.length);
    }

    @Test
    public void testMsgsPerClient() {
        List<Integer> zero = Utils.msgsPerClient(0, 0);
        assertTrue("Expected 0 length for 0 clients", zero.isEmpty());

        List<Integer> oneTwo = Utils.msgsPerClient(1, 2);
        assertTrue("Expected uneven distribution",
                oneTwo.size() == 2 && (oneTwo.get(0) + oneTwo.get(1) == 1));

        List<Integer> twoTwo = Utils.msgsPerClient(2, 2);
        assertTrue("Expected even distribution",
                twoTwo.size() == 2 && twoTwo.get(0) == 1 && twoTwo.get(1) == 1);

        List<Integer> threeTwo = Utils.msgsPerClient(3, 2);
        assertTrue("Expected uneven distribution",
                threeTwo.size() == 2 && threeTwo.get(0) == 2 && threeTwo.get(1) == 1);

    }
}
