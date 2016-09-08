/**
 * 
 */
package io.nats.client.examples;

import io.nats.client.Channel;
import io.nats.client.Connection;
import io.nats.client.Statistics;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author larry
 *
 */
public class BenchUtils {

    /**
     * A utility class for collecting and calculating benchmark metrics.
     */
    public BenchUtils() {
        // TODO Auto-generated constructor stub
    }

    /**
     * A Sample for a particular client.
     */
    class Sample {
        int jobMsgCnt;
        long msgCnt;
        long msgBytes;
        long ioBytes;
        long start;
        long end;

        public Sample() {}

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
            return (double) msgBytes / TimeUnit.NANOSECONDS.toSeconds(duration());
        }

        /**
         * Rate of messages in the job per second.
         * 
         * @return rate of messages in the job per second.
         */
        public long rate() {
            return (long) ((double) jobMsgCnt / TimeUnit.NANOSECONDS.toSeconds(duration()));
        }

        public String toString() {
            DecimalFormat formatter = new DecimalFormat("#,###");
            String rate = formatter.format(rate());
            String throughput = humanBytes(throughput(), false);
            return String.format("%s msgs/sec ~ %s/sec", rate, throughput);
        }
    }

    class SampleGroup extends Sample {

        List<Sample> samples = new ArrayList<Sample>();

        SampleGroup(SampleGroup... groups) {
            for (SampleGroup g : groups) {
                for (Sample stat : g.samples) {
                    addSample(stat);
                }
            }
        }

        public void addSample(Sample stat) {
            samples.add(stat);
            if (samples.size() == 1) {
                start = stat.start;
                end = stat.end;
            }
            this.ioBytes += stat.ioBytes;
            this.jobMsgCnt += stat.jobMsgCnt;
            this.msgCnt += stat.msgCnt;
            this.msgBytes += stat.msgBytes;
            this.start = Math.min(this.start, stat.start);
            this.end = Math.max(this.end, stat.end);
        }

        /**
         * Returns the minimum of the message rates in the SampleGroup.
         * 
         * @return the minimum of the message rates in the SampleGroup
         */
        public long minRate() {
            Long min = null;
            for (Sample s : samples) {
                if (min == null) {
                    min = new Long(s.rate());
                }
                min = Math.min(min, s.rate());
            }
            return min.longValue();
        }

        /**
         * Returns the maximum of the message rates in the SampleGroup.
         * 
         * @return the maximum of the message rates in the SampleGroup
         */
        public long maxRate() {
            Long max = null;
            for (Sample s : samples) {
                if (max == null) {
                    max = new Long(s.rate());
                }
                max = Math.max(max, s.rate());
            }
            return max.longValue();
        }

        /**
         * Returns the average of the message rates in the SampleGroup.
         * 
         * @return the average of the message rates in the SampleGroup
         */
        public long avgRate() {
            long sum = 0L;
            for (Sample s : samples) {
                sum += s.rate();
            }
            return (long) (sum / (long) samples.size());
        }

        /**
         * Returns the standard deviation of the message rates in the SampleGroup.
         * 
         * @return the standard deviation of the message rates in the SampleGroup
         */
        public double stdDev() {
            double avg = avgRate();
            double sum = 0.0;
            for (Sample s : samples) {
                sum += Math.pow((double) s.rate() - avg, 2);
            }
            double variance = sum / (double) samples.size();
            return Math.sqrt(variance);
        }

        public String statistics() {
            DecimalFormat formatter = new DecimalFormat("#,###");
            DecimalFormat floatFormatter = new DecimalFormat("#,###.00");
            return String.format("min %s | avg %s | max %s | stddev %s msgs",
                    formatter.format(minRate()), formatter.format(avgRate()),
                    formatter.format(maxRate()), floatFormatter.format(stdDev()));
        }

        boolean hasSamples() {
            return !samples.isEmpty();
        }
    }

    /**
     * Benchmark to hold the various Samples organized by publishers and subscribers.
     * 
     * @author larry
     *
     */
    class Benchmark {
        String name = null;
        String runId = null;
        SampleGroup pubs = new SampleGroup();
        SampleGroup subs = new SampleGroup();
        Channel<Sample> pubChannel = new Channel<Sample>();
        Channel<Sample> subChannel;

        /**
         * Initializes a Benchmark. After creating a bench call addSubSample/addPubSample. When done
         * collecting samples, call endBenchmark
         */
        Benchmark(String name, String runId, int pubCnt, int subCnt) {
            this.name = name;
            this.runId = runId;
            this.pubChannel = new Channel<Sample>(pubCnt);
            this.subChannel = new Channel<Sample>(pubCnt);
        }

        public void addPubSample(Sample sample) {
            pubChannel.add(sample);
        }

        public void addSubSample(Sample sample) {
            subChannel.add(sample);
        }

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

        public String csv() {
            StringBuffer sb = new StringBuffer();
            String[] headers = { "#RunID", "ClientID", "MsgCount", "MsgBytes", "MsgsPerSec",
                    "BytesPerSec", "DurationSecs" };
            SampleGroup groups = new SampleGroup(subs, pubs);
            return sb.toString();
        }
    }

    /**
     * humanBytes formats bytes as a human readable string.
     * 
     * @param bytes the number of bytes
     * @param si whether to use SI units
     * @return a string representing the number of bytes in human readable string
     */
    String humanBytes(double bytes, boolean si) {
        int base = 1024;
        String[] pre = new String[] { "K", "M", "G", "T", "P", "E" };
        String post = "B";
        if (si) {
            base = 1000;
            pre = new String[] { "k", "M", "G", "T", "P", "E" };
            post = "iB";
        }
        if (bytes < (long) base) {
            return String.format("%.2f B", bytes);
        }
        int exp = (int) (Math.log(bytes) / Math.log(base));
        int index = exp - 1;
        String units = pre[index] + post;
        return String.format("%.2f %s", bytes / Math.pow((double) base, (double) exp), units);
    }

    /**
     * MsgsPerClient divides the number of messages by the number of clients and tries to distribute
     * them as evenly as possible.
     * 
     * @param numMsgs the total number of messages
     * @param numClients the total number of clients
     * @return an array of message counts
     */
    int[] msgsPerClient(int numMsgs, int numClients) {
        int[] counts = null;
        if (numClients == 0 || numMsgs == 0) {
            return counts;
        }
        counts = new int[numClients];
        int mc = numMsgs / numClients;
        for (int i = 0; i < numClients; i++) {
            counts[i] = mc;
        }
        int extra = numMsgs % numClients;
        for (int i = 0; i < extra; i++) {
            counts[i]++;
        }
        return counts;
    }
}
