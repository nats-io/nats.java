package io.nats.examples.jsmulti;

import io.nats.client.Message;

import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;

import static io.nats.examples.jsmulti.Constants.HDR_PUB_TIME;

public class Stats {
    public static final long HUMAN_BYTES_BASE = 1024;
    public static final String[] HUMAN_BYTES_UNITS = new String[] {"b", "kb", "mb", "gb", "tb", "pb", "eb"};
    public static final String ZEROS = "000000000";

    public static final String REPORT_SEP_LINE    = "| --------------- | ----------------- | --------------- | ------------------------ | ---------------- |";
    public static final String REPORT_LINE_HEADER = "| %-15s |             count |            time |                 msgs/sec |        bytes/sec |\n";
    public static final String REPORT_LINE_FORMAT = "| %-15s | %12s msgs | %12s ms | %15s msgs/sec | %12s/sec |\n";

    public static final String LREPORT_SEP_LINE    = "| --------------- | ------------------------ | ---------------- | ------------------------ | ---------------- | ------------------------ | ---------------- |";
    public static final String LREPORT_LINE_HEADER = "| Latency         |                 msgs/sec |        bytes/sec |                 msgs/sec |        bytes/sec |                 msgs/sec |        bytes/sec |";
    public static final String LREPORT_LINE_FORMAT = "| %-15s | %15s msgs/sec | %12s/sec | %15s msgs/sec | %12s/sec | %15s msgs/sec | %12s/sec |\n";

    public double elapsed = 0;
    public double bytes = 0;
    public int messageCount = 0;

    // latency
    public double messagePubToServerTimeElapsed = 0;
    public double messageServerToReceiverElapsed = 0;
    public double messageFullElapsed = 0;

    // timer keeping
    public long now;

    // Misc
    public String hlabel = "";

    public Stats() {}

    public Stats(String hlabel) {
        this.hlabel = hlabel == null ? "" : hlabel;
    }

    public void start() {
        now = System.nanoTime();
    }

    public void stop() {
        elapsed += System.nanoTime() - now;
    }

    public void count(long bytes) {
        messageCount++;
        this.bytes += bytes;
    }

    public void stopAndCount(long bytes) {
        elapsed += System.nanoTime() - now;
        messageCount++;
        this.bytes += bytes;
    }

    public void stopAndCount(Message m) {
        elapsed += System.nanoTime() - now;
        long mReceived = System.currentTimeMillis();
        messageCount++;
        this.bytes += m.getData().length;
        String hPubTime = m.getHeaders().getFirst(HDR_PUB_TIME);
        if (hPubTime != null) {
            long messagePubTime = Long.parseLong(hPubTime);
            long messageStampTime = m.metaData().timestamp().toInstant().toEpochMilli();
            messagePubToServerTimeElapsed += elapsedLatency(messagePubTime, messageStampTime);
            messageServerToReceiverElapsed += elapsedLatency(messageStampTime, mReceived);
            messageFullElapsed += elapsedLatency(messagePubTime, mReceived);
        }
    }

    private double elapsedLatency(double startMs, double stopMs) {
        return (stopMs - startMs) * 1_000_000D;
    }

    public static void report(Stats stats) {
        report(stats, "Total", true, true, System.out);
    }

    public static void report(Stats stats, String label, boolean header, boolean footer) {
        report(stats, label, header, footer, System.out);
    }

    public static void report(Stats stats, String tlabel, boolean header, boolean footer, PrintStream out) {
        double elapsed = stats.elapsed / 1e6;
        double messagesPerSecond = stats.elapsed == 0 ? 0 : stats.messageCount * 1e9 / stats.elapsed;
        double bytesPerSecond = 1e9 * (stats.bytes) / (stats.elapsed);
        if (header) {
            out.println("\n" + REPORT_SEP_LINE);
            out.printf(REPORT_LINE_HEADER, stats.hlabel);
            out.println(REPORT_SEP_LINE);
        }
        out.printf(REPORT_LINE_FORMAT, tlabel,
            format(stats.messageCount),
            format3(elapsed),
            format3(messagesPerSecond),
            humanBytes(bytesPerSecond));
        if (footer) {
            out.println(REPORT_SEP_LINE);
        }
    }

    public static void lreport(Stats stats, String label, boolean header, boolean footer, PrintStream out) {
        double pubMps = stats.messagePubToServerTimeElapsed == 0 ? 0 : stats.messageCount * 1e9 / stats.messagePubToServerTimeElapsed;
        double pubBps = 1e9 * (stats.bytes)/(stats.messagePubToServerTimeElapsed);
        double recMps = stats.messageServerToReceiverElapsed == 0 ? 0 : stats.messageCount * 1e9 / stats.messageServerToReceiverElapsed;
        double recBps = 1e9 * (stats.bytes)/(stats.messageServerToReceiverElapsed);
        double totMps = stats.messageFullElapsed == 0 ? 0 : stats.messageCount * 1e9 / stats.messageFullElapsed;
        double totBps = 1e9 * (stats.bytes)/(stats.messageFullElapsed);
        if (header) {
            out.println("\n" + LREPORT_SEP_LINE);
            out.println(LREPORT_LINE_HEADER);
            out.println(LREPORT_SEP_LINE);
        }
        out.printf(LREPORT_LINE_FORMAT, label,
            format3(pubMps),
            humanBytes(pubBps),
            format3(recMps),
            humanBytes(recBps),
            format3(totMps),
            humanBytes(totBps)
        );
        if (footer) {
            out.println(LREPORT_SEP_LINE);
        }
    }

    public static Stats total(List<Stats> statList) {
        Stats total = new Stats("");
        for (Stats stats : statList) {
            total.elapsed = Math.max(total.elapsed, stats.elapsed);
            total.messageCount += stats.messageCount;
            total.bytes += stats.bytes;
            total.messagePubToServerTimeElapsed = Math.max(total.messagePubToServerTimeElapsed, stats.messagePubToServerTimeElapsed);
            total.messageServerToReceiverElapsed = Math.max(total.messageServerToReceiverElapsed, stats.messageServerToReceiverElapsed);
            total.messageFullElapsed = Math.max(total.messageFullElapsed, stats.messageFullElapsed);

        }
        return total;
    }

    public static void report(List<Stats> statList) {
        report(statList, System.out);
    }

    public static void report(List<Stats> statList, PrintStream out) {
        Stats totalStats = total(statList);
        for (int x = 0; x < statList.size(); x++) {
            report(statList.get(x), "Thread " + (x+1), x == 0, false, out);
        }
        out.println(REPORT_SEP_LINE);
        report(totalStats, "Total", false, true, out);

        if (statList.get(0).messagePubToServerTimeElapsed > 0) {
            for (int x = 0; x < statList.size(); x++) {
                lreport(statList.get(x), "Thread " + (x+1), x == 0, false, out);
            }
            out.println(LREPORT_SEP_LINE);
            lreport(totalStats, "Total", false, true, out);
        }
    }

    public static String humanBytes(double bytes) {
        if (bytes < HUMAN_BYTES_BASE) {
            return String.format("%.2f b", bytes);
        }
        int exp = (int) (Math.log(bytes) / Math.log(HUMAN_BYTES_BASE));
        return String.format("%.2f %s", bytes / Math.pow(HUMAN_BYTES_BASE, exp), HUMAN_BYTES_UNITS[exp]);
    }

    public static String format(Number s) {
        return NumberFormat.getNumberInstance(Locale.getDefault()).format(s);
    }

    public static String format3(Number s) {
        if (s.longValue() >= 1_000_000_000) {
            return humanBytes(s.doubleValue());
//            return format(s.longValue() / 1_000_000_000) + "B";
        }
        String f = format(s);
        int at = f.indexOf('.');
        if (at == 0) {
            return f + "." + ZEROS.substring(0, 3);
        }
        return (f + ZEROS).substring(0, at + 3 + 1);
    }
}
