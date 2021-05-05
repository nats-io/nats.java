package io.nats.examples.jsmulti;

import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;

class Stats {
    private static final String REPORT_SEP_LINE = "| ---------- | ----------------- | --------------- | ----------------------- | ---------------- |";
    private static final long HUMAN_BYTES_BASE = 1024;
    private static final String[] HUMAN_BYTES_UNITS = new String[] {"b", "kb", "mb", "gb", "tb", "pb", "eb"};
    private static final String ZEROS = "000000";

    private double elapsed = 0;
    private double bytes = 0;
    private int messageCount = 0;
    private long now;

    void start() {
        now = System.nanoTime();
    }

    void stop() {
        elapsed += System.nanoTime() - now;
    }

    void count(long bytes) {
        messageCount++;
        this.bytes += bytes;
    }

    void stopAndCount(long bytes) {
        elapsed += System.nanoTime() - now;
        count(bytes);
    }

    static void report(Stats stats, String label, boolean header, boolean footer) {
        double elapsed = stats.elapsed / 1e6;
        double messagesPerSecond = stats.elapsed == 0 ? 0 : stats.messageCount * 1e9 / stats.elapsed;
        double bytesPerSecond = 1e9 * (stats.bytes)/(stats.elapsed);
        if (header) {
            System.out.println("\n" + REPORT_SEP_LINE);
            System.out.println("|            |             count |            time |                msgs/sec |        bytes/sec |");
            System.out.println(REPORT_SEP_LINE);
        }
        System.out.printf("| %-10s | %12s msgs | %12s ms | %14s msgs/sec | %12s/sec |\n", label,
                format(stats.messageCount),
                format3(elapsed),
                format3(messagesPerSecond),
                humanBytes(bytesPerSecond));

        if (footer) {
            System.out.println(REPORT_SEP_LINE);
        }
    }

    static void report(List<Stats> statList) {
        Stats total = new Stats();
        int x = 0;
        for (Stats stats : statList) {
            report(stats, "Thread " + (++x), x == 1, false);
            total.elapsed = Math.max(total.elapsed, stats.elapsed);
            total.messageCount += stats.messageCount;
            total.bytes += stats.bytes;
        }

        System.out.println(REPORT_SEP_LINE);
        report(total, "Total", false, true);
    }

    static String humanBytes(double bytes) {
        if (bytes < HUMAN_BYTES_BASE) {
            return String.format("%.2f b", bytes);
        }
        int exp = (int) (Math.log(bytes) / Math.log(HUMAN_BYTES_BASE));
        return String.format("%.2f %s", bytes / Math.pow(HUMAN_BYTES_BASE, exp), HUMAN_BYTES_UNITS[exp]);
    }

    static String format(Number s) {
        return NumberFormat.getNumberInstance(Locale.getDefault()).format(s);
    }

    static String format3(Number s) {
        String f = format(s);
        int at = f.indexOf('.');
        if (at == 0) {
            return f + "." + ZEROS.substring(0, 3);
        }
        return (f + ZEROS).substring(0, at + 3 + 1);
    }
}
