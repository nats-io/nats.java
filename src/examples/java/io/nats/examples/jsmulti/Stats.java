package io.nats.examples.jsmulti;

import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;

public class Stats {
    public static final long HUMAN_BYTES_BASE = 1024;
    public static final String[] HUMAN_BYTES_UNITS = new String[] {"b", "kb", "mb", "gb", "tb", "pb", "eb"};
    public static final String ZEROS = "000000000";

    public static final String REPORT_SEP_LINE    = "| --------------- | ----------------- | --------------- | ------------------------ | ---------------- |";
    public static final String REPORT_LINE_HEADER = "|                 |             count |            time |                 msgs/sec |        bytes/sec |";
    public static final String REPORT_LINE_FORMAT = "| %-15s | %12s msgs | %12s ms | %15s msgs/sec | %12s/sec |\n";

    public double elapsed = 0;
    public double bytes = 0;
    public int messageCount = 0;
    public long now;

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
        count(bytes);
    }

    public static void report(Stats stats) {
        report(stats, "Total", true, true, System.out);
    }

    public static void report(Stats stats, String label, boolean header, boolean footer) {
        report(stats, label, header, footer, System.out);
    }

    public static void report(Stats stats, String label, boolean header, boolean footer, PrintStream out) {
        double elapsed = stats.elapsed / 1e6;
        double messagesPerSecond = stats.elapsed == 0 ? 0 : stats.messageCount * 1e9 / stats.elapsed;
        double bytesPerSecond = 1e9 * (stats.bytes)/(stats.elapsed);
        if (header) {
            out.println("\n" + REPORT_SEP_LINE);
            out.println(REPORT_LINE_HEADER);
            out.println(REPORT_SEP_LINE);
        }
        out.printf(REPORT_LINE_FORMAT, label,
                format(stats.messageCount),
                format3(elapsed),
                format3(messagesPerSecond),
                humanBytes(bytesPerSecond));

        if (footer) {
            out.println(REPORT_SEP_LINE);
        }
    }

    public static Stats total(List<Stats> statList) {
        Stats total = new Stats();
        for (Stats stats : statList) {
            total.elapsed = Math.max(total.elapsed, stats.elapsed);
            total.messageCount += stats.messageCount;
            total.bytes += stats.bytes;
        }
        return total;
    }

    public static void report(List<Stats> statList) {
        report(statList, System.out);
    }

    public static void report(List<Stats> statList, PrintStream out) {
        Stats total = new Stats();
        for (int x = 0; x < statList.size(); x++) {
            report(statList.get(x), "Thread " + (x+1), x == 0, false, out);
        }
        out.println(REPORT_SEP_LINE);
        report(total, "Total", false, true, out);
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
        String f = format(s);
        int at = f.indexOf('.');
        if (at == 0) {
            return f + "." + ZEROS.substring(0, 3);
        }
        return (f + ZEROS).substring(0, at + 3 + 1);
    }
}
