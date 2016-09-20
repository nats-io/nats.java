package io.nats.benchmark;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class SampleGroup extends Sample {
    List<Sample> samples = new ArrayList<Sample>();

    SampleGroup(SampleGroup... groups) {
        for (SampleGroup g : groups) {
            for (Sample stat : g.samples) {
                addSample(stat);
            }
        }
    }

    /**
     * Adds a sample to the group.
     * 
     * @param stat the sample to add
     */
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

    /**
     * Formats the statistics for this group in a human-readable format.
     * 
     * @return the statistics for this group in a human-readable format (string)
     */
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
