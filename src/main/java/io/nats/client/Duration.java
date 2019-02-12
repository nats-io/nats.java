package io.nats.client;

import java.util.concurrent.TimeUnit;

public class Duration implements Comparable<Duration>{
    public static final Duration ZERO = new Duration(0, TimeUnit.SECONDS);

    public static Duration between(Duration start, Duration end) {
        //convert start time to same unit as end
        Duration newStart = new Duration(end.unit.convert(start.time, start.unit), end.unit);
        return new Duration(end.time-newStart.time, end.unit);
    }

    public static Duration now() {
        return new Duration(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    private TimeUnit unit;
    private long time;

    public static Duration ofSeconds(long seconds) {
        return new Duration(seconds, TimeUnit.SECONDS);
    }

    public static Duration ofMinutes(long minutes) {
        return new Duration(minutes, TimeUnit.MINUTES);
    }

    public static Duration ofMillis(long millis) {
        return new Duration(millis, TimeUnit.MILLISECONDS);
    }

    public static Duration ofHours(long hours) {
        return new Duration(hours, TimeUnit.HOURS);
    }

    public static Duration ofDays(long days) {
        return new Duration(days, TimeUnit.DAYS);
    }

    public Duration(long time, TimeUnit unit) {
        this.time = time;
        this.unit = unit;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public void setUnit(TimeUnit unit) {
        this.unit = unit;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long toNanos() {
        return unit.toNanos(time);
    }

    public long toMillis() {
        return unit.toMillis(time);
    }

    public Duration minus(Duration d) {
        //convert other duration to same time unit as this one
        Duration other = new Duration(unit.convert(d.time, d.unit), unit);
        return new Duration(time - other.time, unit);
    }

    public Duration plus(Duration d) {
        //convert other duration to same time unit as this one
        Duration other = new Duration(unit.convert(d.time, d.unit), unit);
        return new Duration(time + other.time, unit);
    }

    public Duration plusSeconds(long seconds) {
        return plus(new Duration(seconds, TimeUnit.SECONDS));
    }

    @Override
    public int compareTo(Duration duration) {
        //convert other duration to same time unit as this one
        Duration other = new Duration(unit.convert(duration.time, duration.unit), unit);
        if(time > other.time) return 1;
        if(other.time > time) return -1;
        return 0;
    }
}
