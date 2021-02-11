package io.nats.client;

import io.nats.client.impl.JsonUtils;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;

public class StreamState {
    private final long msgs;
    private final long bytes;
    private final long firstSeq;
    private final long lastSeq;
    private final long consumerCount;
    private final ZonedDateTime firstTime;
    private final ZonedDateTime lastTime;

    public StreamState(String json) {
        msgs = JsonUtils.readLong(json, MESSAGES_RE, 0);
        bytes = JsonUtils.readLong(json, BYTES_RE, 0);
        firstSeq = JsonUtils.readLong(json, FIRST_SEQ_RE, 0);
        lastSeq = JsonUtils.readLong(json, LAST_SEQ_RE, 0);
        consumerCount = JsonUtils.readLong(json, CONSUMER_COUNT_RE, 0);
        firstTime = JsonUtils.readDate(json, FIRST_TS_RE);
        lastTime = JsonUtils.readDate(json, LAST_TS_RE);
    }

    /**
     * Gets the message count of the stream.
     *
     * @return the message count
     */
    public long getMsgCount() {
        return msgs;
    }

    /**
     * Gets the byte count of the stream.
     *
     * @return the byte count
     */
    public long getByteCount() {
        return bytes;
    }

    /**
     * Gets the first sequence number of the stream.
     *
     * @return a sequence number
     */
    public long getFirstSequence() {
        return firstSeq;
    }

    /**
     * Gets the time stamp of the first message in the stream
     *
     * @return the first time
     */
    public ZonedDateTime getFirstTime() {
        return firstTime;
    }

    /**
     * Gets the last sequence of a message in the stream
     *
     * @return a sequence number
     */
    public long getLastSequence() {
        return lastSeq;
    }

    /**
     * Gets the time stamp of the last message in the stream
     *
     * @return the first time
     */
    public ZonedDateTime getLastTime() {
        return lastTime;
    }

    /**
     * Gets the number of consumers attached to the stream.
     *
     * @return the consumer count
     */
    public long getConsumerCount() {
        return consumerCount;
    }

    @Override
    public String toString() {
        return "StreamState{" +
                "msgs=" + msgs +
                ", bytes=" + bytes +
                ", firstSeq=" + firstSeq +
                ", lastSeq=" + lastSeq +
                ", consumerCount=" + consumerCount +
                ", firstTime=" + firstTime +
                ", lastTime=" + lastTime +
                '}';
    }
}
