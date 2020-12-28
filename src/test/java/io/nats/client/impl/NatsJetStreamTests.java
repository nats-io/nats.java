package io.nats.client.impl;

import io.nats.client.Message;
import io.nats.client.MessageMetaData;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.nats.client.impl.NatsJetStream.isValidStreamName;
import static org.junit.jupiter.api.Assertions.*;

public class NatsJetStreamTests {

    private static final String JS_REPLY_TO = "$JS.ACK.test-stream.test-consumer.1.2.3.1605139610113260000";

    @Test
    public void testJSMetaData() {
        Message msg = getJsMessage(JS_REPLY_TO);

        // two calls to msg.metaData are for coverage to test lazy initializer
        MessageMetaData jsmd = msg.metaData(); // this call takes a different path
        assertNotNull(msg.metaData()); // this call shows that the lazy will work

        assertEquals("test-stream", jsmd.getStream());
        assertEquals("test-consumer", jsmd.getConsumer());
        assertEquals(1, jsmd.deliveredCount());
        assertEquals(2, jsmd.streamSequence());
        assertEquals(3, jsmd.consumerSequence());
        assertEquals(2020, jsmd.timestamp().getYear());
        assertEquals(6, jsmd.timestamp().getMinute());
        assertEquals(113260000, jsmd.timestamp().getNano());
        assertEquals(-1, jsmd.pendingCount());

        jsmd = getJsMessage(JS_REPLY_TO + ".555").metaData();
        assertEquals(555, jsmd.pendingCount());
    }

    @Test
    public void testJSMessageCoverage() {
        Message msg = getJsMessage(JS_REPLY_TO);
        assertTrue(msg.isJetStream());

        // two calls to msg.metaData are for coverage to test lazy initializer
        assertNotNull(msg.metaData()); // this call takes a different path
        assertNotNull(msg.metaData()); // this call shows that the lazy will work

        assertThrows(IllegalArgumentException.class, () -> msg.ackSync(null));

        // coverage of isSync flag
        assertThrows(IllegalStateException.class, () -> msg.ackSync(Duration.ZERO));

        assertThrows(IllegalStateException.class, () -> msg.ackSync(Duration.ofSeconds(1)));
    }

    private NatsMessage getJsMessage(String replyTo) {
        return new NatsMessage.IncomingMessageFactory("sid", "subj", replyTo, 0, false).getMessage();
    }

    @Test
    public void testInvalidJSMessage() {
        Message m = new NatsMessage.IncomingMessageFactory("sid", "subj", "replyTo", 0, false).getMessage();
        assertFalse(m.isJetStream());
        assertThrows(IllegalStateException.class, m::ack);
        assertThrows(IllegalStateException.class, m::nak);
        assertThrows(IllegalStateException.class, () -> m.ackSync(Duration.ofSeconds(42)));
        assertThrows(IllegalStateException.class, m::inProgress);
        assertThrows(IllegalStateException.class, m::term);
    }

    @Test
    public void notJetream() {
        NatsMessage m = NatsMessage.builder().subject("test").build();
        assertThrows(IllegalStateException.class, m::ack);
        assertThrows(IllegalStateException.class, m::nak);
        assertThrows(IllegalStateException.class, () -> m.ackSync(Duration.ZERO));
        assertThrows(IllegalStateException.class, m::inProgress);
        assertThrows(IllegalStateException.class, m::term);
        assertThrows(IllegalStateException.class, m::metaData);
    }

    @Test
    public void invalidConstruction() {
        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(NatsMessage.builder().subject("test").build()));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(getJsMessage("$JS.ACK.not.enough.parts")));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(getJsMessage(JS_REPLY_TO + ".too.many.parts")));

        assertThrows(IllegalArgumentException.class,
                () -> new NatsJetStreamMetaData(getJsMessage("$JS.ZZZ.enough.parts.though.need.three.more")));
    }


    @Test
    public void constructAccountLimitImpl() {
        NatsJetStream.AccountLimitImpl impl = new NatsJetStream.AccountLimitImpl(
                "{\"max_memory\": 42, \"max_storage\": 24, \"max_streams\": 73, \"max_consumers\": 37}");

        assertEquals(42, impl.getMaxMemory());
        assertEquals(24, impl.getMaxStorage());
        assertEquals(73, impl.getMaxStreams());
        assertEquals(37, impl.getMaxConsumers());

        impl = new NatsJetStream.AccountLimitImpl("{}");
        assertEquals(-1, impl.getMaxMemory());
        assertEquals(-1, impl.getMaxStorage());
        assertEquals(-1, impl.getMaxStreams());
        assertEquals(+1, impl.getMaxConsumers());
    }

    @Test
    public void constructAccountStatsImpl() {
        NatsJetStream.AccountStatsImpl impl = new NatsJetStream.AccountStatsImpl(
                "{\"memory\": 42, \"storage\": 24, \"streams\": 73, \"consumers\": 37}");

        assertEquals(42, impl.getMemory());
        assertEquals(24, impl.getStorage());
        assertEquals(73, impl.getStreams());
        assertEquals(37, impl.getConsumers());

        impl = new NatsJetStream.AccountStatsImpl("{}");
        assertEquals(-1, impl.getMemory());
        assertEquals(-1, impl.getStorage());
        assertEquals(-1, impl.getStreams());
        assertEquals(+1, impl.getConsumers());
    }


    @Test
    public void miscCoverage() {
        assertFalse(isValidStreamName(null));
        assertFalse(isValidStreamName("no.dot"));
        assertFalse(isValidStreamName("no*star"));
        assertFalse(isValidStreamName("no>gt"));
        assertTrue(isValidStreamName("ok"));
    }
}
