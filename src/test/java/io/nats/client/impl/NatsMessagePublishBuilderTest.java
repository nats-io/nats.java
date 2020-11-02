package io.nats.client.impl;

import io.nats.client.Message;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class NatsMessagePublishBuilderTest {

    @Test
    public void testBuilderHPUB() {

        Headers headers = new Headers();
        headers.put("header1", "value1.1");
        headers.add("header1", "value1.2");
        headers.put("header2", "value2.1");

        Message message = starterBuilder().headers(headers).build();

        assertStarter(message, false);

        assertNotNull(message.getHeaders());
        assertEquals(2, message.getHeaders().size());
        assertEquals(2, message.getHeaders().values("header1").size());
        assertEquals(1, message.getHeaders().values("header2").size());

        assertMessageString(message,
                "HPUB subject replyTo 59 64\r\n",
                "header1: value1.1\r\n",
                "header1: value1.2\r\n",
                "header2: value2.1\r\n",
                "\r\n\r\n"
        );
    }

    private void assertStarter(Message message, boolean expectedUtfMode) {
        assertEquals("subject", message.getSubject());
        assertEquals("replyTo", message.getReplyTo());
        assertEquals("Hello", new String(message.getData(), StandardCharsets.UTF_8));
        assertEquals(expectedUtfMode, message.isUtf8mode());
    }

    @Test
    public void testBuilderPubUTF8() {
        Message message = starterBuilder().utf8mode(true).build();
        assertStarter(message, true);
        assertMessageString(message, "PUB subject replyTo 5");
    }

    @Test
    public void testBuilderPubNoUTF8() {
        Message message = starterBuilder().build(); // default is utf8Mode false
        assertStarter(message, false);
        assertMessageString(message, "PUB subject replyTo 5");

        message = starterBuilder().utf8mode(false).build();
        assertStarter(message, false);
        assertMessageString(message, "PUB subject replyTo 5");
    }

    private void assertMessageString(Message message, String messageStart, String... contains) {
        String messageString = new String(message.getProtocolBytes(), StandardCharsets.UTF_8);
        assertTrue(messageString.startsWith(messageStart));
        if (contains != null) {
            for (String c : contains) {
                assertTrue(messageString.contains(c));
            }
        }
    }

    private NatsMessage.PublishBuilder starterBuilder() {
        return new NatsMessage.PublishBuilder()
                .subject("subject")
                .replyTo("replyTo")
                .data("Hello", StandardCharsets.UTF_8)
                .maxPayload(10000L);
    }
}