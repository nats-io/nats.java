package io.nats.client.impl;

import io.nats.client.Message;
import io.nats.client.MessageBuilder;
import io.nats.client.Nats;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;


import static org.junit.Assert.*;

public class NatMessageBuilderImplTest {

    private MessageBuilder messageBuilder;

    @Before
    public void setUp() throws Exception {

        messageBuilder = Nats.messageBuilder();
    }

    @Test
    public void testBuilderHPUB() {

        LinkedHashMap<String, List<String>> headers = new LinkedHashMap<>();

        List<String> headerValue = headers.computeIfAbsent("header1", s -> new ArrayList<>());
        headerValue.add("value1");
        messageBuilder.withSubject("subject").withReplyTo("replyTo").withHpub(true).withHeaders(headers)
                .addHeader("header2", "value2").addHeader("header1", "value1.1")
        .withData("Hello".getBytes(StandardCharsets.UTF_8));

        Message message = messageBuilder.build();

        assertEquals("subject", messageBuilder.getSubject());
        assertEquals("replyTo", messageBuilder.getReplyTo());
        assertEquals(true, messageBuilder.isHpub());

        assertEquals(2, messageBuilder.getHeaders().size());
        assertEquals(2, messageBuilder.getHeaders().get("header1").size());
        assertEquals(1, messageBuilder.getHeaders().get("header2").size());

        String messageString = new String(message.getProtocolBytes(), StandardCharsets.UTF_8);

        System.out.println(messageString);
        assertTrue(messageString.startsWith("HPUB subject replyTo 55 60\r\n"));
        assertTrue(messageString.contains("header1: value1\r\n"));
        assertTrue(messageString.contains("header1: value1.1\r\n"));
        assertTrue(messageString.contains("header2: value2\r\n"));
        assertTrue(messageString.contains("\r\n\r\n"));


    }


    @Test
    public void testBuilderHPUBJustMap() {

        Map<String, List<String>> headers = new LinkedHashMap<>();

        hpubCommon(headers);


    }



    @Test
    public void testBuilderHPUBWithHashMap() {

        Map<String, List<String>> headers = new HashMap<>();

        hpubCommon(headers);


    }

    protected void hpubCommon(Map<String, List<String>> headers) {
        List<String> headerValue = headers.computeIfAbsent("header1", s -> new ArrayList<>());
        headerValue.add("value1");
        messageBuilder.withSubject("subject").withReplyTo("replyTo").withHpub(true).withHeaders(headers)
                .addHeader("header2", "value2").addHeader("header1", "value1.1")
                .withData("Hello".getBytes(StandardCharsets.UTF_8));

        Message message = messageBuilder.build();

        assertEquals("subject", messageBuilder.getSubject());
        assertEquals("replyTo", messageBuilder.getReplyTo());
        assertEquals(true, messageBuilder.isHpub());

        assertEquals(2, messageBuilder.getHeaders().size());
        assertEquals(2, messageBuilder.getHeaders().get("header1").size());
        assertEquals(1, messageBuilder.getHeaders().get("header2").size());

        String messageString = new String(message.getProtocolBytes(), StandardCharsets.UTF_8);

        System.out.println(messageString);
        assertTrue(messageString.startsWith("HPUB subject replyTo 55 60\r\n"));
        assertTrue(messageString.contains("header1: value1\r\n"));
        assertTrue(messageString.contains("header1: value1.1\r\n"));
        assertTrue(messageString.contains("header2: value2\r\n"));
        assertTrue(messageString.contains("\r\n\r\n"));


        assertEquals("Hello", new String(messageBuilder.getData(), StandardCharsets.UTF_8));
        

    }


    @Test
    public void testBuilderPubUTF8() {

        messageBuilder.withSubject("subject").withReplyTo("replyTo").withHpub(false).withUtf8mode(true)
                .withData("Hello".getBytes(StandardCharsets.UTF_8));

        Message message = messageBuilder.build();

        assertEquals("subject", messageBuilder.getSubject());
        assertEquals("replyTo", messageBuilder.getReplyTo());
        assertEquals(true, messageBuilder.isUtf8mode());


        String messageString = new String(message.getProtocolBytes(), StandardCharsets.UTF_8);

        System.out.println(messageString);
        assertTrue(messageString.startsWith("PUB subject replyTo 5"));


    }

    @Test
    public void testBuilderPubNoUTF8() {

        messageBuilder.withSubject("subject").withReplyTo("replyTo").withHpub(false).withUtf8mode(false)
                .withData("Hello".getBytes(StandardCharsets.UTF_8));

        Message message = messageBuilder.build();

        assertEquals("subject", messageBuilder.getSubject());
        assertEquals("replyTo", messageBuilder.getReplyTo());
        assertEquals(false, messageBuilder.isUtf8mode());


        String messageString = new String(message.getProtocolBytes(), StandardCharsets.UTF_8);

        System.out.println(messageString);
        assertTrue(messageString.startsWith("PUB subject replyTo 5"));


    }
}