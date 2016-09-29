/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.nats.client.Parser.MsgArg;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Arrays;

@Category(UnitTest.class)
public class MessageTest {
    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    @Test
    public void testMessage() {
        byte[] payload = "This is a message payload.".getBytes();
        String subj = "foo";
        String reply = "bar";
        Message msg = new Message();
        msg.setData(null, 0, payload.length);
        msg.setData(payload, 0, payload.length);
        msg.setReplyTo(reply);
        msg.setSubject(subj);
    }

    @Test
    public void testMessageStringStringByteArray() {
        Message msg = new Message("foo", "bar", "baz".getBytes());
        assertEquals("foo", msg.getSubject());
        assertEquals("bar", msg.getReplyTo());
        assertTrue(Arrays.equals("baz".getBytes(), msg.getData()));
    }

    @Test(expected = NullPointerException.class)
    public void testNullSubject() {
        new Message(null, "bar", "baz".getBytes());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWhitespaceSubject() {
        new Message(" \t", "bar", "baz".getBytes());
    }

    @Test
    public void testMsgArgConstructor() {
        final byte[] subj = "foo".getBytes();
        final byte[] reply = "bar".getBytes();
        final byte[] payload = "Hello World".getBytes();
        final SubscriptionImpl sub = mock(SubscriptionImpl.class);

        MsgArg ma = new Parser(new ConnectionImpl()).new MsgArg();
        ma.subject = ByteBuffer.allocate(subj.length);
        ma.subject.put(subj);
        ma.reply = ByteBuffer.allocate(reply.length);
        ma.reply.put(reply);
        ma.size = payload.length;

        Message msg = Mockito.spy(new Message(ma, sub, payload, 0, ma.size));
        assertArrayEquals(payload, msg.getData());
        assertArrayEquals(subj, msg.getSubjectBytes());
        assertEquals(new String(subj), msg.getSubject());
        assertArrayEquals(reply, msg.getReplyToBytes());
        assertEquals(new String(reply), msg.getReplyTo());

        // Now test for an exception
        // PowerMockito.mockStatic(Message.class);
        // createPartialMock(Message.class, "sysArrayCopy");
        // PowerMockito.doThrow(new ArrayIndexOutOfBoundsException()).when(Message.class);
        // Message.sysArrayCopy(any(byte[].class), anyInt(), any(byte[].class), anyInt(), anyInt());
        // Message msg2 = new Message(ma, sub, payload, 0, ma.size);
    }

    @Test
    public void testMessageByteArrayLongStringStringSubscriptionImpl() {
        byte[] payload = "This is a message payload.".getBytes();
        String subj = "foo";
        String reply = "bar";

        Message msg = new Message(payload, payload.length, subj, reply, null);
        assertEquals(subj, msg.getSubject());
        assertEquals(reply, msg.getReplyTo());
        assertTrue(Arrays.equals(payload, msg.getData()));
    }

    @Test
    public void testGetData() {
        byte[] payload = "This is a message payload.".getBytes();
        String subj = "foo";
        String reply = "bar";

        Message msg = new Message(payload, payload.length, subj, reply, null);
        assertEquals(subj, msg.getSubject());
        assertEquals(reply, msg.getReplyTo());
        assertTrue(Arrays.equals(payload, msg.getData()));

    }

    // @Test
    // public void testGetSubject() {
    // fail("Not yet implemented"); // TODO
    // }

    // @Test
    // public void testSetSubject() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetReplyTo() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testSetReplyTo() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetSubscription() {
    // fail("Not yet implemented"); // TODO
    // }

    @Test
    public void testSetData() {
        byte[] data = "hello".getBytes();
        Message msg = new Message();
        msg.setData(data);
    }

    @Test
    public void testToString() {
        byte[] data = "hello".getBytes();
        Message msg = new Message();
        msg.setData(data);
        msg.setSubject("foo");
        msg.setReplyTo("bar");
        assertEquals("{Subject=foo;Reply=bar;Payload=<hello>}", msg.toString());

        String longPayload = "this is a really long message that's easily over 32 "
                + "characters long so it will be truncated.";
        data = longPayload.getBytes();
        msg.setData(data);
        assertEquals("{Subject=foo;Reply=bar;Payload="
                + "<this is a really long message th60 more bytes>}", msg.toString());
    }

}
