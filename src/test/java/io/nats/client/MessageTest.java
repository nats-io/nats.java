/*
 *  Copyright (c) 2015-2017 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.nats.client.Parser.MsgArg;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;
import java.util.Arrays;

@Category(UnitTest.class)
public class MessageTest extends BaseUnitTest {

    @Mock
    private AsyncSubscriptionImpl subMock;

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
    }

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

        MsgArg ma = new MsgArg();
        ma.subject = ByteBuffer.allocate(subj.length);
        ma.subject.put(subj);
        ma.reply = ByteBuffer.allocate(reply.length);
        ma.reply.put(reply);
        ma.size = payload.length;

        Message msg = null;

        msg = Mockito.spy(new Message(ma, sub, payload, 0, ma.size));

        assertArrayEquals(payload, msg.getData());
        assertArrayEquals(subj, msg.getSubjectBytes());
        assertEquals(new String(subj), msg.getSubject());
        assertArrayEquals(reply, msg.getReplyToBytes());
        assertEquals(new String(reply), msg.getReplyTo());
    }

    @Test
    public void testMsgArgConstructorSourceBufferTooSmall() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(containsString("smaller than requested copy length"));
        final byte[] subj = "foo".getBytes();
        final byte[] reply = "bar".getBytes();
        final byte[] payload = "Hello World".getBytes();
        final SubscriptionImpl sub = mock(SubscriptionImpl.class);

        MsgArg ma = new MsgArg();
        ma.subject = ByteBuffer.allocate(subj.length);
        ma.subject.put(subj);
        ma.reply = ByteBuffer.allocate(reply.length);
        ma.reply.put(reply);
        ma.size = payload.length;

        int length = payload.length + 4;

        new Message(ma, sub, payload, 0, length);

    }

    @Test
    public void testMsgArgConstructorCopyLengthMismatch() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(containsString("larger than ma.size"));
        final byte[] subj = "foo".getBytes();
        final byte[] reply = "bar".getBytes();
        final byte[] payload = "Hello World".getBytes();
        final SubscriptionImpl sub = mock(SubscriptionImpl.class);

        MsgArg ma = new MsgArg();
        ma.subject = ByteBuffer.allocate(subj.length);
        ma.subject.put(subj);
        ma.reply = ByteBuffer.allocate(reply.length);
        ma.reply.put(reply);
        ma.size = payload.length - 4;
        int length = payload.length;

        new Message(ma, sub, payload, 0, length);
    }

    @Test
    public void testMessageByteArrayStringStringSubscriptionImpl() {
        byte[] payload = "This is a message payload.".getBytes();
        String subj = "foo";
        String reply = "bar";

        Message msg = new Message(payload, subj, reply, null);
        assertEquals(subj, msg.getSubject());
        assertEquals(reply, msg.getReplyTo());
        assertTrue(Arrays.equals(payload, msg.getData()));
    }

    @Test
    public void testGetData() {
        byte[] payload = "This is a message payload.".getBytes();
        String subj = "foo";
        String reply = "bar";

        Message msg = new Message(payload, subj, reply, null);
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
    @Test
    public void testSetReplyTo() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Reply subject cannot be empty or whitespace.");
        Message msg = new Message();
        msg.setReplyTo("");
    }

    @Test
    public void testSetReplyToNull() {
        Message msg = new Message();
        msg.setReplyTo("foobar");
        assertEquals("foobar", msg.getReplyTo());
        msg.setReplyTo(null, 44);
        assertNull(msg.getReplyTo());
        assertNull(msg.getReplyToBytes());
    }

    @Test
    public void testGetSubscription() {
        byte[] payload = "This is a message payload.".getBytes();
        String subj = "foo";
        String reply = "bar";

        Message msg = new Message(payload, subj, reply, subMock);
        assertEquals(subMock, msg.getSubscription());
    }

    @Test
    public void testSetData() {
        byte[] data = "hello".getBytes();
        Message msg = new Message();
        msg.setData(data);
    }

    @Test
    public void testSetDataLengthMismatch() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("nats: source buffer smaller than requested copy length");
        byte[] data = "hello".getBytes();
        Message msg = new Message();
        msg.setData(data, 0, data.length + 4);
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
