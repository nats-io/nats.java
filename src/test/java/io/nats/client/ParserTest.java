/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.nats.client.ConnectionImpl.Control;
import io.nats.client.Parser.NatsOp;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.concurrent.TimeoutException;

@Category(UnitTest.class)
public class ParserTest {
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
    public void testParseControl() {
        // Control c = new Control();
        Options o = new Options();
        ConnectionImpl conn = new ConnectionImpl(o);
        Parser parser = new Parser(conn);

        Control c = null;

        NatsOp.valueOf(NatsOp.OP_START.toString());

        // Test with NULL line
        c = conn.new Control(null);
        assertTrue(c.op == null);
        assertTrue(c.args == null);

        // Test line with single op
        c = conn.new Control("op");
        assertNotNull(c.op);
        assertEquals(c.op, "op");
        assertNull(c.args);

        // Test line with trailing spaces
        c = conn.new Control("op   ");
        assertNotNull(c.op);
        assertEquals(c.op, "op");
        assertNull(c.args);

        // Test line with op and args
        c = conn.new Control("op    args");
        assertNotNull(c.op);
        assertEquals(c.op, "op");
        assertNotNull(c.args);
        assertEquals(c.args, "args");

        // Test line with op and args and trailing spaces
        c = conn.new Control("op   args  ");
        assertNotNull(c.op);
        assertEquals(c.op, "op");
        assertNotNull(c.args);
        assertEquals(c.args, "args");

        // Test line with op and args args
        c = conn.new Control("op   args  args   ");
        assertNotNull(c.op);
        assertEquals(c.op, "op");
        assertNotNull(c.args);
        assertEquals(c.args, "args  args");

    }

    @Test
    public void testParseGoodLines() {
        Parser parser = null;
        String[] goodLines = {
                // OP_PLUS_OK
                "+OK\r\n",
                // OP_PLUS_PING
                "PING\r\n",
                // OP_PLUS_PONG
                "PONG\r\n", "MSG  foo 1 0\r\n\r\n", "MSG \tfoo 1 0\r\n\r\n",
                "MSG \tfoo 1 5\r\nHello\r\n",
                // MSG_END default (not an error)
                "MSG \tfoo 1 6\r\nHello2\r\t" };
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            parser = c.parser;
            try (Subscription sub = c.subscribeSync("foo")) {
                for (String s : goodLines) {
                    // ConnectionImpl.printSubs(c);
                    byte[] buffer = s.getBytes();
                    try {
                        parser.parse(buffer, buffer.length);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("Should not have thrown an exception for [" + s + "]: "
                                + e.getMessage());
                    }
                }
            }
        } // ConnectionImpl
        catch (IOException | TimeoutException e1) {
            // TODO Auto-generated catch block
            fail(e1.getMessage());
        }
    }

    // Tests OP_MINUS_ERR, OP_MINUS_ERR_SPC, MINUS_ERR_ARG
    @Test
    public void testMinusErrDoesNotThrow() {
        Parser parser;
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            parser = c.parser;
            String s = String.format("-ERR %s\r\n", Constants.SERVER_ERR_AUTH_VIOLATION);
            try {
                byte[] b = s.getBytes();
                parser.parse(b, b.length);
            } catch (Exception e) {
                e.printStackTrace();
                fail("Should not have thrown an exception for [" + s + "]");
            }

            s = "-ERR  'A boring error'\r\n";
            try {
                byte[] b = s.getBytes();
                parser.parse(b, b.length);
            } catch (Exception e) {
                fail("Should not have thrown an exception for [" + s + "]");
            }

        } catch (IOException | TimeoutException e) {
            // From the connection only
            fail(e.getMessage());
        } // ConnectionImpl
    }

    @Test
    public void testParseBadLines() {
        Parser parser = null;

        String[] badLines = {
                // OP_START default
                "QQ more\r\n",
                // OP_M default
                "MQ is bad\r\n",
                // OP_MS default
                "MSP is bad\r\n",
                // OP_MSG default
                "MSGP is bad\r\n",
                // OP_PLUS default
                "+FOO\r\n",
                // OP_PLUS_O default
                "+ON\r\n",
                // OP_MINUS default
                "-BAD\r\n",
                // OP_MINUS_E default
                "-ELF\r\n",
                // OP_MINUS_ER default
                "-ERP\r\n",
                // OP_MINUS_ERR default
                "-ERRS\r\n",
                // OP_P default
                "PEM\r\n",
                // OP_PO default
                "POP\r\n",
                // OP_PON default
                "PONY\r\n",
                // OP_PI default
                "PIP\r\n",
                // OP_PIN default
                "PINT\r\n",
                // OP_PING default
                // "PING\r\t\n",
                // OP_PONG default
                // "PONG\r\t\n",
                // state default
                "Z\r\n" };

        try (ConnectionImpl nc =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {

            parser = nc.parser;
            boolean exThrown = false;

            for (String s : badLines) {
                exThrown = false;
                byte[] buffer = s.getBytes();
                try {
                    parser.parse(buffer, buffer.length);
                } catch (ParseException e) {
                    assertTrue("Wrong exception type. Should have thrown ParseException",
                            e instanceof ParseException);
                    exThrown = true;
                }
                assertTrue("Should have thrown ParseException for " + s, exThrown);
                // Reset to OP_START for next line
                nc.ps.state = NatsOp.OP_START;
            }

        } // ConnectionImpl
        catch (IOException | TimeoutException e1) {
            fail(e1.getMessage());
        }
    }

    @Test
    public void testLargeArgs() {
        Parser parser = null;
        int payloadSize = 66000;
        char[] buf = new char[payloadSize];
        for (int i = 0; i < payloadSize - 2; i++) {
            buf[i] = 'A';
        }
        buf[buf.length - 2] = '\r';
        buf[buf.length - 1] = '\n';

        String msg = String.format("MSG foo 1 %d\r\n", payloadSize);
        byte[] msgBytes = msg.getBytes();
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {

            parser = c.parser;
            try (Subscription sub = c.subscribeSync("foo")) {
                try {
                    parser.parse(msgBytes, msgBytes.length);
                } catch (Exception e) {
                    fail(e.getMessage());
                }

            }
        } // ConnectionImpl
        catch (IOException | TimeoutException e1) {
            fail(e1.getMessage());
        }
    }

    @Test
    public void TestParserSplitMsg() {

        try (ConnectionImpl nc =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            // nc.ps = &parseState{}
            byte[] buf = null;

            nc.parser.ps = nc.parser.new ParseState();
            boolean exThrown = false;
            buf = "MSG a\r\n".getBytes();
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                exThrown = true;
            }
            assertTrue(exThrown);

            nc.parser.ps = nc.parser.new ParseState();
            exThrown = false;
            buf = "MSG a b c\r\n".getBytes();
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                exThrown = true;
            }
            assertTrue(exThrown);

            nc.parser.ps = nc.parser.new ParseState();

            int expectedCount = 1;
            int expectedSize = 3;

            assertEquals(0, nc.getStats().getInMsgs());
            assertEquals(0, nc.getStats().getInBytes());

            buf = "MSG a".getBytes();
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                fail("Parser error: " + e.getMessage());
            }
            if (nc.parser.ps.argBuf == null) {
                fail("Arg buffer should have been created");
            }

            buf = " 1 3\r\nf".getBytes();
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong msg size: ", 3, nc.parser.ps.ma.size);
            assertEquals("Wrong sid: ", 1, nc.parser.ps.ma.sid);
            assertEquals("Wrong subject: ", "a", Parser.bufToString(nc.parser.ps.ma.subject));
            assertNotNull("Msg buffer should have been created", nc.parser.ps.msgBuf);

            buf = "oo\r\n".getBytes();
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }
            assertEquals("Wrong #msgs: ", expectedCount, nc.getStats().getInMsgs());
            assertEquals("Wrong #bytes: ", expectedSize, nc.getStats().getInBytes());
            assertNull("Buffers should be null now", nc.parser.ps.argBuf);
            assertNull("Buffers should be null now", nc.parser.ps.msgBuf);

            buf = "MSG a 1 3\r\nfo".getBytes();
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }
            assertEquals("Wrong msg size: ", 3, nc.parser.ps.ma.size);
            assertEquals("Wrong sid: ", 1, nc.parser.ps.ma.sid);
            assertEquals("Wrong subject: ", "a", new String(nc.parser.ps.ma.subject.array(), 0,
                    nc.parser.ps.ma.subject.limit()));
            assertNotNull("Msg buffer should have been created", nc.parser.ps.msgBuf);
            assertNotNull("Arg buffer should have been created", nc.parser.ps.argBuf);

            expectedCount++;
            expectedSize += 3;

            buf = "o\r\n".getBytes();
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong #msgs: ", expectedCount, nc.getStats().getInMsgs());
            assertEquals("Wrong #bytes: ", expectedSize, nc.getStats().getInBytes());
            assertNull("Buffers should be null now", nc.parser.ps.argBuf);
            assertNull("Buffers should be null now", nc.parser.ps.msgBuf);

            buf = "MSG a 1 6\r\nfo".getBytes();
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong msg size: ", 6, nc.parser.ps.ma.size);
            assertEquals("Wromg sid: ", 1, nc.parser.ps.ma.sid);
            assertEquals("Wrong subject: ", "a", Parser.bufToString(nc.parser.ps.ma.subject));
            assertNotNull("Msg buffer should have been created", nc.parser.ps.msgBuf);
            assertNotNull("Arg buffer should have been created", nc.parser.ps.argBuf);

            buf = "ob".getBytes();
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            expectedCount++;
            expectedSize += 6;

            buf = "ar\r\n".getBytes();
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong #msgs: ", expectedCount, nc.getStats().getInMsgs());
            assertEquals("Wrong #bytes: ", expectedSize, nc.getStats().getInBytes());
            assertNull("Buffers should be null now", nc.parser.ps.argBuf);
            assertNull("Buffers should be null now", nc.parser.ps.msgBuf);

            // Let's have a msg that is bigger than the parser's scratch size.
            // Since we prepopulate the msg with 'foo', adding 3 to the size.
            int msgSize = nc.parser.ps.msgBufStore.length + 100 + 3;
            buf = String.format("MSG a 1 b %d\r\nfoo", msgSize).getBytes();
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong msg size: ", msgSize, nc.parser.ps.ma.size);
            assertEquals("Wrong sid: ", 1, nc.parser.ps.ma.sid);
            assertEquals("Wrong subject: ", "a", Parser.bufToString(nc.parser.ps.ma.subject));
            assertEquals("Wrong reply: ", "b", Parser.bufToString(nc.parser.ps.ma.reply));
            assertNotNull("Msg buffer should have been created", nc.parser.ps.msgBuf);
            assertNotNull("Arg buffer should have been created", nc.parser.ps.argBuf);

            expectedCount++;
            expectedSize += msgSize;

            int bufSize = msgSize - 3;

            buf = new byte[bufSize];
            for (int i = 0; i < bufSize; i++) {
                buf[i] = (byte) ('a' + (i % 26));
            }
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong state: ", nc.parser.ps.state, Parser.NatsOp.MSG_PAYLOAD);
            assertEquals("Wrong msg size: ", msgSize, nc.parser.ps.ma.size);
            assertEquals("Wrong msg size: ", nc.parser.ps.msgBuf.limit(), nc.parser.ps.ma.size);
            // Check content:
            byte[] tmp = new byte[3];
            ByteBuffer tmpBuf = nc.parser.ps.msgBuf.duplicate();
            tmpBuf.rewind();
            tmpBuf.get(tmp);
            assertEquals("Wrong msg content: ", "foo", new String(tmp));
            for (int k = 3; k < nc.ps.ma.size; k++) {
                assertEquals("Wrong msg content: ", (byte) ('a' + ((k - 3) % 26)), tmpBuf.get(k));
            }

            buf = "\r\n".getBytes();
            try {
                nc.parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong #msgs: ", expectedCount, nc.getStats().getInMsgs());
            assertEquals("Wrong #bytes: ", expectedSize, nc.getStats().getInBytes());
            assertNull("Buffers should be null now", nc.parser.ps.argBuf);
            assertNull("Buffers should be null now", nc.parser.ps.msgBuf);
            assertEquals("Wrong state: ", nc.parser.ps.state, Parser.NatsOp.OP_START);
        } catch (IOException | TimeoutException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail(e.getMessage());

        } // Connection
    } // testParserSplitMsg
}

