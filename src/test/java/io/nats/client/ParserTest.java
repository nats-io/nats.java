/*
 *  Copyright (c) 2015-2017 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static io.nats.client.Nats.defaultOptions;
import static io.nats.client.UnitTestUtilities.newMockedConnection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import io.nats.client.ConnectionImpl.Control;
import io.nats.client.ConnectionImpl.Srv;
import io.nats.client.Parser.NatsOp;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;

@Category(UnitTest.class)
public class ParserTest extends BaseUnitTest {

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
    }

    private static final String[] testServers = {"nats://localhost:1222", "nats://localhost:1223",
            "nats://localhost:1224", "nats://localhost:1225", "nats://localhost:1226",
            "nats://localhost:1227", "nats://localhost:1228"};

    @Test
    public void testParseControl() throws Exception {
        ConnectionImpl conn = new ConnectionImpl(defaultOptions());

        Control c = null;

        NatsOp.valueOf(NatsOp.OP_START.toString());

        // Test with NULL line
        c = new Control(null);
        assertTrue(c.op == null);
        assertTrue(c.args == null);

        // Test line with single op
        c = new Control("op");
        assertNotNull(c.op);
        assertEquals(c.op, "op");
        assertNull(c.args);

        // Test line with trailing spaces
        c = new Control("op   ");
        assertNotNull(c.op);
        assertEquals(c.op, "op");
        assertNull(c.args);

        // Test line with op and args
        c = new Control("op    args");
        assertNotNull(c.op);
        assertEquals(c.op, "op");
        assertNotNull(c.args);
        assertEquals(c.args, "args");

        // Test line with op and args and trailing spaces
        c = new Control("op   args  ");
        assertNotNull(c.op);
        assertEquals(c.op, "op");
        assertNotNull(c.args);
        assertEquals(c.args, "args");

        // Test line with op and args args
        c = new Control("op   args  args   ");
        assertNotNull(c.op);
        assertEquals(c.op, "op");
        assertNotNull(c.args);
        assertEquals(c.args, "args  args");

    }

    @Test
    public void testParseGoodLines() throws Exception {
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
                "MSG \tfoo 1 6\r\nHello2\r\t"};

        try (ConnectionImpl nc = (ConnectionImpl) newMockedConnection()) {
            parser = ConnectionAccessor.getParser(nc);
            try (Subscription sub = nc.subscribeSync("foo")) {
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
    }

    @Test
    public void testParseLongWithLengthZero() {
        assertEquals(-1, Parser.parseLong(null, 0));
    }

    // Tests OP_MINUS_ERR, OP_MINUS_ERR_SPC, MINUS_ERR_ARG
    @Test
    public void testMinusErrDoesNotThrow() {
        Parser parser;
        try (ConnectionImpl nc = new ConnectionImpl(defaultOptions())) {
            parser = ConnectionAccessor.getParser(nc);
            String s = String.format("-ERR %s\r\n", Nats.SERVER_ERR_AUTH_VIOLATION);
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
                e.printStackTrace();
                fail("Should not have thrown an exception for [" + s + "]");
            }

        } // ConnectionImpl
    }

    @Test
    public void testParseBadLines() throws Exception {
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
                "Z\r\n"};

        try (ConnectionImpl nc = new ConnectionImpl(defaultOptions())) {
            parser = ConnectionAccessor.getParser(nc);
            boolean exThrown = false;

            for (String s : badLines) {
                exThrown = false;
                byte[] buffer = s.getBytes();
                try {
                    parser.parse(buffer, buffer.length);
                } catch (Exception e) {
                    assertTrue("Wrong exception type. Should have thrown ParseException",
                            e instanceof ParseException);
                    exThrown = true;
                }
                assertTrue("Should have thrown ParseException for " + s, exThrown);
                // Reset to OP_START for next line
                parser.ps.state = NatsOp.OP_START;
            }
        } // ConnectionImpl
    }

    @Test
    public void testLargeArgs() throws Exception {
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
        try (ConnectionImpl c = (ConnectionImpl) new ConnectionImpl(defaultOptions())) {
            parser = ConnectionAccessor.getParser(c);
            c.setOutputStream(mock(OutputStream.class));
            try (Subscription sub = c.subscribeSync("foo")) {
                try {
                    parser.parse(msgBytes, msgBytes.length);
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    @Test
    public void testParserLargeMsg() throws Exception {
        int size = 912000;
        byte[] preamble = String.format("MSG foo 1 %d\r\n", size).getBytes();
        int preambleLength = preamble.length;
        byte[] data = new byte[preambleLength + size + 2];
        System.arraycopy(preamble, 0, data, 0, preambleLength);
        for (int i = preambleLength; i < preambleLength + size; i++) {
            data[i] = (byte) 'A';
        }
        data[size-2] = '\r';
        data[size-1] = '\n';

        try (ConnectionImpl c = new ConnectionImpl(defaultOptions())) {
            c.setOutputStream(mock(OutputStream.class));
            try (Subscription sub = c.subscribeSync("foo")) {
                Parser parser = ConnectionAccessor.getParser(c);
                parser.parse(data, data.length);
            }
        }
    }

        @Test
    public void testParserSplitMsg() throws Exception {
        try (ConnectionImpl nc = new ConnectionImpl(defaultOptions())) {
            // nc.ps = &parseState{}
            byte[] buf = null;

            Parser parser = ConnectionAccessor.getParser(nc);

            parser.ps = new Parser.ParseState();
            boolean exThrown = false;
            buf = "MSG a\r\n".getBytes();
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                exThrown = true;
            }
            assertTrue(exThrown);

            parser.ps = new Parser.ParseState();
            exThrown = false;
            buf = "MSG a b c\r\n".getBytes();
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                exThrown = true;
            }
            assertTrue(exThrown);

            parser.ps = new Parser.ParseState();

            assertEquals(0, nc.getStats().getInMsgs());
            assertEquals(0, nc.getStats().getInBytes());

            buf = "MSG a".getBytes();
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                fail("Parser error: " + e.getMessage());
            }
            if (parser.ps.argBuf == null) {
                fail("Arg buffer should have been created");
            }

            buf = " 1 3\r\nf".getBytes();
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong msg size: ", 3, parser.ps.ma.size);
            assertEquals("Wrong sid: ", 1, parser.ps.ma.sid);
            assertEquals("Wrong subject: ", "a", Parser.bufToString(parser.ps.ma.subject));
            assertNotNull("Msg buffer should have been created", parser.ps.msgBuf);

            buf = "oo\r\n".getBytes();
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            int expectedCount = 1;
            int expectedSize = 3;

            assertEquals("Wrong #msgs: ", expectedCount, nc.getStats().getInMsgs());
            assertEquals("Wrong #bytes: ", expectedSize, nc.getStats().getInBytes());
            assertNull("Buffers should be null now", parser.ps.argBuf);
            assertNull("Buffers should be null now", parser.ps.msgBuf);

            buf = "MSG a 1 3\r\nfo".getBytes();
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }
            assertEquals("Wrong msg size: ", 3, parser.ps.ma.size);
            assertEquals("Wrong sid: ", 1, parser.ps.ma.sid);
            assertEquals("Wrong subject: ", "a", new String(parser.ps.ma.subject.array(), 0,
                    parser.ps.ma.subject.limit()));
            assertNotNull("Msg buffer should have been created", parser.ps.msgBuf);
            assertNotNull("Arg buffer should have been created", parser.ps.argBuf);

            expectedCount++;
            expectedSize += 3;

            buf = "o\r\n".getBytes();
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong #msgs: ", expectedCount, nc.getStats().getInMsgs());
            assertEquals("Wrong #bytes: ", expectedSize, nc.getStats().getInBytes());
            assertNull("Buffers should be null now", parser.ps.argBuf);
            assertNull("Buffers should be null now", parser.ps.msgBuf);

            buf = "MSG a 1 6\r\nfo".getBytes();
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong msg size: ", 6, parser.ps.ma.size);
            assertEquals("Wromg sid: ", 1, parser.ps.ma.sid);
            assertEquals("Wrong subject: ", "a", Parser.bufToString(parser.ps.ma.subject));
            assertNotNull("Msg buffer should have been created", parser.ps.msgBuf);
            assertNotNull("Arg buffer should have been created", parser.ps.argBuf);

            buf = "ob".getBytes();
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            expectedCount++;
            expectedSize += 6;

            buf = "ar\r\n".getBytes();
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong #msgs: ", expectedCount, nc.getStats().getInMsgs());
            assertEquals("Wrong #bytes: ", expectedSize, nc.getStats().getInBytes());
            assertNull("Buffers should be null now", parser.ps.argBuf);
            assertNull("Buffers should be null now", parser.ps.msgBuf);

            // Let's have a msg that is bigger than the parser's scratch size.
            // Since we prepopulate the msg with 'foo', adding 3 to the size.
            int msgSize = parser.ps.msgBufStore.length + 100 + 3;
            buf = String.format("MSG a 1 b %d\r\nfoo", msgSize).getBytes();
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong msg size: ", msgSize, parser.ps.ma.size);
            assertEquals("Wrong sid: ", 1, parser.ps.ma.sid);
            assertEquals("Wrong subject: ", "a", Parser.bufToString(parser.ps.ma.subject));
            assertEquals("Wrong reply: ", "b", Parser.bufToString(parser.ps.ma.reply));
            assertNotNull("Msg buffer should have been created", parser.ps.msgBuf);
            assertNotNull("Arg buffer should have been created", parser.ps.argBuf);

            expectedCount++;
            expectedSize += msgSize;

            int bufSize = msgSize - 3;

            buf = new byte[bufSize];
            for (int i = 0; i < bufSize; i++) {
                buf[i] = (byte) ('a' + (i % 26));
            }
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong state: ", parser.ps.state, Parser.NatsOp.MSG_PAYLOAD);
            assertEquals("Wrong msg size: ", msgSize, parser.ps.ma.size);
            assertEquals("Wrong msg size: ", parser.ps.msgBuf.limit(), parser.ps.ma.size);
            // Check content:
            byte[] tmp = new byte[3];
            ByteBuffer tmpBuf = parser.ps.msgBuf.duplicate();
            tmpBuf.rewind();
            tmpBuf.get(tmp);
            assertEquals("Wrong msg content: ", "foo", new String(tmp));
            for (int k = 3; k < parser.ps.ma.size; k++) {
                assertEquals("Wrong msg content: ", (byte) ('a' + ((k - 3) % 26)), tmpBuf.get(k));
            }

            buf = "\r\n".getBytes();
            try {
                parser.parse(buf, buf.length);
            } catch (ParseException e) {
                e.printStackTrace();
                fail("Parser error: " + e.getMessage());
            }

            assertEquals("Wrong #msgs: ", expectedCount, nc.getStats().getInMsgs());
            assertEquals("Wrong #bytes: ", expectedSize, nc.getStats().getInBytes());
            assertNull("Buffers should be null now", parser.ps.argBuf);
            assertNull("Buffers should be null now", parser.ps.msgBuf);
            assertEquals("Wrong state: ", parser.ps.state, Parser.NatsOp.OP_START);
        }
    } // testParserSplitMsg

    @Test
    public void testParserSplitMsgArgs() throws Exception {
        try (ConnectionImpl nc = new ConnectionImpl(defaultOptions())) {
            Parser parser = ConnectionAccessor.getParser(nc);

            parser.ps = new Parser.ParseState();
            byte[] buf = "MSG a".getBytes();
            parser.parse(buf);
            assertEquals(Parser.NatsOp.MSG_ARG, parser.ps.state);
            assertNotNull(parser.ps.argBuf);
            String ab = new String(parser.ps.argBuf.array(), 0, parser.ps.argBuf.position());
            assertEquals("a", ab);

            buf = ".b".getBytes();
            parser.parse(buf);
            assertEquals(Parser.NatsOp.MSG_ARG, parser.ps.state);
            assertNotNull(parser.ps.argBuf);
            ab = new String(parser.ps.argBuf.array(), 0, parser.ps.argBuf.position());
            assertEquals("a.b", ab);

            buf = ".c 1 1\r".getBytes();
            parser.parse(buf);
            assertEquals(Parser.NatsOp.MSG_ARG, parser.ps.state);
            assertNotNull(parser.ps.argBuf);
            ab = new String(parser.ps.argBuf.array(), 0, parser.ps.argBuf.position());
            assertEquals("a.b.c 1 1", ab);
            assertEquals(1, parser.ps.drop);

            buf = "\n".getBytes();
            parser.parse(buf);
            assertEquals(Parser.NatsOp.MSG_PAYLOAD, parser.ps.state);
            assertEquals("a.b.c", new String(parser.ps.ma.subject.array(), 0, parser.ps.ma.subject.limit()));
            assertEquals(1L, parser.ps.ma.sid);
            assertNotNull(parser.ps.argBuf);
        }
    }


    @Test
    public void testProcessMsgArgsErrors() {
        String tooFewArgsString = "foo bar";
        byte[] args = tooFewArgsString.getBytes();

        boolean exThrown = false;
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            Parser parser = ConnectionAccessor.getParser(c);
            parser.processMsgArgs(args, 0, args.length);
        } catch (ParseException e) {
            exThrown = true;
            String msg = String.format("Wrong msg: [%s]\n", e.getMessage());
            assertTrue(msg, e.getMessage().startsWith("nats: processMsgArgs bad number of args"));
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            assertTrue("Should have thrown ParseException", exThrown);
        }

        String badSizeString = "foo 1 -1";
        args = badSizeString.getBytes();

        exThrown = false;
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            Parser parser = ConnectionAccessor.getParser(c);
            parser.processMsgArgs(args, 0, args.length);
        } catch (ParseException e) {
            exThrown = true;
            String msg = String.format("Wrong msg: [%s]\n", e.getMessage());
            assertTrue(msg,
                    e.getMessage().startsWith("nats: processMsgArgs bad or missing size: "));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            assertTrue("Should have thrown ParseException", exThrown);
        }
    }

    @Test
    public void testProcessMsgArgsNegativeSize() throws Exception {
        thrown.expect(ParseException.class);
        thrown.expectMessage("nats: processMsgArgs bad or missing size:");

        String msg = "MSG foo 1 -100\r\n";
        byte[] msgBytes = msg.getBytes();
        try (ConnectionImpl c = new ConnectionImpl(defaultOptions())) {
            c.setOutputStream(mock(OutputStream.class));
            try (Subscription sub = c.subscribeSync("foo")) {
                Parser parser = ConnectionAccessor.getParser(c);
                parser.parse(msgBytes, msgBytes.length);
            }
        }
    }

    @Test
    public void testMsgArg() {
        Parser.MsgArg arg = new Parser.MsgArg();
        arg.subject.put("subject".getBytes());
        arg.reply.put("reply".getBytes());
        arg.sid = 22;
        arg.size = 128;

        assertNotNull(arg.toString());
        assertFalse(arg.toString().isEmpty());

        arg.subject = null;
        assertNotNull(arg.toString());
        assertFalse(arg.toString().isEmpty());

        arg.reply = null;
        assertNotNull(arg.toString());
        assertFalse(arg.toString().isEmpty());
    }

    @Test
    public void testAsyncInfo() throws Exception {
        try (ConnectionImpl conn = new ConnectionImpl(defaultOptions())) {
            Parser parser = ConnectionAccessor.getParser(conn);
            assertEquals(conn, parser.nc);

            assertEquals("Expected OP_START", NatsOp.OP_START, parser.ps.state);

            byte[] info = "INFO {}\r\n".getBytes();
            assertEquals("Expected OP_START", NatsOp.OP_START, parser.ps.state);

            parser.parse(Arrays.copyOfRange(info, 0, 1), 1);
            assertEquals(NatsOp.OP_I, parser.ps.state);

            parser.parse(Arrays.copyOfRange(info, 1, 2), 1);
            assertEquals(NatsOp.OP_IN, parser.ps.state);

            parser.parse(Arrays.copyOfRange(info, 2, 3), 1);
            assertEquals(NatsOp.OP_INF, parser.ps.state);

            parser.parse(Arrays.copyOfRange(info, 3, 4), 1);
            assertEquals(NatsOp.OP_INFO, parser.ps.state);

            parser.parse(Arrays.copyOfRange(info, 4, 5), 1);
            assertEquals(NatsOp.OP_INFO_SPC, parser.ps.state);

            // System.err.println("info length = " + info.length);
            // String str = new String(info, 5, info.length - 5);
            // System.err.println("Substring = [" + str + "]");

            parser.parse(Arrays.copyOfRange(info, 5, info.length), info.length - 5);
            assertEquals(NatsOp.OP_START, parser.ps.state);

            // All at once
            parser.parse(info, info.length);
            assertEquals(NatsOp.OP_START, parser.ps.state);

            // Server pool needs to be setup
            conn.setupServerPool();

            // Partials requiring argBuf
            ServerInfo expectedServer = new ServerInfo("test", "localhost", 4222, "1.2.3", true,
                    true, 2 * 1024 * 1024, new String[] {"localhost:5222", "localhost:6222"});

            // Set NoRandomize so that the check with expectedServer info
            // matches.
            conn.setOptions(new Options.Builder().dontRandomize().build());

            String jsonString = expectedServer.toString();
            String infoString = String.format("%s\r\n", jsonString);
            // System.err.println(infoString);
            info = infoString.getBytes();

            assertEquals(NatsOp.OP_START, parser.ps.state);

            parser.parse(info, 9);
            assertEquals(NatsOp.INFO_ARG, parser.ps.state);
            assertNotNull(parser.ps.argBuf);

            parser.parse(Arrays.copyOfRange(info, 9, 11), 2);
            assertEquals(NatsOp.INFO_ARG, parser.ps.state);
            assertNotNull(parser.ps.argBuf);

            parser.parse(Arrays.copyOfRange(info, 11, info.length), info.length - 11);
            assertEquals(NatsOp.OP_START, parser.ps.state);
            assertNull(parser.ps.argBuf);

            // Comparing the string representation is good enough
//            verify(c, times(1)).processAsyncInfo(infoString.trim());
            assertEquals(expectedServer.toString(), parser.nc.getConnectedServerInfo()
                    .toString());
            assertEquals(expectedServer.toString(), conn.getConnectedServerInfo().toString());

            // Good INFOs
            String[] good = {"INFO {}\r\n", "INFO  {}\r\n", "INFO {} \r\n",
                    "INFO { \"server_id\": \"test\"  }   \r\n", "INFO {\"connect_urls\":[]}\r\n"};
            for (String gi : good) {
                parser.ps = new Parser.ParseState();
                try {
                    parser.parse(gi.getBytes(), gi.getBytes().length);
                } catch (ParseException e) {
                    fail("Unexpected parse failure: " + e.getMessage());
                    e.printStackTrace();
                }
                assertEquals(parser.ps.state, NatsOp.OP_START);
            }

            // Wrong INFOs
            String[] wrong = {"IxNFO {}\r\n", "INxFO {}\r\n", "INFxO {}\r\n", "INFOx {}\r\n",
                    "INFO{}\r\n", "INFO {}"};
            for (String wi : wrong) {
                parser.ps = new Parser.ParseState();
                boolean exThrown = false;
                try {
                    parser.parse(wi.getBytes(), wi.getBytes().length);
                } catch (ParseException e) {
                    exThrown = true;
                }
                if (!exThrown && (parser.ps.state == NatsOp.OP_START)) {
                    fail("Should have failed: " + wi);
                }
            }
            // Now test the decoding of "connect_urls"

            // No randomize for now
            conn.getOptions().noRandomize = true;
            // Reset the pool
            conn.setupServerPool();
            // Reinitialize the parser
            parser.ps = new Parser.ParseState();

            info = "INFO {\"connect_urls\":[\"localhost:5222\"]}\r\n".getBytes();
            parser.parse(info);

            // Pool now should contain localhost:4222 (the default URL) and localhost:5222
            String[] srvList = {"localhost:4222", "localhost:5222"};
            checkPool(conn, true, Arrays.asList(srvList));

            // Make sure that if client receives the same, it is not added again.
            parser.parse(info, info.length);
            assertEquals(parser.ps.state, NatsOp.OP_START);

            // Pool should still contain localhost:4222 (the default URL) and localhost:5222
            checkPool(conn, true, Arrays.asList(srvList));

            // Receive a new URL
            info = "INFO {\"connect_urls\":[\"localhost:6222\"]}\r\n".getBytes();
            parser.parse(info);
            assertEquals(parser.ps.state, NatsOp.OP_START);

            // Pool now should contain localhost:4222 (the default URL) localhost:5222 and
            // localhost:6222
            srvList = new String[] {"localhost:4222", "localhost:5222", "localhost:6222"};
            checkPool(conn, true, Arrays.asList(srvList));

            // Receive more than 1 URL at once
            info = "INFO {\"connect_urls\":[\"localhost:7222\", \"localhost:8222\"]}\r\n"
                    .getBytes();
            parser.parse(info);
            assertEquals(parser.ps.state, NatsOp.OP_START);

            // Pool now should contain localhost:4222 (the default URL) localhost:5222,
            // localhost:6222, localhost:7222 and localhost:8222
            srvList = new String[] {"localhost:4222", "localhost:5222", "localhost:6222",
                    "localhost:7222", "localhost:8222"};
            checkPool(conn, true, Arrays.asList(srvList));

            // Test with pool randomization now. Note that with randomization,
            // the initial pool is randomized, then each array of urls that the
            // client gets from the INFO protocol is randomized, but added to
            // the end of the pool.
            conn.getOptions().noRandomize = false;
            conn.setupServerPool();

            info = "INFO {\"connect_urls\":[\"localhost:5222\"]}\r\n".getBytes();
            parser.parse(info);
            assertEquals(parser.ps.state, NatsOp.OP_START);

            // Pool now should contain localhost:4222 (the default URL) and localhost:5222
            srvList = new String[] {"localhost:4222", "localhost:5222"};
            checkPool(conn, true, Arrays.asList(srvList));

            // Make sure that if client receives the same, it is not added again.
            parser.parse(info, info.length);
            assertEquals(parser.ps.state, NatsOp.OP_START);

            // Pool should still contain localhost:4222 (the default URL) and localhost:5222
            checkPool(conn, true, Arrays.asList(srvList));

            // Receive a new URL
            info = "INFO {\"connect_urls\":[\"localhost:6222\"]}\r\n".getBytes();
            parser.parse(info);
            assertEquals(parser.ps.state, NatsOp.OP_START);

            // Pool now should contain localhost:4222 (the default URL) localhost:5222 and
            // localhost:6222
            srvList = new String[] {"localhost:4222", "localhost:5222", "localhost:6222"};
            checkPool(conn, true, Arrays.asList(srvList));

            // Receive more than 1 URL at once. Add more than 2 to increase the chance of
            // the array being shuffled.
            info = ("INFO {\"connect_urls\":[\"localhost:7222\", \"localhost:8222\", " +
                    "\"localhost:9222\",\"localhost:10222\",\"localhost:11222\"]}\r\n")
                    .getBytes();
            parser.parse(info);
            assertEquals(parser.ps.state, NatsOp.OP_START);

            // Pool now should contain localhost:4222 (the default URL) localhost:5222,
            // localhost:6222, localhost:7222, localhost:8222, localhost:9222, localhost:10222
            // and localhost:11222
            srvList = new String[] {"localhost:4222", "localhost:5222", "localhost:6222",
                    "localhost:7222", "localhost:8222", "localhost:9222", "localhost:10222",
                    "localhost:11222"};
            checkPool(conn, false, Arrays.asList(srvList));

            // Finally, check that (part of) the pool should be randomized.
            String[] allUrls = new String[] {"localhost:4222", "localhost:5222", "localhost:6222",
                    "localhost:7222", "localhost:8222", "localhost:9222", "localhost:10222",
                    "localhost:11222"};
            int same = 0;
            int i = 0;
            for (Srv s : ConnectionAccessor.getSrvPool(conn)) {
                if (s.url.getAuthority().equals(allUrls[i])) {
                    same++;
                }
                i++;
            }
            assertNotEquals("Pool does not seem to be randomized", same, allUrls.length);

            // Check that pool may be randomized on setup, but new URLs are always
            // added at end of pool.
            Options opts = new Options.Builder().build();
            opts.servers = Nats.processUrlArray(testServers);
            conn.setOptions(opts); // default is randomize

            // Reset the pool
            conn.setupServerPool();

            // Reinitialize the parser
            parser.ps = new Parser.ParseState();

            // Capture the pool sequence after randomization
            List<String> urlsAfterPoolSetup = new ArrayList<>();
            for (Srv srv : conn.getServerPool()) {
                urlsAfterPoolSetup.add(srv.url.toString());
            }
            String[] newUrls =  { "localhost:6222", "localhost:7222", "localhost:8222",
                    "localhost:9222", "localhost:10222", "localhost:11222", "localhost:12222"};

            for (String newUrl : newUrls) {
                info = ("INFO {\"connect_urls\":[\"" + newUrl + "\"]}\r\n").getBytes();
                parser.parse(info);
                assertEquals(parser.ps.state, NatsOp.OP_START);
                // Check that pool order does not change up to the new addition(s).
                List<Srv> srvPool = conn.getServerPool();
                for (i = 0; i < urlsAfterPoolSetup.size(); i++) {
                    assertEquals(srvPool.get(i).url.toString(), urlsAfterPoolSetup.get(i));
                }
            }
        }
    }

    private void checkPool(ConnectionImpl conn, boolean inThatOrder, List<String> urls) {
        // Check booth pool and urls map
        List<Srv> srvPool = ConnectionAccessor.getSrvPool(conn);
        if (srvPool.size() != urls.size()) {
            fail(String.format("Pool should have %d elements, has %d", urls.size(),
                    srvPool.size()));
        }

        Map<String, URI> connUrls = ConnectionAccessor.getUrls(conn);
        if (connUrls.size() != urls.size()) {
            fail(String.format("Map should have %d elements, has %d", urls.size(),
                    connUrls.size()));
        }

        for (int i = 0; i < urls.size(); i++) {
            String url = urls.get(i);
            if (inThatOrder) {
                if (!srvPool.get(i).url.getAuthority().equals(url)) {
                    fail(String.format("Pool should have %s at index %d, has %s", url, i,
                            srvPool.get(i).url.getAuthority()));
                }
            } else {
                if (!connUrls.containsKey(url)) {
                    fail(String.format("Pool should have %s", url));
                }
            }
        }
    }
}

