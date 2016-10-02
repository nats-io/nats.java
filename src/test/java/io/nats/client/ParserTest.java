/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.UnitTestUtilities.newMockedConnection;
import static io.nats.client.UnitTestUtilities.setLogLevel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.nats.client.ConnectionImpl.Control;
import io.nats.client.ConnectionImpl.Srv;
import io.nats.client.Parser.NatsOp;

import ch.qos.logback.classic.Level;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Category(UnitTest.class)
public class ParserTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(ParserTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        verifier.setup();
    }

    @After
    public void tearDown() throws Exception {
        verifier.teardown();
        setLogLevel(Level.INFO);
    }

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

    @Test
    public void testParseLongWithLengthZero() {
        assertEquals(-1, Parser.parseLong(null, 0));
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
    public void testLargeArgs() throws IOException, TimeoutException {
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
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {

            parser = c.parser;
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
    public void testParserSplitMsg() throws IOException, TimeoutException {
        try (ConnectionImpl nc = (ConnectionImpl) newMockedConnection()) {
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
        }
    } // testParserSplitMsg

    @Test
    public void testProcessMsgArgsErrors() {
        String tooFewArgsString = "foo bar";
        byte[] args = tooFewArgsString.getBytes();

        boolean exThrown = false;
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            c.parser.processMsgArgs(args, 0, args.length);
        } catch (ParseException e) {
            exThrown = true;
            String msg = String.format("Wrong msg: [%s]\n", e.getMessage());
            assertTrue(msg, e.getMessage().startsWith("nats: processMsgArgs bad number of args"));
        } catch (IOException | TimeoutException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
            fail(e1.getMessage());
        } finally {
            assertTrue("Should have thrown ParseException", exThrown);
        }

        String badSizeString = "foo 1 -1";
        args = badSizeString.getBytes();

        exThrown = false;
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            c.parser.processMsgArgs(args, 0, args.length);
        } catch (ParseException e) {
            exThrown = true;
            String msg = String.format("Wrong msg: [%s]\n", e.getMessage());
            assertTrue(msg,
                    e.getMessage().startsWith("nats: processMsgArgs bad or missing size: "));
        } catch (IOException | TimeoutException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
            fail(e1.getMessage());
        } finally {
            assertTrue("Should have thrown ParseException", exThrown);
        }
    }

    @Test
    public void testProcessMsgArgsNegativeSize()
            throws IOException, TimeoutException, ParseException {
        thrown.expect(ParseException.class);
        thrown.expectMessage("nats: processMsgArgs bad or missing size:");

        String msg = String.format("MSG foo 1 -100\r\n");
        byte[] msgBytes = msg.getBytes();
        try (ConnectionImpl c = (ConnectionImpl) newMockedConnection()) {
            Parser parser = c.parser;
            try (Subscription sub = c.subscribeSync("foo")) {
                parser.parse(msgBytes, msgBytes.length);
            }
        }
    }

    @Test
    public void testAsyncInfo() throws IOException, TimeoutException, ParseException {
        try (ConnectionImpl c =
                new ConnectionFactory().createConnection(new TCPConnectionFactoryMock())) {
            c.parser.ps = c.parser.new ParseState();

            assertEquals("Expected OP_START", NatsOp.OP_START, c.parser.ps.state);

            byte[] info = "INFO {}\r\n".getBytes();
            assertEquals("Expected OP_START", NatsOp.OP_START, c.parser.ps.state);

            c.parser.parse(Arrays.copyOfRange(info, 0, 1), 1);
            assertEquals(NatsOp.OP_I, c.parser.ps.state);

            c.parser.parse(Arrays.copyOfRange(info, 1, 2), 1);
            assertEquals(NatsOp.OP_IN, c.parser.ps.state);

            c.parser.parse(Arrays.copyOfRange(info, 2, 3), 1);
            assertEquals(NatsOp.OP_INF, c.parser.ps.state);

            c.parser.parse(Arrays.copyOfRange(info, 3, 4), 1);
            assertEquals(NatsOp.OP_INFO, c.parser.ps.state);

            c.parser.parse(Arrays.copyOfRange(info, 4, 5), 1);
            assertEquals(NatsOp.OP_INFO_SPC, c.parser.ps.state);

            // System.err.println("info length = " + info.length);
            // String str = new String(info, 5, info.length - 5);
            // System.err.println("Substring = [" + str + "]");

            c.parser.parse(Arrays.copyOfRange(info, 5, info.length), info.length - 5);
            assertEquals(NatsOp.OP_START, c.parser.ps.state);

            // All at once
            c.parser.parse(info, info.length);
            assertEquals(NatsOp.OP_START, c.parser.ps.state);

            // Server pool needs to be setup
            c.setupServerPool();

            // Partials requiring argBuf
            ServerInfo expectedServer = new ServerInfo("test", "localhost", 4222, "1.2.3", true,
                    true, 2 * 1024 * 1024, new String[] { "localhost:5222", "localhost:6222" });
            Gson gson = new GsonBuilder().create();
            String jsonString = gson.toJson(expectedServer);
            String infoString = String.format("INFO %s\r\n", jsonString);
            // System.err.println(infoString);
            info = infoString.getBytes();

            assertEquals(NatsOp.OP_START, c.parser.ps.state);

            c.parser.parse(info, 9);
            assertEquals(NatsOp.INFO_ARG, c.parser.ps.state);
            assertNotNull(c.parser.ps.argBuf);

            c.parser.parse(Arrays.copyOfRange(info, 9, 11), 2);
            assertEquals(NatsOp.INFO_ARG, c.parser.ps.state);
            assertNotNull(c.parser.ps.argBuf);

            c.parser.parse(Arrays.copyOfRange(info, 11, info.length), info.length - 11);
            assertEquals(NatsOp.OP_START, c.parser.ps.state);
            assertNull(c.parser.ps.argBuf);

            // Comparing the string representation is good enough
            assertEquals(expectedServer.toString(), c.getConnectedServerInfo().toString());

            // Good INFOs
            String[] good = { "INFO {}\r\n", "INFO  {}\r\n", "INFO {} \r\n",
                    "INFO { \"server_id\": \"test\"  }   \r\n", "INFO {\"connect_urls\":[]}\r\n" };
            for (String gi : good) {
                c.parser.ps = c.parser.new ParseState();
                try {
                    c.parser.parse(gi.getBytes(), gi.getBytes().length);
                } catch (ParseException e) {
                    fail("Unexpected parse failure: " + e.getMessage());
                    e.printStackTrace();
                }
                assertEquals(c.parser.ps.state, NatsOp.OP_START);
            }

            // Wrong INFOs
            String[] wrong = { "IxNFO {}\r\n", "INxFO {}\r\n", "INFxO {}\r\n", "INFOx {}\r\n",
                    "INFO{}\r\n", "INFO {}" };
            for (String wi : wrong) {
                c.parser.ps = c.parser.new ParseState();
                boolean exThrown = false;
                try {
                    c.parser.parse(wi.getBytes(), wi.getBytes().length);
                } catch (ParseException e) {
                    exThrown = true;
                }
                if (!exThrown && (c.parser.ps.state == NatsOp.OP_START)) {
                    fail("Should have failed: " + wi);
                }
            }
            // Now test the decoding of "connect_urls"

            // No randomize for now
            c.opts.setNoRandomize(true);
            // Reset the pool
            c.setupServerPool();
            // Reinitialize the parser
            c.parser.ps = c.parser.new ParseState();
            info = "INFO {\"connect_urls\":[\"localhost:5222\"]}\r\n".getBytes();
            c.parser.parse(info);

            // Pool now should contain localhost:4222 (the default URL) and localhost:5222
            String[] srvList = { "localhost:4222", "localhost:5222" };
            checkPool(c, Arrays.asList(srvList));

            // Make sure that if client receives the same, it is not added again.
            c.parser.parse(info, info.length);
            assertEquals(c.parser.ps.state, NatsOp.OP_START);

            // Pool should still contain localhost:4222 (the default URL) and localhost:5222
            checkPool(c, Arrays.asList(srvList));

            // Receive a new URL
            info = "INFO {\"connect_urls\":[\"localhost:6222\"]}\r\n".getBytes();
            c.parser.parse(info);
            assertEquals(c.parser.ps.state, NatsOp.OP_START);

            // Pool now should contain localhost:4222 (the default URL) localhost:5222 and
            // localhost:6222
            srvList = new String[] { "localhost:4222", "localhost:5222", "localhost:6222" };
            checkPool(c, Arrays.asList(srvList));

            // Receive more than 1 URL at once
            info = "INFO {\"connect_urls\":[\"localhost:7222\", \"localhost:8222\"]}\r\n"
                    .getBytes();
            c.parser.parse(info);
            assertEquals(c.parser.ps.state, NatsOp.OP_START);

            // Pool now should contain localhost:4222 (the default URL) localhost:5222,
            // localhost:6222
            // localhost:7222 and localhost:8222
            srvList = new String[] { "localhost:4222", "localhost:5222", "localhost:6222",
                    "localhost:7222", "localhost:8222" };
            checkPool(c, Arrays.asList(srvList));

            // Test with pool randomization now
            c.getOptions().setNoRandomize(false);
            c.setupServerPool();

            info = "INFO {\"connect_urls\":[\"localhost:5222\"]}\r\n".getBytes();
            c.parser.parse(info);
            assertEquals(c.parser.ps.state, NatsOp.OP_START);

            // Pool now should contain localhost:4222 (the default URL) and localhost:5222
            srvList = new String[] { "localhost:4222", "localhost:5222" };
            checkPool(c, Arrays.asList(srvList));

            // Make sure that if client receives the same, it is not added again.
            c.parser.parse(info, info.length);
            assertEquals(c.parser.ps.state, NatsOp.OP_START);

            // Pool should still contain localhost:4222 (the default URL) and localhost:5222
            checkPool(c, Arrays.asList(srvList));

            // Receive a new URL
            info = "INFO {\"connect_urls\":[\"localhost:6222\"]}\r\n".getBytes();
            c.parser.parse(info);
            assertEquals(c.parser.ps.state, NatsOp.OP_START);

            // Pool now should contain localhost:4222 (the default URL) localhost:5222 and
            // localhost:6222
            srvList = new String[] { "localhost:4222", "localhost:5222", "localhost:6222" };
            checkPool(c, Arrays.asList(srvList));

            // Receive more than 1 URL at once
            info = "INFO {\"connect_urls\":[\"localhost:7222\", \"localhost:8222\"]}\r\n"
                    .getBytes();
            c.parser.parse(info);
            assertEquals(c.parser.ps.state, NatsOp.OP_START);

            // Pool now should contain localhost:4222 (the default URL) localhost:5222,
            // localhost:6222
            // localhost:7222 and localhost:8222
            srvList = new String[] { "localhost:4222", "localhost:5222", "localhost:6222",
                    "localhost:7222", "localhost:8222" };
            checkPool(c, Arrays.asList(srvList));

            // Finally, check that the pool should be randomized.
            String[] allUrls = new String[] { "localhost:4222", "localhost:5222", "localhost:6222",
                    "localhost:7222", "localhost:8222" };
            int same = 0;
            int i = 0;
            for (Srv s : c.srvPool) {
                if (s.url.getAuthority().equals(allUrls[i])) {
                    same++;
                }
                i++;
            }
            assertNotEquals(same, allUrls.length);
        }
    }

    void checkPool(ConnectionImpl conn, List<String> urls) {
        // Check booth pool and urls map
        if (conn.srvPool.size() != urls.size()) {
            fail(String.format("Pool should have %d elements, has %d", urls.size(),
                    conn.srvPool.size()));
        }

        if (conn.urls.size() != urls.size()) {
            fail(String.format("Map should have %d elements, has %d", urls.size(),
                    conn.urls.size()));
        }

        for (int i = 0; i < urls.size(); i++) {
            String url = urls.get(i);
            if (conn.getOptions().isNoRandomize()) {
                if (!conn.srvPool.get(i).url.getAuthority().equals(url)) {
                    fail(String.format("Pool should have %s at index %d, has %s", url, i,
                            conn.srvPool.get(i).url.getAuthority()));
                }
            } else {
                if (!conn.urls.containsKey(url)) {
                    fail(String.format("Pool should have %s", url));
                }
            }
        }
    }

}

