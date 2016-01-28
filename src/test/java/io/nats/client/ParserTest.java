/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.TimeoutException;

import io.nats.client.ConnectionImpl.Control;
import io.nats.client.Parser.NatsOp;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class ParserTest {
	@Rule
	public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testParseControl() {
		//		Control c = new Control();
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
		assertEquals(c.op,"op");
		assertNull(c.args);

		// Test line with trailing spaces
		c = conn.new Control("op   ");
		assertNotNull(c.op);
		assertEquals (c.op,"op");
		assertNull(c.args);

		// Test line with op and args
		c = conn.new Control("op    args");
		assertNotNull(c.op);
		assertEquals (c.op,"op");
		assertNotNull(c.args);
		assertEquals(c.args, "args");

		// Test line with op and args and trailing spaces
		c = conn.new Control("op   args  ");
		assertNotNull(c.op);
		assertEquals(c.op,"op");
		assertNotNull(c.args);
		assertEquals(c.args, "args");

		// Test line with op and args args
		c = conn.new Control("op   args  args   ");
		assertNotNull(c.op);
		assertEquals(c.op,"op");
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
				"PONG\r\n",
				"MSG  foo 1 0\r\n\r\n",
				"MSG \tfoo 1 0\r\n\r\n",
				"MSG \tfoo 1 5\r\nHello\r\n",
				// MSG_END default (not an error)
				"MSG \tfoo 1 6\r\nHello2\r\t"
		};
		try (TCPConnectionMock mock = new TCPConnectionMock()) {
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
				parser = c.parser;
				try (Subscription sub = c.subscribeSync("foo")) {
					for (String s : goodLines) {
//						ConnectionImpl.printSubs(c);
						byte[] buffer = s.getBytes();
						try {
							parser.parse(buffer, buffer.length);
						} catch (Exception e) {
							fail("Should not have thrown an exception for [" + s + "]");
						}			
					}
				}
			} // ConnectionImpl
			catch (IOException | TimeoutException e1) {
				// TODO Auto-generated catch block
				fail(e1.getMessage());
			}
		}// TCPConnectionMock
	}

	// Tests OP_MINUS_ERR, OP_MINUS_ERR_SPC, MINUS_ERR_ARG
	@Test
	public void testMinusErr() {
		Parser parser;
		try (TCPConnectionMock mock = new TCPConnectionMock()) {
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {
				parser = c.parser;
				String s = "-ERR 'A boring error'\r\n";
				try {		
					byte[] b = s.getBytes();
					parser.parse(b, b.length);
				} catch (Exception e) {
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
		} // TCPConnectionMock
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
				"PING\r\t\n",
				// OP_PONG default
				"PONG\r\t\n",
				// state default
				"Z\r\n"
		};

		try (TCPConnectionMock mock = new TCPConnectionMock()) {
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {

				parser = c.parser;
				boolean exThrown = false;

				for (String s : badLines)
				{
					exThrown = false;
					byte[] buffer = s.getBytes();
					try {
						parser.parse(buffer, buffer.length);
					} catch (ParseException e) {
						assertTrue("Wrong exception type. Should have thrown ParseException", 
								e instanceof ParseException);
						exThrown = true;
					}
					assertTrue("Should have thrown ParseException", exThrown);
					// Reset to OP_START for next line
					parser.ps.state = NatsOp.OP_START;
				}

			} // ConnectionImpl
			catch (IOException | TimeoutException e1) {
				fail(e1.getMessage());
			}
		} // TCPConnectionMock
	}

	@Test
	public void testLargeArgs() {
		Parser parser = null;
		int payloadSize = 66000;
		char[] buf = new char[payloadSize];
		for (int i=0; i<payloadSize-2; i++)
		{
			buf[i] = 'A';
		}
		buf[buf.length-2] = '\r';
		buf[buf.length-1] = '\n';

		String msg = String.format("MSG foo 1 %d\r\n", payloadSize);
		byte[] msgBytes = msg.getBytes();
		try (TCPConnectionMock mock = new TCPConnectionMock()) {
			try (ConnectionImpl c = new ConnectionFactory().createConnection(mock)) {

				parser = c.parser;
				try (Subscription sub = c.subscribeSync("foo")) {
					try {
						parser.parse(msgBytes, msgBytes.length);
					} catch (Exception e) {
						fail(e.getMessage());
					}			

				}
			}  // ConnectionImpl
			catch (IOException | TimeoutException e1) {
				fail(e1.getMessage());
			}
		} // TCPConnectionMock
	}
}
