package io.nats.client;

import static org.junit.Assert.*;
import io.nats.client.ConnectionImpl.Control;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ParserTest {

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

		System.out.println("Test with NULL line: ");
		c = conn.new Control(null);
		assertTrue(c.op == null);
		assertTrue(c.args == null);

		System.out.println("Test line with single op: ");
		c = conn.new Control("op");
		assertNotNull(c.op);
		assertEquals(c.op,"op");
		assertNull(c.args);

		System.out.println("Test line with trailing spaces: ");
		c = conn.new Control("op   ");
		assertNotNull(c.op);
		assertEquals (c.op,"op");
		assertNull(c.args);

		System.out.println("Test line with op and args: ");
		c = conn.new Control("op    args");
		assertNotNull(c.op);
		assertEquals (c.op,"op");
		assertNotNull(c.args);
		assertEquals(c.args, "args");

		System.out.println("Test line with op and args and trailing spaces: ");
		c = conn.new Control("op   args  ");
		assertNotNull(c.op);
		assertEquals(c.op,"op");
		assertNotNull(c.args);
		assertEquals(c.args, "args");

		System.out.println("Test line with op and args args: ");
		c = conn.new Control("op   args  args   ");
		assertNotNull(c.op);
		assertEquals(c.op,"op");
		assertNotNull(c.args);
		assertEquals(c.args, "args  args");

	}

}
