package io.nats.client;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class MessageTest {
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
	public void testMessage() {
		byte[] payload = "This is a message payload.".getBytes();
		String subj = "foo";
		String reply = "bar";
		Message m = new Message();
		m.setData(null, 0, payload.length);
		m.setData(payload, 0, payload.length);
		m.setReplyTo(reply);
		m.setSubject(subj);
	}

	@Test
	public void testMessageStringStringByteArray() {
		Message m = new Message("foo", "bar", "baz".getBytes());
		assertEquals("foo", m.getSubject());
		assertEquals("bar", m.getReplyTo());
		assertTrue(Arrays.equals( "baz".getBytes(), m.getData()));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testNullSubject() {
		new Message(null, "bar", "baz".getBytes());
	}

	@Test(expected=IllegalArgumentException.class)
	public void testWhitespaceSubject() {
		new Message(" 	", "bar", "baz".getBytes());
	}

//	@Test
//	public void testMessageMsgArgSubscriptionImplByteArrayLong() {
//		fail("Not yet implemented"); // TODO
//	}

	@Test
	public void testMessageByteArrayLongStringStringSubscriptionImpl() {
		byte[] payload = "This is a message payload.".getBytes();
		String subj = "foo";
		String reply = "bar";

		Message m = new Message(payload, payload.length, subj, reply, null);
		assertEquals(subj, m.getSubject());
		assertEquals(reply, m.getReplyTo());
		assertTrue(Arrays.equals(payload, m.getData()));		
	}

//	@Test
//	public void testGetData() {
//		fail("Not yet implemented"); // TODO
//	}
//
//	@Test
//	public void testGetSubject() {
//		fail("Not yet implemented"); // TODO
//	}

//	@Test
//	public void testSetSubject() {
//		fail("Not yet implemented"); // TODO
//	}
//
//	@Test
//	public void testGetReplyTo() {
//		fail("Not yet implemented"); // TODO
//	}
//
//	@Test
//	public void testSetReplyTo() {
//		fail("Not yet implemented"); // TODO
//	}
//
//	@Test
//	public void testGetSubscription() {
//		fail("Not yet implemented"); // TODO
//	}
//
//	@Test
//	public void testSetData() {
//		fail("Not yet implemented"); // TODO
//	}
//
//	@Test
//	public void testToString() {
//		fail("Not yet implemented"); // TODO
//	}

}
