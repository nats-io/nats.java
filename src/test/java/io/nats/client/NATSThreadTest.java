package io.nats.client;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class NATSThreadTest implements Runnable {

	final static int NUM_THREADS = 5;
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
	public void testRun() {
		NATSThread nt = new NATSThread(this);
		throwException = true;
		nt.start();
	}
	
	boolean throwException = false;
	public void run() {
		try {
			if (throwException)
				throw new Error("just for a test");
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testNATSThreadRunnable() {
		NATSThread[] threads = new NATSThread[NUM_THREADS];
		for (int i=0; i<NUM_THREADS; i++) {
			NATSThread nt = new NATSThread(this);
			nt.start();
			threads[i] = nt;
		}
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertTrue(NATSThread.getThreadsAlive() > 0);
		assertTrue(NATSThread.getThreadsCreated() > 0);
		NATSThread.setDebug(true);
		assertEquals(true, NATSThread.getDebug());
		NATSThread.setDebug(false);
		assertEquals(false, NATSThread.getDebug());
		NATSThread.setDebug(true);
		try {
			for (int i=0; i<NUM_THREADS; i++)
				threads[i].join(500);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

//	@Test
//	public void testNATSThreadRunnableString() {
//		fail("Not yet implemented"); // TODO
//	}
//
//	@Test
//	public void testGetThreadsCreated() {
//		fail("Not yet implemented"); // TODO
//	}
//
//	@Test
//	public void testGetThreadsAlive() {
//		fail("Not yet implemented"); // TODO
//	}
//
//	@Test
//	public void testGetDebug() {
//		fail("Not yet implemented"); // TODO
//	}
//
//	@Test
//	public void testSetDebug() {
//		fail("Not yet implemented"); // TODO
//	}
//

}
