/**
 * 
 */
package io.nats.client;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author larry
 *
 */
public class SyncSubscriptionImplTest {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link io.nats.client.SyncSubscriptionImpl#SyncSubscriptionImpl(io.nats.client.ConnectionImpl, java.lang.String, java.lang.String)}.
	 */
	@Test
	public void testSyncSubscriptionImpl() {
		fail("Not yet implemented");
	}

	/**
	 * Test method for {@link io.nats.client.SyncSubscriptionImpl#nextMsg()}.
	 * @throws NoServersException 
	 * @throws MaxMessagesException 
	 * @throws SlowConsumerException 
	 * @throws ConnectionClosedException 
	 * @throws BadSubscriptionException 
	 */
	@Test
	public void testNextMsg() throws NoServersException, BadSubscriptionException, ConnectionClosedException, SlowConsumerException, MaxMessagesException {
		ConnectionImpl c = null;
		SyncSubscription sub = null;
		if (c==null) {
			Options o = new Options();
			o.setUrl("nats://localhost:4222");
			o.setConnectionName("SYNC_SUBJ_TEST");
			o.setSubChanLen(65536);
			
			c = new ConnectionImpl(o);
			try {
				c.connect();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			sub = c.subscribeSync("foo");
			Message m = sub.nextMsg();
			System.err.println("Here's the Message: " + m.toString());
		}
		
	}

	/**
	 * Test method for {@link io.nats.client.SyncSubscriptionImpl#nextMsg(long)}.
	 * @throws MaxMessagesException 
	 * @throws SlowConsumerException 
	 * @throws ConnectionClosedException 
	 * @throws BadSubscriptionException 
	 * @throws NoServersException 
	 */
	@Test
	public void testNextMsgLong() throws BadSubscriptionException, ConnectionClosedException, SlowConsumerException, MaxMessagesException, NoServersException {
		ConnectionImpl c = null;
		SyncSubscription sub = null;
		if (c==null) {
			Options o = new Options();
			o.setUrl("nats://localhost:4222");
			o.setConnectionName("SYNC_SUBJ_TEST");
			o.setSubChanLen(65536);
			
			c = new ConnectionImpl(o);
			try {
				c.connect();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			sub = c.subscribeSync("foo");
			long t0 = System.nanoTime();
			Message m = sub.nextMsg(5000);
			long elapsedSec = (System.nanoTime() - t0)/1000000000;
			if (!(elapsedSec == 5)) {
				System.err.println("Timeout didn't work, elapsed time was " + elapsedSec);
				fail("Timeout didn't work, elapsed time was " + elapsedSec);
			}
			
			System.err.println("Here's the Message: " + ((m == null) ? "null" : m.toString()));
			
			c.close();
		}
		
	}

}
