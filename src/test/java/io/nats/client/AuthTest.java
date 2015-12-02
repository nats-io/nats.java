package io.nats.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class AuthTest {
	@Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

	NATSServer s = null;

	int hitDisconnect;
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		s = util.createServerWithConfig("auth_1222.conf");
		Thread.sleep(500);
	}

	@After
	public void tearDown() throws Exception {
		s.shutdown();
		Thread.sleep(500);
	}


	UnitTestUtilities util = new UnitTestUtilities();

	class DisconnectHandler implements DisconnectedEventHandler {
		@Override
		public void onDisconnect(ConnectionEvent event) {
			hitDisconnect++;
		}
	}
	private void connectAndFail(String url)
	{
		try
		{
			System.out.println("Trying: " + url);

			hitDisconnect = 0;
			ConnectionFactory cf = new ConnectionFactory(url);
			cf.setDisconnectedEventHandler(new DisconnectHandler());
			Connection c = cf.createConnection();
			c.close();
			assertTrue(c.isClosed());
			
			fail("Expected a failure; did not receive one");
		}
		catch (AuthorizationException e) {
			System.out.println("Success with expected failure: " + e.getMessage());
		}
		catch (Exception e)
		{
//			if (e.getMessage().contains("Authorization"))
//			{
//				System.out.println("Success with expected failure: " + e.getMessage());
//			}
//			else
//			{
				fail("Unexpected exception thrown: " + e);
//			}
		}
		finally
		{
			if (hitDisconnect > 0)
				fail("The disconnect event handler was incorrectly invoked.");
		}
	}

	@Test
	public void testAuthSuccess() 
			throws IOException, TimeoutException
	{
		Connection c = new ConnectionFactory("nats://username:password@localhost:1222").createConnection();
		assertTrue(!c.isClosed());
		c.close();
		assertTrue(c.isClosed());
	}

	@Test
	public void testAuthFailure()
	{
		connectAndFail("nats://username@localhost:1222");
		connectAndFail("nats://username:badpass@localhost:1222");
		connectAndFail("nats://localhost:1222");
		connectAndFail("nats://badname:password@localhost:1222");
	}
}
