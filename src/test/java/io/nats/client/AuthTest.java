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

	@Test
	public void testAuthFailure()
	{
		String[] urls = 
			{ 
					"nats://username@localhost:1222",
					"nats://username:badpass@localhost:1222",
					"nats://localhost:1222", 
					"nats://badname:password@localhost:1222"
			};
		
		try
		{
			hitDisconnect = 0;
			ConnectionFactory cf = new ConnectionFactory();
			cf.setDisconnectedEventHandler(new DisconnectedEventHandler() {
				@Override
				public void onDisconnect(ConnectionEvent event) {
					hitDisconnect++;
				}				
			});

			for (String url : urls)
			{
				boolean exThrown = false;
				System.err.println("Trying: " + url);
				cf.setUrl(url);
				try (Connection c = cf.createConnection())
				{
					assertTrue(c.isClosed());
					fail("Should have received an error while trying to connect");
				} catch (AuthorizationException e) {
					exThrown = true;
					System.out.println("Success with expected failure: " + e.getMessage());					
				} finally {
					assertTrue("Should have received an error while trying to connect",
							exThrown);
				}
				
			}
		}
		catch (AuthorizationException e) {
			System.out.println("Success with expected failure: " + e.getMessage());
		}
		catch (Exception e)
		{
			fail("Unexpected exception thrown: " + e);
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
		try(Connection c = new ConnectionFactory("nats://username:password@localhost:1222")
				.createConnection())
		{
			assertTrue(!c.isClosed());
			c.close();
			assertTrue(c.isClosed());
		}
	}
}
