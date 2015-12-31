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

	}

	@After
	public void tearDown() throws Exception {
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

		try	(NATSServer	s = util.createServerWithConfig("auth_1222.conf"))
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
				Thread.sleep(100);
				try (Connection c = cf.createConnection())
				{
					assertTrue(c.isClosed());
					fail("Should not have connected");
				} catch (AuthorizationException e) {
					exThrown = true;
					System.out.println("Success with expected failure: " + e.getMessage());					
				} finally {
					assertTrue("Should have received an error while trying to connect",
							exThrown);
				}
				Thread.sleep(100);				
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
			assertTrue("The disconnect event handler was incorrectly invoked.",
					hitDisconnect == 0);
		}
	}

	@Test
	public void testAuthSuccess() 
			throws IOException, TimeoutException
	{
		try	(NATSServer	s = util.createServerWithConfig("auth_1222.conf"))
		{

			try(Connection c = new ConnectionFactory("nats://username:password@localhost:1222")
					.createConnection())
			{
				assertTrue(!c.isClosed());
				c.close();
				assertTrue(c.isClosed());
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
