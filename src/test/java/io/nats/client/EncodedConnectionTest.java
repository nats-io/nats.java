/**
 * 
 */
package io.nats.client;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.nats.client.Constants.*;

/**
 * @author larry
 *
 */
public class EncodedConnectionTest {

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

	EncodedConnection newEConn() {
		Connection nc = null;
		EncodedConnection ec = null;
		try {
			nc = new ConnectionFactory().createConnection();
			ec = new EncodedConnection(nc, DEFAULT_ENCODER);		
		} catch (IOException | TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ec;
	}
	
	@Test
	public void testConstructorErrors() {
		Connection c = mock(Connection.class);
		EncodedConnection ec = new EncodedConnection(c, DEFAULT_ENCODER);
		
		fail("Not yet implemented"); // TODO
	}

}
