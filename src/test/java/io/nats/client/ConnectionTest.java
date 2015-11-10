package io.nats.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

import io.nats.client.impl.NATSConnection;
import io.nats.client.impl.NATSConnection.ConnState;

public class ConnectionTest {

	@Test(expected = NoServersException.class)
	public void whenNoServersExistExceptionIsThrown() throws IOException, URISyntaxException, NoServersException {
		Options o = new Options();
		String url = null;
		o.setUrl(url);
		
		NATSConnection conn = new NATSConnection(o);
//		conn.connect();
		System.out.println("end of test");

	}

	@Test
	public void testConnect() throws Exception {
		Options o = new Options();
		o.setUrl(new URI("nats://localhost:4222"));
		NATSConnection conn = new NATSConnection(o);
		conn.connect();
		if (conn.status != ConnState.CONNECTED)
			fail("didn't connect");
	}
	
//	@Test
//	public void testSub() throws Exception {
//		Options o = new Options();
//		o.setUrl(new URI("nats://localhost:4222"));
//		NATSConnection conn = new NATSConnection(o);
//		conn.connect();
//	}
}
