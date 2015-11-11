package io.nats.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

import io.nats.client.ConnectionImpl.ConnState;

public class ConnectionTest {
	boolean gotMessage = false;

	@Test(expected = NoServersException.class)
	public void whenNoServersExistExceptionIsThrown() throws IOException, URISyntaxException, NoServersException {
		Options o = new Options();
		String url = null;
		o.setUrl(url);
		
		ConnectionImpl conn = new ConnectionImpl(o);
//		conn.connect();
		System.out.println("end of test");

	}

	@Test
	public void testConnect() throws Exception {
		Options o = new Options();
		o.setUrl(new URI("nats://localhost:4222"));
		ConnectionImpl conn = new ConnectionImpl(o);
		conn.connect();
		if (conn.status != ConnState.CONNECTED)
			fail("didn't connect");
		
		conn.subscribe("foo", new MessageHandler() {
			public void onMessage(Message msg) {
				System.out.print("Got a message: " + msg.toString());
				gotMessage=true;
			}
		});
		
		while (!gotMessage) {
			
		}
	}
	
//	@Test
//	public void testSub() throws Exception {
//		Options o = new Options();
//		o.setUrl(new URI("nats://localhost:4222"));
//		ConnectionImpl conn = new ConnectionImpl(o);
//		conn.connect();
//	}
}
