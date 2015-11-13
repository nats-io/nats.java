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
		
//		conn.subscribe("foo", new MessageHandler() {
//			public void onMessage(Message msg) {
//				System.out.print("Got a message: " + msg.toString());
//				gotMessage=true;
//			}
//		});
//		
//		while (!gotMessage) {
//			
//		}
	}
	
	@Test
	public void testSub() throws Exception {
		Connection c = connect("nats://localhost:4222");
		if (c == null)
			fail("Couldn't connect");
			
		SyncSubscription s = (SyncSubscription)c.subscribeSync("foo");
		assert(s != null);
		
	}
	
	@Test
	public void testNewInbox() throws NoServersException {
//		Connection c = connect("nats://localhost:4222");
		Options o = new Options();
		o.setUrl("nats://localhost:4222");
		
		ConnectionImpl conn = new ConnectionImpl(o);

		String s = conn.newInbox(); 
		assert (s!=null);
		assert (s.length()==20);
		System.err.println("Inbox = " + s);
	}
	
	private Connection connect(String s) {
		ConnectionImpl c = null; 
		Options o = new Options();
		try {
			o.setUrl(new URI(s));
			o.setConnectionName("CONNTEST");
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		try {
			c = new ConnectionImpl(o);
			c.connect();
		} catch (NoServersException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			c = null;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			c = null;
		}

		return c;
	}
	
	
}
