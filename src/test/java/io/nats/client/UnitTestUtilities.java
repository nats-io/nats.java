package io.nats.client;

import java.io.IOException;
import org.junit.Assert.*;
import org.junit.Test;

public class UnitTestUtilities {
	final Object mu = new Object();
	static NATSServer defaultServer = null;
	Process authServerProcess = null;

	public void startDefaultServer()
	{
		synchronized(mu)
		{
			if (defaultServer == null)
			{
				defaultServer = new NATSServer();
			}
		}
	}

	public void stopDefaultServer()
	{
		synchronized (mu)
		{
			defaultServer.shutdown();;
			defaultServer = null;
		}
	}

	public void bounceDefaultServer(int delayMillis)
	{
		stopDefaultServer();
		try {
			Thread.sleep(delayMillis);
		} catch (InterruptedException e) {
			// NOOP
		}
		startDefaultServer();
	}

	public void startAuthServer() throws IOException
	{
		authServerProcess = Runtime.getRuntime().exec("gnatsd -config auth.conf");
	}

//	private static void testExpectedException(Action call, Type exType)
//	{
//		try {
//			call.Invoke();
//		}
//		catch (Exception e)
//		{
//			System.out.println(e);
//			
//			assertThat(e, instanceOf(exType));
//			return;
//		}
//
//		fail("No exception thrown!");
//	}

	private NATSServer createServerOnPort(int p)
	{
		return new NATSServer(p);
	}

	private NATSServer createServerWithConfig(String configFile)
	{
		return new NATSServer(configFile);
	}
}
