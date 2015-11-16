import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.AsyncSubscription;
import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Constants;
import io.nats.client.ConnExceptionArgs;
import io.nats.client.ErrorEventHandler;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.NATSException;
import io.nats.client.Statistics;
import io.nats.client.SyncSubscription;

public class Subscriber implements MessageHandler, ErrorEventHandler {
	Map<String, String> parsedArgs = new HashMap<String, String>();

	int count 		= 1000000;
	String url 		= Constants.DEFAULT_URL;
	String subject 	= "foo";
	boolean sync 	= false;
	int received 	= 0;
	boolean verbose = false;
	long startTime 	= 0L;
	long elapsedTime = 0L;

	final Object testLock = new Object();

	public void run(String[] args)
	{ 
		parseArgs(args);
		banner();

		ConnectionFactory cf = new ConnectionFactory(url);
		Connection c = null;
		try {
			c = cf.createConnection();
		} catch (NATSException e) {
			System.err.println("Couldn't connect to " + url + " " + e.getMessage());
			return;
		}

		long elapsed=0L;

		if (sync)
		{
			elapsed = receiveSyncSubscriber(c);
		}
		else
		{
			elapsed = receiveAsyncSubscriber(c);
		}
		long elapsedSeconds = TimeUnit.SECONDS.convert(elapsed, TimeUnit.NANOSECONDS);
		System.out.printf("Received %d msgs in %d seconds ", count, 
				elapsedSeconds);
		if (elapsedSeconds > 0) {
		System.out.printf("(%d msgs/second).\n",
				(count / elapsedSeconds));
		} else {
			System.out.println();
			System.out.println("Test not long enough to produce meaningful stats. "
					+ "Please increase the message count (-count n)");
		}
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
		}
		printStats(c);
	}

	private void printStats(Connection c)
	{
		Statistics s = c.getStats();
		System.out.println("Statistics:  ");
		System.out.printf("   Incoming Payload Bytes: %d\n", s.getInBytes());
		System.out.printf("   Incoming Messages: %d\n", s.getInMsgs());
	}

	@Override
	public void onMessage(Message msg) {
		if (received == 0)
			startTime = System.nanoTime();

		received++;

		if (verbose)
			System.out.println("Received: " + msg);

		if (received >= count)
		{
			elapsedTime = System.nanoTime() - startTime;
			synchronized(testLock)
			{
				testLock.notify();
			}
		}
	}
	
	@Override
	public void onError(ConnExceptionArgs error) {
		System.err.println("Connection error encountered: " + error.getError());
	}
	
	private long receiveAsyncSubscriber(Connection c)
	{
		AsyncSubscription s = c.subscribeAsync(subject, this);

		synchronized(testLock)
		{
			try {
				s.start();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			try {
				testLock.wait();
			} catch (InterruptedException e) {
				/* NOOP */
			}
		}

		return elapsedTime;
	}

	private long receiveSyncSubscriber(Connection c)
	{
		long t0 = 0L;
		
		SyncSubscription s = (SyncSubscription) c.subscribeSync(subject);
		try
		{
			s.nextMessage();
			received++;

			t0 = System.nanoTime();

			while (received < count)
			{
				received++;
				Message m = s.nextMessage();
				if (verbose)
					System.out.println("Received: " + m);
			}

		} catch (Exception ne) {
			System.err.println("Error receiving synchronously:");
			ne.printStackTrace();
		} finally {
			elapsedTime = System.nanoTime()-t0;			
		}
		return elapsedTime;
	}

	private void usage()
	{
		System.out.println(
				"Usage:  java Publish [-url url] [-subject subject] " +
				"-count [count] [-sync] [-verbose]");

		System.exit(-1);
	}

	private void parseArgs(String[] args)
	{
		if (args == null)
			return;

		for (int i = 0; i < args.length; i++)
		{
			if (args[i].equals("-sync") ||
					args[i].equals("-verbose"))
			{
				parsedArgs.put(args[i], "true");
			}
			else
			{
				if (i + 1 == args.length)
					usage();

				parsedArgs.put(args[i], args[i + 1]);
				i++;
			}

		}

		if (parsedArgs.containsKey("-count"))
			count = Integer.parseInt(parsedArgs.get("-count"));

		if (parsedArgs.containsKey("-url"))
			url = parsedArgs.get("-url");

		if (parsedArgs.containsKey("-subject"))
			subject = parsedArgs.get("-subject");

		if (parsedArgs.containsKey("-sync"))
			sync = true;

		if (parsedArgs.containsKey("-verbose"))
			verbose = true;
	}

	private void banner()
	{
		System.out.printf("Receiving %d messages on subject %s\n",
				count, subject);
		System.out.printf("  URL: %s\n", url);
		System.out.printf("  Receiving: %s\n",
				sync ? "Synchronously" : "Asynchronously");
	}

	public static void main(String[] args)
	{
		try
		{
			new Subscriber().run(args);
		}
		catch (Exception ex)
		{
			String s = ex.getMessage();
			if (s != null)
				System.err.println("Exception: " + s);
			else
				System.err.println("Exception:");
			System.out.println(ex);
			ex.printStackTrace();
		} finally {
			System.exit(0);
		}
	}
}

