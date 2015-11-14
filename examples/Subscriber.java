import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.nats.client.AsyncSubscription;
import io.nats.client.BadSubscriptionException;
import io.nats.client.Connection;
import io.nats.client.ConnectionClosedException;
import io.nats.client.ConnectionFactory;
import io.nats.client.Constants;
import io.nats.client.MaxMessagesException;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.NATSException;
import io.nats.client.Options;
import io.nats.client.SlowConsumerException;
import io.nats.client.Statistics;
import io.nats.client.SyncSubscription;

public class Subscriber implements MessageHandler {
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

	public void Run(String[] args)
	{
		parseArgs(args);
		banner();

		ConnectionFactory cf = new ConnectionFactory(Constants.DEFAULT_URL);
		Connection c = null;
		try {
			c = cf.createConnection();
		} catch (NATSException e) {
			System.err.println("Connect error:");
			e.printStackTrace();
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
		long elapsedSeconds = TimeUnit.NANOSECONDS.convert(elapsed, TimeUnit.SECONDS);
		System.out.printf("Received %d msgs in %d seconds ", count, 
				elapsedSeconds);
		System.out.printf("(%d msgs/second).\n",
				(int)(count / elapsedSeconds));
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
	
	private long receiveAsyncSubscriber(Connection c)
	{
		AsyncSubscription s = c.subscribeAsync(subject, this);

		synchronized(testLock)
		{
			// s.start();
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

		} catch (NATSException ne) {
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
			new Subscriber().Run(args);
		}
		catch (Exception ex)
		{
			System.out.println("Exception: " + ex.getMessage());
			System.out.println(ex);
		}
	}
}

