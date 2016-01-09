package io.nats.client.examples;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.nats.client.*;

public class Replier implements MessageHandler {
	Map<String, String> parsedArgs = new HashMap<String, String>();

	int count = 20000;
	String url = Constants.DEFAULT_URL;
	String subject = "foo";
	boolean sync = false;
	int received = 0;
	boolean verbose = false;

	long start = 0L;
	long end = 0L;

	Lock testLock = new ReentrantLock();
	Condition allDone = testLock.newCondition();
	boolean done = false;

	byte[] replyBytes = "reply".getBytes(Charset.forName("UTF-8"));

	public void run(String[] args)
	{
		parseArgs(args);
		banner();

		try (Connection c = new ConnectionFactory(url).createConnection()) {
			//		try {
			//			cf = new ConnectionFactory(url);
			//			c = cf.createConnection();
			//		} catch (IOException e) {
			//			System.err.println("Couldn't connect: " + e.getCause());
			//			System.exit(-1);
			//		} catch (TimeoutException e) {
			//			System.err.println("Couldn't connect: " + e.getCause());
			//			System.exit(-1);
			//		}

			long elapsed, seconds;

			if (sync)
			{
				elapsed = receiveSyncSubscriber(c);
			}
			else
			{
				elapsed = receiveAsyncSubscriber(c);
			}
			seconds = TimeUnit.NANOSECONDS.toSeconds(elapsed);
			System.out.printf("Replied to %d msgs in %d seconds ", received, seconds);

			if (seconds > 0) {
				System.out.printf("(%d msgs/second).\n",
						(received / seconds));
			} else {
				System.out.println();
				System.out.println("Test not long enough to produce meaningful stats. "
						+ "Please increase the message count (-count n)");
			}
			printStats(c);
		} catch (IOException | TimeoutException e) {
			System.err.println("Couldn't connect: " + e.getMessage());
			System.exit(-1);
		}
	}

	private void printStats(Connection c)
	{
		Statistics s = c.getStats();
		System.out.printf("Statistics:  \n");
		System.out.printf("   Incoming Payload Bytes: %d\n", s.getInBytes());
		System.out.printf("   Incoming Messages: %d\n", s.getInMsgs());
		System.out.printf("   Outgoing Payload Bytes: %d\n", s.getOutBytes());
		System.out.printf("   Outgoing Messages: %d\n", s.getOutMsgs());
	}

	@Override
	public void onMessage(Message msg) {
		if (received++ == 0)
			start = System.nanoTime();

		if (verbose)
			System.out.println("Received: " + msg);

		Connection c = msg.getSubscription().getConnection();
		try {
			c.publish(msg.getReplyTo(), replyBytes);
		} catch (ConnectionClosedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (received >= count)
		{
			end = System.nanoTime();
			testLock.lock();
			try
			{
				System.out.println("I'M DONE");
				done=true;
				allDone.signal();
			}
			finally {
				testLock.unlock();
			}
		}		
	}
	private long receiveAsyncSubscriber(Connection c)
	{
		AsyncSubscription s = c.subscribeAsync(subject, this);
		// just wait to complete
		testLock.lock();
		try
		{
			s.start();
			while (!done)
				allDone.await();
		} catch (InterruptedException e) {
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			testLock.unlock();
		}

		return end - start;
	}


	private long receiveSyncSubscriber(Connection c)
	{
		SyncSubscription s = c.subscribeSync(subject);

		Message m = null;
		try {
			while (received < count)
			{
				m = s.nextMessage();
				if (received++ == 0)
					start = System.nanoTime();

				if (verbose)
					System.out.println("Received: " + m);

				c.publish(m.getReplyTo(),replyBytes);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		end = System.nanoTime();
		return end-start;
	}

	private void usage()
	{
		System.err.println(
				"Usage:  Publish [-url url] [-subject subject] " +
				"[-count count] [-sync] [-verbose]");

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
			new Replier().run(args);
		}
		catch (Exception ex)
		{
			System.err.println("Exception: " + ex.getMessage());
			ex.printStackTrace();
		} finally {
			System.exit(0);
		}
	}



}
