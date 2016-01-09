package io.nats.client.examples;
/**
 * 
 */


import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Constants;
import io.nats.client.Statistics;

/**
 * @author larry
 *
 */
public class Publisher {
	Map<String, String> parsedArgs = 
			new HashMap<String, String>();

	int count = 2000000;
	String url = Constants.DEFAULT_URL;
	String subject = "foo";
	byte[] payload = null;

	public void run(String[] args)
	{
		long elapsed = 0L;

		parseArgs(args);
		banner();

		try (Connection c = new ConnectionFactory(url).createConnection()) {

			long t0 = System.nanoTime();

			try {
				for (int i = 0; i < count; i++)
				{
					c.publish(subject, payload);
				}
				c.flush();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			long t1 = System.nanoTime();

			elapsed = TimeUnit.NANOSECONDS.toSeconds(t1 - t0);
			System.out.println("Elapsed time is " + elapsed + " seconds");

			System.out.printf("Published %d msgs in %d seconds ", count, elapsed);
			if (elapsed > 0) {
				System.out.printf("(%d msgs/second).\n",
						(int)(count / elapsed));
			} else {
				System.out.println("\nTest not long enough to produce meaningful stats. "
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
		System.out.println("Statistics:  ");
		System.out.printf("   Outgoing Payload Bytes: %d\n", s.getOutBytes());
		System.out.printf("   Outgoing Messages: %d\n", s.getOutMsgs());
	}

	private void usage()
	{
		System.err.println(
				"Usage:  java Publish [-url url] [-subject subject] " +
				"-count [count] [-payload payload]");

		System.exit(-1);
	}

	private void parseArgs(String[] args)
	{
		if (args == null)
			return;

		for (int i = 0; i < args.length; i++)
		{
			if (i + 1 == args.length)
				usage();

			parsedArgs.put(args[i], args[i + 1]);
			i++;
		}

		if (parsedArgs.containsKey("-count"))
			count = Integer.parseInt(parsedArgs.get("-count"));

		if (parsedArgs.containsKey("-url"))
			url = parsedArgs.get("-url");

		if (parsedArgs.containsKey("-subject"))
			subject = parsedArgs.get("-subject");

		if (parsedArgs.containsKey("-payload"))
			payload = parsedArgs.get("-payload").getBytes(Charset.forName("US-ASCII"));
	}

	private void banner()
	{
		System.out.printf("Publishing %d messages on subject %s\n",
				count, subject);
		System.out.printf("  URL: %s\n", url);
		System.out.printf("  Payload is %d bytes.\n",
				payload != null ? payload.length : 0);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try
		{
			new Publisher().run(args);
		}
		catch (Exception ex)
		{
			System.err.println("Exception: " + ex.getMessage());
			System.err.println(ex);
		}
		finally {
			System.exit(0);
		}


	}

}
