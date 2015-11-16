/**
 * 
 */


import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.nats.client.Connection;
import io.nats.client.ConnectionClosedException;
import io.nats.client.ConnectionFactory;
import io.nats.client.Constants;
import io.nats.client.NATSException;
import io.nats.client.Options;
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

	public void Run(String[] args)
	{
		long elapsed = 0L;
		Connection c = null;
		
		parseArgs(args);
		banner();

		ConnectionFactory cf = new ConnectionFactory();
		cf.setUrl(url);
		try {
			c = cf.createConnection();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}

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
			System.err.println("\nTest not long enough to produce meaningful stats. "
					+ "Please increase the message count (-count n)");
		} 
		
		printStats(c);
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
			new Publisher().Run(args);
		}
		catch (Exception ex)
		{
			System.err.println("Exception: " + ex.getMessage());
			System.err.println(ex);
		}


	}

}
