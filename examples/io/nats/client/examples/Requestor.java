/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client.examples;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Constants;
import io.nats.client.Message;
import io.nats.client.NATSException;
import io.nats.client.Statistics;

public class Requestor {
	Map<String, String> parsedArgs = new HashMap<String, String>();

	int count = 20000;
	String url = Constants.DEFAULT_URL;
	String subject = "foo";
	byte[] payload = null;
	long start, end, elapsed;

	public void run(String[] args)
	{
		parseArgs(args);
		banner();

		try (Connection c = new ConnectionFactory(url).createConnection()) {
			start = System.nanoTime();
			int received = 0;

			Message m = null;
			byte[] reply = null;
			try {
				for (int i = 0; i < count; i++)
				{
					m = c.request(subject, payload, 10000);
					if (m == null)
						break;

					received++;
					reply = m.getData();
					if (reply != null)
						System.out.println("Got reply: " + new String(reply));
					else
						System.out.println("Got reply with null payload");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			end = System.nanoTime();
			elapsed = TimeUnit.NANOSECONDS.toSeconds(end-start);

			System.out.printf("Completed %d requests in %d seconds ", received, elapsed);
			if (elapsed > 0) {
				System.out.printf("(%d msgs/second).\n",
						(received / elapsed));
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
		System.out.printf("Statistics:  ");
		System.out.printf("   Incoming Payload Bytes: %d\n", s.getInBytes());
		System.out.printf("   Incoming Messages: %d\n", s.getInMsgs());
		System.out.printf("   Outgoing Payload Bytes: %d\n", s.getOutBytes());
		System.out.printf("   Outgoing Messages: %d\n", s.getOutMsgs());
	}

	private void usage()
	{
		System.err.println(
				"Usage:  Requestor [-url url] [-subject subject] " +
				"[-count count] [-payload payload]");

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
			payload = parsedArgs.get("-payload").getBytes(Charset.forName("UTF-8"));
	}

	private void banner()
	{
		System.out.printf("Sending %d requests on subject %s\n",
				count, subject);
		System.out.printf("  URL: %s\n", url);
		System.out.printf("  Payload is %d bytes.\n",
				payload != null ? payload.length : 0);
	}

	public static void main(String[] args)
	{
		try
		{
			new Requestor().run(args);
		}
		catch (Exception ex)
		{
			System.err.println("Exception: " + ex.getMessage());
			ex.printStackTrace();
		}
		finally {
			System.exit(0);
		}

	}
}
