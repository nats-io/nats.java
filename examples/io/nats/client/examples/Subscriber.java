/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client.examples;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.nats.client.AsyncSubscription;
import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Constants;
import io.nats.client.ExceptionHandler;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.NATSException;
import io.nats.client.SlowConsumerException;
import io.nats.client.Statistics;
import io.nats.client.Subscription;
import io.nats.client.SyncSubscription;

public class Subscriber implements MessageHandler, ExceptionHandler {
	Map<String, String> parsedArgs = new HashMap<String, String>();

	int count 		= 1000000;
	String url 		= Constants.DEFAULT_URL;
	String subject 	= "foo";
	boolean sync 	= false;
	long received 	= 0L;
	boolean verbose = false;
	long start 	= 0L;
	long elapsed = 0L;

	final Object testLock = new Object();

	public void run(String[] args)
	{ 
		parseArgs(args);
		banner();

		try (Connection c = new ConnectionFactory(url).createConnection()) {

			if (sync)
			{
				elapsed = receiveSyncSubscriber(c);
			}
			else
			{
				c.setExceptionHandler(this);

				elapsed = receiveAsyncSubscriber(c);
			}
			long elapsedSeconds = TimeUnit.SECONDS.convert(elapsed, TimeUnit.NANOSECONDS);
			System.out.printf("Received %d msgs in %d seconds ", received, 
					elapsedSeconds);
			if (elapsedSeconds > 0) {
				System.out.printf("(%d msgs/second).\n",
						(received / elapsedSeconds));
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
		System.out.println("Statistics:  ");
		System.out.printf("   Incoming Payload Bytes: %d\n", s.getInBytes());
		System.out.printf("   Incoming Messages: %d\n", s.getInMsgs());
	}

	@Override
	public void onMessage(Message msg) {
		if (received == 0)
			start = System.nanoTime();

		if (msg == null)
			System.out.println("Message==null");
		received++;

		if (verbose)
			System.out.println("Received: " + msg);

		if (received >= count)
		{
			elapsed = System.nanoTime() - start;
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

		return elapsed;
	}

	private long receiveSyncSubscriber(Connection c)
	{
		SyncSubscription s = (SyncSubscription) c.subscribeSync(subject);

		try
		{
			while (received < count)
			{
				Message m = s.nextMessage();
				if (received++ == 0) {
					start = System.nanoTime();
				}
				if (verbose)
					System.out.println("Received: " + m);
			}

		} catch (Exception ne) {
			System.err.println("Error receiving synchronously:");
			ne.printStackTrace();
		} 
		elapsed = System.nanoTime()-start;			
		return elapsed;
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

	@Override
	public void onException(NATSException e) {
		if (e.getCause() instanceof SlowConsumerException)
			System.err.println("Warning: SLOW CONSUMER");
		else
			e.printStackTrace();
	}
}

