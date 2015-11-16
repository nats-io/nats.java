import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
	Message replyMsg = null;
	
	long start = 0L;
	long end = 0L;

	Lock testLock = new ReentrantLock();
	Condition allDone = testLock.newCondition();
	boolean done = false;
	
	String replyText = "reply";
	byte[] replyBytes = replyText.getBytes(Charset.forName("UTF-8"));
	
	public void run(String[] args)
	{
		parseArgs(args);
		banner();

		ConnectionFactory cf = null;
		Connection c = null;
		
		try {
			cf = new ConnectionFactory(url);
			c = cf.createConnection();
		} catch (NATSException e) {
			System.err.println("Couldn't connect: " + e.getCause());
			System.exit(-1);
		}
		
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
		System.out.printf("Replied to %d msgs in %d seconds ", count, seconds);
		System.out.printf("(%d replies/second).",
				(count / seconds));
		printStats(c);

	}

	private void printStats(Connection c)
	{
		Statistics s = c.getStats();
		System.out.printf("Statistics:  \n");
		System.out.printf("   Incoming Payload Bytes: %d\n", s.getInBytes());
		System.out.printf("   Incoming Messages: %d", s.getInMsgs());
		System.out.printf("   Outgoing Payload Bytes: %d", s.getOutBytes());
		System.out.printf("   Outgoing Messages: %d", s.getOutMsgs());
	}

	@Override
	public void onMessage(Message msg) {
		if (received == 0)
			start = System.nanoTime();

		received++;

		if (verbose)
			System.err.println("Received: " + msg);

		replyMsg.setSubject(msg.getReplyTo());
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
		long t0 = System.nanoTime();

		c.subscribeAsync(subject, this);
		// just wait to complete
		testLock.lock();
		try
		{
			while (!done)
				allDone.await();
		} catch (InterruptedException e) {
		}
		finally {
			testLock.unlock();
		}

		return System.nanoTime() - t0;
	}


	private long receiveSyncSubscriber(Connection c)
	{
		SyncSubscription s = c.subscribeSync(subject);
		try {
			s.nextMessage();
		} catch (Exception e) {
			e.printStackTrace();
		}
		received++;

		start = System.nanoTime();

		while (received < count)
		{
			received++;
			Message m = null;
			try {
				m = s.nextMessage();
			} catch (Exception e) {
				e.printStackTrace();                	
			}
			if (verbose)
				System.out.println("Received: " + m);

			replyMsg.setSubject(m.getReplyTo());
			try {
				c.publish(replyMsg);
			} catch (ConnectionClosedException e) {
				e.printStackTrace();
			}
		}
		end = System.nanoTime();
		return end-start;
	}

	private void usage()
	{
		System.err.println(
				"Usage:  Publish [-url url] [-subject subject] " +
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
			new Replier().run(args);
		}
		catch (Exception ex)
		{
			System.err.println("Exception: " + ex.getMessage());
			ex.printStackTrace();
		}
	}



}
