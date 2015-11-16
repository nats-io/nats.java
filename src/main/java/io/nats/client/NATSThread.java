package io.nats.client;

import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * NATSThread
 * <p/>
 * Custom thread base class
 *
 * @author Brian Goetz and Tim Peierls
 */
public class NATSThread extends Thread {
	public static final String DEFAULT_NAME = "NATSThread";
	private static volatile boolean debugLifecycle = false;
	private static final AtomicInteger created = new AtomicInteger();
	private static final AtomicInteger alive = new AtomicInteger();
	private static final Logger log = Logger.getAnonymousLogger();

	public NATSThread(Runnable r) {
		this(r, DEFAULT_NAME);
	}

	public NATSThread(Runnable runnable, String name) {
		super(runnable, name + "-" + created.incrementAndGet());
		setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
			public void uncaughtException(Thread t,
					Throwable e) {
				log.log(Level.SEVERE,
						"UNCAUGHT in thread " + t.getName(), e);
			}
		});
	}

	public void run() {
		// Copy debug flag to ensure consistent value throughout.
		boolean debug = debugLifecycle;
		if (debug) log.log(Level.FINE, "Created " + getName());
		try {
			alive.incrementAndGet();
			super.run();
		} finally {
			alive.decrementAndGet();
			if (debug) log.log(Level.FINE, "Exiting " + getName());
		}
	}

	public static int getThreadsCreated() {
		return created.get();
	}

	public static int getThreadsAlive() {
		return alive.get();
	}

	public static boolean getDebug() {
		return debugLifecycle;
	}

	public static void setDebug(boolean b) {
		debugLifecycle = b;
	}
}
