/**
 * 
 */
package io.nats.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 * This is the implementation of the AsyncSubscription interface.
 *
 */
class AsyncSubscriptionImpl extends SubscriptionImpl implements AsyncSubscription {

//	private ExecutorService executor = Executors.newSingleThreadExecutor(new NATSThreadFactory("msgfeederfactory"));
	private MessageHandler msgHandler;
	private NATSThread msgFeederThread = null;
	private MessageFeeder msgFeeder;

	class MessageFeeder implements Runnable {
		private volatile boolean running = true;

		public boolean isRunning() {
			return running;
			
		}
		public void terminate() {
	        running = false;
	    }

		@Override
		public void run(){
			logger.debug("msgFeeder has started");
			conn.deliverMsgs(mch);
		}
	}

	protected AsyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue, MessageHandler cb) {
		super(nc, subj, queue);
		this.msgHandler = cb;
		msgFeeder = new MessageFeeder();
	}

	@Override
	protected boolean processMsg(Message m) {
		Connection localConn;
		MessageHandler localHandler;
		long  localMax;

		mu.lock();
		try {
			localConn = this.getConnection();
			localHandler = this.msgHandler;
			localMax = this.getMax();
		} finally {
			mu.unlock();
		}

		// TODO Should this happen? And should it be handled differently?
		// the message handler has not been setup yet, drop the 
		// message.
		if (localHandler == null)
			return true;

		if (localConn == null)
			return false;

		long d = delivered.incrementAndGet();
		if (localMax <= 0 || d <= localMax) {

			try {
				localHandler.onMessage(m);
			} catch (Exception e) { }

			if (d == localMax) {
				try {
					unsubscribe();
				} catch (BadSubscriptionException e) {
					logger.debug("Bad subscription while unsubscribing:", e);
				} catch (NATSException e) {
					logger.debug("Exception while unsubscribing:", e);
					e.printStackTrace();
				} finally {
					this.conn = null;
				}
			}
		}
		return true;
	}

    private boolean isStarted()
    {
        return (msgFeederThread != null);
//    	return (!executor.isTerminated());
    }


	void enable() {
		if (msgFeederThread == null) {
			msgFeederThread = new NATSThread(msgFeeder, "msgFeeder");
			msgFeederThread.start();
			logger.debug("Started msgFeeder for subject: " + this.getSubject() + " sid: " + this.getSid());
		}
	}

	void disable() {
		if (msgFeeder != null) {
			try {
				msgFeeder.terminate();
				msgFeederThread.join();
			} catch (InterruptedException e) {
			}
			msgFeeder = null;
		}
	}

	@Override
	public void setMessageHandler(MessageHandler cb) {
		this.msgHandler = cb;
	}

	@Override
	public void start() throws IllegalStateException 
    {
		//TODO fix exception handling
		if (isStarted())
            return;

        if (!isValid())
            throw new IllegalStateException("Subscription is not valid.");

        conn.sendSubscriptionMessage(this);
        
        enable();
    }


}
