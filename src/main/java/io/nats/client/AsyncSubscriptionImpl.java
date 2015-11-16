/**
 * 
 */
package io.nats.client;

import java.io.IOException;

/*
 * This is the implementation of the AsyncSubscription interface.
 *
 */
class AsyncSubscriptionImpl extends AbstractSubscriptionImpl implements AsyncSubscription {

	private MessageHandler msgHandler;
//	private MsgHandlerEventArgs msgHandlerArgs = new MsgHandlerEventArgs();
	private NATSThread msgFeeder = null;

	Runnable runnable = new Runnable(){
		@Override
		public void run(){
			logger.debug("msgFeeder has started");
			conn.deliverMsgs(mch);
		}
	};

	protected AsyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue, MessageHandler cb) {
		super(nc, subj, queue);
		this.msgHandler = cb;
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
				} catch (ConnectionClosedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (BadSubscriptionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					this.conn = null;
				}
			}
		}
		msgHandler.onMessage(m);
		return true;
	}

    private boolean isStarted()
    {
        return (msgFeeder != null);
    }


	void enable() {
		if (msgFeeder == null) {
			msgFeeder = new NATSThread(runnable, "msgFeeder");
			msgFeeder.start();
			logger.debug("Started msgFeeder for subject: " + this.getSubject() + " sid: " + this.getSid());
		}
	}

	void disable() {
		if (msgFeeder != null) {
			try {
				msgFeeder.join();
			} catch (InterruptedException e) {
			}
			msgFeeder = null;
		}
	}

	@Override
	public void start() throws Exception 
    {
		//TODO fix exception handling
		if (isStarted())
            return;

        if (conn == null)
            throw new BadSubscriptionException();

        conn.sendSubscriptionMessage(this);
        conn.flush();
        
        enable();
    }


}
