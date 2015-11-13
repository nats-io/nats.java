/**
 * 
 */
package io.nats.client;

import java.io.IOException;

/**
 * @author Larry McQueary
 *
 */
public class AsyncSubscriptionImpl extends AbstractSubscriptionImpl implements AsyncSubscription {

	private MessageHandler msgHandler;
//    private MsgHandlerEventArgs msgHandlerArgs = new MsgHandlerEventArgs();
    private Thread                msgFeeder = null;
    Runnable runnable = new Runnable(){
//        private ConnectionImpl conn =;
        @Override
        public void run(){
        	conn.deliverMsgs(mch);
        }};

	public AsyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue, MessageHandler cb) {
		super(nc, subj, queue);
		this.msgHandler = cb;
	}

	@Override
	protected boolean processMsg(Message m) {
		Connection c;
		MessageHandler handler;
		long  max;
		
		mu.lock();
		try {
			c = this.getConnection();
			handler = this.msgHandler;
			max = this.getMax();
		} finally {
			mu.unlock();
		}
		
        // TODO Can this even happen?
		// the message handler has not been setup yet, drop the 
        // message.
		if (handler == null)
			return true;
		
		if (conn == null)
			return false;
		
		long d = delivered.incrementAndGet();
		if (max <= 0 || d <= max) {
			if (d == max) {
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
				}
				this.conn = null;
			}
		}
		msgHandler.onMessage(m);
		return true;
	}
	
	private void enable() {
	    msgFeeder = new Thread(runnable);
	    msgFeeder.start();
	}
	
	private void disable() {
		
	}
	

}
