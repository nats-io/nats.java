package io.nats.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultExceptionHandler implements ExceptionHandler {
	private final static Logger logger = LoggerFactory.getLogger(DefaultExceptionHandler.class);
	
	@Override
	public void handleException(Connection conn, Subscription sub, Throwable e) {
		System.err.println("Exception thrown on Subscription sid:" + sub.getSid() + 
				" subject: " + sub.getSubject());
		e.printStackTrace();
	}

	@Override
	public void handleSlowConsumerException(Connection conn, Subscription sub, 
			SlowConsumerException e) {
		logger.error("SLOW CONSUMER subscription subject=" + sub.getSubject() + " sid:"+sub.getSid());
	}

}
