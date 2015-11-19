package io.nats.client;

public interface ExceptionHandler {
	public void handleException(Connection conn, Subscription subscription, Throwable e);

//	public void handleSlowConsumerException(Connection conn, 
//			                                Subscription sub, 
//			                                Throwable e);

	void handleSlowConsumerException(Connection conn, Subscription sub, SlowConsumerException e);

}
