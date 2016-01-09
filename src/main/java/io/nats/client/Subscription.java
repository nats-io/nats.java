/**
 * 
 */
package io.nats.client;

import java.io.IOException;

/**
 * @author larry
 *
 */
public interface Subscription extends AutoCloseable {
 
	/*
	 * Retrieves the subject of interest from the AsyncSubscriptionImpl object.
	 * @return the subject of interest
	 */
    String getSubject();

    /*
     * Optional queue group name. If present, all subscriptions with the
     * same name will form a distributed queue, and each message will
     * only be processed by one member of the group.
     * @return the name of the queue groups this subscriber belongs to.
     * 
     */
    String getQueue();

    /*
     * @return the Connection this subscriber was created on.
     */
    Connection getConnection();

    /*
     * @return true if the subscription is active, false otherwise.
     */
    boolean isValid();

    /* 
     * Removes interest in the given subject.
     */
    void unsubscribe() throws IOException;

    /*
     * autoUnsubscribe will issue an automatic unsubscribe that is
     * processed by the server when max messages have been received.
     * This can be useful when sending a request to an unknown number
     * of subscribers. request() uses this functionality.
     * @param max The number of messages to receive before 
     *            unsubscribing.
     */
    void autoUnsubscribe(int max) throws IOException;

    /*
     * Gets the number of messages delivered to, but not processed, by
     * this subscriber.
     * @return the number of delivered messages.
     */
    int getQueuedMessageCount();

    /**
     * Get the Subscription ID for this subscription.
     * @return the Subscription ID.
     */
	long getSid();

	void setMax(long max);
	
	long getMax();
	
	@Override
	void close();
}
