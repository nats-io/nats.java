package io.nats.client;

public interface AsyncSubscription extends Subscription {

	/**
	 * Starts message delivery to this subscription
	 * @throws BadSubscriptionException - if the subscription is invalid/closed.
	 */
	void start();

	/**
	 * @param cb - the message handler to set
	 */
	void setMessageHandler(MessageHandler cb);

}
