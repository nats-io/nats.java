/**
 * 
 */
package io.nats.client;

/**
 * @author Larry McQueary
 *
 */
public class NoServersException extends NATSException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4978276368102899114L;
	public NoServersException() {
		super("nats: No servers available for connection");
	}

	public NoServersException(String message) {
		super(message);
	}
}
