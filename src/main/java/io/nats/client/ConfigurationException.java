/**
 * 
 */
package io.nats.client;

/**
 * @author Larry McQueary
 *
 */
public class ConfigurationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4978276368102899114L;
	public ConfigurationException() {
		super("nats: No servers available for connection");
	}

	public ConfigurationException(String message) {
		super(message);
	}
}
