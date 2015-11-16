/**
 * 
 */
package io.nats.client;

/**
 * @author larry
 *
 */
public class MaxMessagesException extends NATSException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3230721002784773022L;

	/**
	 * 
	 */
	public MaxMessagesException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param msg
	 */
	public MaxMessagesException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param e
	 */
	public MaxMessagesException(Throwable e) {
		super(e);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param string
	 * @param e
	 */
	public MaxMessagesException(String string, Exception e) {
		super(string, e);
		// TODO Auto-generated constructor stub
	}

}
