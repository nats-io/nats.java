/**
 * 
 */
package io.nats.client;

/**
 * @author larry
 *
 */
public class SlowConsumerException extends NATSException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3097038253885136890L;

	/**
	 * 
	 */
	public SlowConsumerException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param msg
	 */
	public SlowConsumerException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param e
	 */
	public SlowConsumerException(Throwable e) {
		super(e);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param string
	 * @param e
	 */
	public SlowConsumerException(String string, Exception e) {
		super(string, e);
		// TODO Auto-generated constructor stub
	}

}
