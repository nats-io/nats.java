/**
 * 
 */
package io.nats.client;

import java.io.IOException;

/**
 *
 */
public class MaxMessagesException extends IOException {

	private static final long serialVersionUID = -3230721002784773022L;

	public MaxMessagesException() {
		// TODO Auto-generated constructor stub
	}

	public MaxMessagesException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

	public MaxMessagesException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	public MaxMessagesException(String string, Exception e) {
		super(string, e);
		// TODO Auto-generated constructor stub
	}

}
