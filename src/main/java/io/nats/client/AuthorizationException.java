/**
 * 
 */
package io.nats.client;

import java.io.IOException;

/**
 * 
 *
 */
public class AuthorizationException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public AuthorizationException() {
		this("nats: Authorization Failed");
	}

	public AuthorizationException(String arg0) {
		super(arg0);
	}

	public AuthorizationException(Throwable arg0) {
		super(arg0);
	}

	public AuthorizationException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

}
