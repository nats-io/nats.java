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

	private static final long serialVersionUID = -4860115934074829966L;

	public AuthorizationException() {
		super();
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
