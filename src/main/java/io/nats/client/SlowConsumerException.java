/**
 * 
 */
package io.nats.client;

import java.io.IOException;

public class SlowConsumerException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public SlowConsumerException() {
		// TODO Auto-generated constructor stub
	}

	public SlowConsumerException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

	public SlowConsumerException(Throwable e) {
		super(e);
		// TODO Auto-generated constructor stub
	}

	public SlowConsumerException(String string, Exception e) {
		super(string, e);
		// TODO Auto-generated constructor stub
	}

}
