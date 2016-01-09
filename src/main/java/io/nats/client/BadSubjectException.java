package io.nats.client;

public class BadSubjectException extends IllegalArgumentException {

	public BadSubjectException() {
		this("nats: Invalid Subject");
	}

	public BadSubjectException(String s) {
		super(s);
		// TODO Auto-generated constructor stub
	}

	public BadSubjectException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	public BadSubjectException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

}
