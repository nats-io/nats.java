package io.nats.client;

public class EncodedConnection {

	protected EncodedConnection(Connection c, String defaultEncoder) {
		if (c == null) {
			throw new IllegalArgumentException("Connection cannot be null");
		}
		// TODO test for bad encoder
		
	}
}
