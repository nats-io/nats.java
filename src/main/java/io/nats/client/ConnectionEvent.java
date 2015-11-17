package io.nats.client;

public class ConnectionEvent {
	ConnectionImpl nc;

	public ConnectionEvent(ConnectionImpl c) {
		this.nc = c;
	}

	public ConnectionImpl getConn() {
		return nc;
	}

}
