package io.nats.client;

public class ConnectionEvent {
	Connection nc;

	public ConnectionEvent(ConnectionImpl c) {
		this.nc = c;
	}

	public Connection getConnection() {
		return nc;
	}

}
