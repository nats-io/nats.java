package io.nats.client;

public class ConnEventArgs {
	ConnectionImpl nc;

	public ConnEventArgs(ConnectionImpl c) {
		this.nc = c;
	}

	public ConnectionImpl getConn() {
		return nc;
	}

}
