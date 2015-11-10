package io.nats.client;

import io.nats.client.impl.NATSConnection;

public class ConnEventArgs {
	NATSConnection nc;

	public ConnEventArgs(NATSConnection c) {
		this.nc = c;
	}

	public NATSConnection getConn() {
		return nc;
	}

}
