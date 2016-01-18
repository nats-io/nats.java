/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

public class NATSException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Connection nc;
	private Subscription sub;
	
	NATSException() {
		super();
	}
	
	public NATSException(String msg) {
		super(msg);
	}
	
	NATSException(Throwable e) {
		super(e);
	}

	public NATSException(String string, Exception e) {
		super(string, e);
	}

	public NATSException(Throwable e, Connection nc, Subscription sub) {
		super(e);
		this.setConnection(nc);
		this.setSubscription(sub);
	}

	public void setConnection(Connection nc) {
		this.nc = nc;
	}
	
	public Connection getConnection() {
		return this.nc;
	}
	
	public void setSubscription(Subscription sub) {
		this.sub = sub;
	}

	public Subscription getSubscription() {
		return sub;
	}
}
