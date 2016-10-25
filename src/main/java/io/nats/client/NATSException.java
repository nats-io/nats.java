/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

public class NATSException extends Exception {
    private static final long serialVersionUID = 1L;

    private Connection nc;
    private Subscription sub;

    NATSException() {
        super();
    }

    public NATSException(String msg) {
        super(msg);
    }

    NATSException(Throwable ex) {
        super(ex);
    }

    public NATSException(String string, Exception ex) {
        super(string, ex);
    }

    /**
     * General-purpose NATS exception for asynchronous events.
     * 
     * @param ex the asynchronous exception
     * @param nc the NATS connection on which the event occurred (if applicable)
     * @param sub the {@code Subscription} on which the event occurred (if applicable)
     */
    public NATSException(Throwable ex, Connection nc, Subscription sub) {
        super(ex);
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
