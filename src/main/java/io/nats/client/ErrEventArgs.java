package io.nats.client;

import io.nats.client.impl.NATSConnection;
import io.nats.client.impl.NATSSubscription;

public class ErrEventArgs {
    private NATSConnection c;
    private NATSSubscription s;
    private String err;

    public ErrEventArgs(NATSConnection c, NATSSubscription s, String err)
    {
        this.c = c;
        this.s = s;
        this.err = err;
    }

    // Gets the connection associated with the event.
    public NATSConnection getConnection()
    {
        return c;
    }

    // Gets the NATSSubscription associated with the event.
    public NATSSubscription getSubscription()
    {
        return s;
    }

    // Gets the error associated with the event.
    public String getError()
    {
        return err;
    }
}
