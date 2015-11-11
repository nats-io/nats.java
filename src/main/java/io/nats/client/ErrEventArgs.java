package io.nats.client;

public class ErrEventArgs {
    private ConnectionImpl c;
    private SubscriptionImpl s;
    private String err;

    public ErrEventArgs(ConnectionImpl c, SubscriptionImpl s, String err)
    {
        this.c = c;
        this.s = s;
        this.err = err;
    }

    // Gets the connection associated with the event.
    public ConnectionImpl getConnection()
    {
        return c;
    }

    // Gets the SubscriptionImpl associated with the event.
    public SubscriptionImpl getSubscription()
    {
        return s;
    }

    // Gets the error associated with the event.
    public String getError()
    {
        return err;
    }
}
