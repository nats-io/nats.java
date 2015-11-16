package io.nats.client;

public class ConnExceptionArgs {
    private ConnectionImpl c;
    private Subscription s;
    private String err;

    public ConnExceptionArgs(ConnectionImpl c, Subscription subscription, String err)
    {
        this.c = c;
        this.s = subscription;
        this.err = err;
    }

    // Gets the connection associated with the event.
    public ConnectionImpl getConnection()
    {
        return c;
    }

    // Gets the Subscription associated with the event.
    public Subscription getSubscription()
    {
        return s;
    }

    // Gets the error associated with the event.
    public String getError()
    {
        return err;
    }
}
