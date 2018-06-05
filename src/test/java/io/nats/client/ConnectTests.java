package io.nats.client;

import org.junit.Test;

public class ConnectTests {
    @Test
    public void testDefaultConnection() {
        Connection nc = Nats.connect();
    }
    
    @Test
    public void testConnection() {
        Connection nc = Nats.connect("foo");
    }
    
    @Test
    public void testConnectionWithOptions() {
        Options options = new Options.Builder().build();
        Connection nc = Nats.connect("foo", options);
    }
}