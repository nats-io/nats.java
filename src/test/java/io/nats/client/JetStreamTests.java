package io.nats.client;

import static org.junit.Assert.*;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

// Assumes ./nats-server -js is running:
public class JetStreamTests {
    @Test
    public void testJSNext() throws Exception {
        Options opts = new Options.Builder()
            .oldRequestStyle()
            .build();
        try (Connection nats = Nats.connect(opts)) {
            // Create a stream:
            nats.request("$JS.API.STREAM.CREATE.STREAM1",
                        "{\"name\":\"STREAM1\",\"subjects\":[\"STREAM1.*\"],\"retention\":\"workqueue\",\"max_consumers\":-1,\"max_msgs\":-1,\"max_bytes\":-1,\"max_age\":0,\"max_msg_size\":-1,\"storage\":\"file\",\"discard\":\"old\",\"num_replicas\":1}".getBytes(),
                        Duration.ofSeconds(15));
            // Create a consumer:
            nats.request("$JS.API.CONSUMER.DURABLE.CREATE.STREAM1.CONSUMER1",
                        "{\"stream_name\":\"STREAM1\",\"config\":{\"durable_name\":\"CONSUMER1\",\"deliver_policy\":\"all\",\"ack_policy\":\"explicit\",\"max_deliver\":-1,\"replay_policy\":\"instant\"}}".getBytes(),
                        Duration.ofSeconds(15));
            // Enqueue a message:
            nats.request("STREAM1.CONSUMER1", "msg1".getBytes(), Duration.ofSeconds(15));
            // Request from the stream, always times-out:
            nats.request("$JS.API.CONSUMER.MSG.NEXT.STREAM1.CONSUMER1", new byte[0]).get(15L, TimeUnit.SECONDS);
        }
    }
}