package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamMessageTtlTtlOnDisabledStream {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            // This throwaway stream never enabled AllowMsgTTL, so it rejects any
            // per-message TTL header.
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name("ORDERS_NO_TTL")
                    .subjects("no-ttl.>")
                    .build();
            jsm.addStream(sc);

            JetStream js = nc.jetStream();

            // NATS-DOC-START
            // Publishing a Nats-TTL header to a stream without AllowMsgTTL is
            // rejected by the server, and nothing is stored.
            Headers headers = new Headers();
            headers.add("Nats-TTL", "60s");

            try {
                js.publish("no-ttl.canceled", headers,
                        "order 4242 canceled".getBytes(StandardCharsets.UTF_8));
            }
            catch (JetStreamApiException e) {
                // Error 10166: "per-message TTL is disabled". Enabling
                // AllowMsgTTL on the stream is the fix.
                System.out.println("Publish rejected [" + e.getApiErrorCode() + "]: "
                        + e.getErrorDescription());
            }
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
