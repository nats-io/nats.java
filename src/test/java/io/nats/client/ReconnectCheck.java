package io.nats.client;

import java.io.IOException;
import java.time.Duration;

/* Program to reproduce #231 */
public class ReconnectCheck {
    private static long received;
    private static long published;

    private static long lastReceivedId;

    public static void main(String []args) throws IOException, InterruptedException {
        Connection natsIn = Nats.connect(buildOptions("IN"));
        Connection natsOut = Nats.connect(buildOptions("OUT"));

        Dispatcher natsDispatcher = natsIn.createDispatcher(m -> {
            long receivedId = Long.parseLong(new String(m.getData()));

            if (receivedId < lastReceivedId) {
                System.out.printf("##### Tid: %d, Received stale data: got %d, last received %d%n", Thread.currentThread().getId(), receivedId, lastReceivedId);
            }
            lastReceivedId = receivedId;

            if (received++ % 1_000_000 == 0) {
                System.out.printf("Tid: %d, Received %d messages%n", Thread.currentThread().getId(), received);
            }
        });

        natsDispatcher.subscribe("foo");

        long id = 0;

        while (true) {
            for (int i = 0; i < 100_000; i++) {
                natsOut.publish("foo", ("" + id++).getBytes());
                if (published++ % 1_000_000 == 0) {
                    System.out.printf("Tid: %d, Published %d messages.%n", Thread.currentThread().getId(), published);
                }
            }
            Thread.sleep(1);
        }
    }

    private static Options buildOptions(String name) {
        Options.Builder natsOptions = new Options.Builder().servers(new String[]{"nats://127.0.0.1:4222"})
                .connectionName(name);
        natsOptions.maxReconnects(-1).reconnectWait(Duration.ofSeconds(1))
                .connectionTimeout(Duration.ofSeconds(5));
        natsOptions.pingInterval(Duration.ofMillis(100));
        natsOptions.connectionListener((conn, e) ->
                System.out.printf("Tid: %d, %s, NATS: connection event - %s, connected url: %s. servers: %s %n", Thread.currentThread().getId(), name, e, conn.getConnectedUrl(), conn.getServers()));
        natsOptions.errorListener(new ErrorListener() {
            @Override
            public void slowConsumerDetected(Connection conn, Consumer consumer) {
                System.out.printf("Tid: %d, %s, %s: Slow Consumer%n", Thread.currentThread().getId(), name, conn.getConnectedUrl());
            }

            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {
                System.out.printf("Tid: %d, %s, Nats '%s' exception: %s%n", Thread.currentThread().getId(), name, conn.getConnectedUrl(), exp.toString());
            }

            @Override
            public void errorOccurred(Connection conn, String error) {
                System.out.printf("Tid: %d, %s, Nats '%s': Error %s%n", Thread.currentThread().getId(), name, conn.getConnectedUrl(), error);
            }
        });

        // Do not cache any messages when Nats connection is down.
        natsOptions.reconnectBufferSize(-1);
        return natsOptions.build();
    }
}
