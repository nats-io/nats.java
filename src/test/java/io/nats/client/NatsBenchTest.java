/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static io.nats.client.UnitTestUtilities.await;
import static io.nats.client.UnitTestUtilities.runDefaultServer;
import static io.nats.client.UnitTestUtilities.setLogLevel;
import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.nats.examples.NatsBench;

import ch.qos.logback.classic.Level;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;

@Category(PerfTest.class)
public class NatsBenchTest {
    ExecutorService service;
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = LoggerFactory.getLogger(NatsBenchTest.class);

    static final LogVerifier verifier = new LogVerifier();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {
        service = Executors.newCachedThreadPool(new NATSThreadFactory("natsbench"));
        MockitoAnnotations.initMocks(this);
        verifier.setup();
    }

    @After
    public void tearDown() throws Exception {
        service.shutdownNow();
        verifier.teardown();
        setLogLevel(Level.INFO);
    }

    @Test
    public void testSocketWriteSpeedNIO() throws IOException {
        try (NatsServer srv = runDefaultServer()) {
            final AtomicLong published = new AtomicLong(0);
            final InetSocketAddress hostAddress = new InetSocketAddress("localhost", 4222);
            final SocketChannel channel = SocketChannel.open(hostAddress);
            final ByteBuffer controlBuf = ByteBuffer.allocate(Parser.MAX_CONTROL_LINE_SIZE);
            final ByteBuffer sendBuf = ByteBuffer.allocate(8192);

            channel.configureBlocking(false);
            channel.socket().setSendBufferSize(8 * 1024 * 1024);
            channel.socket().setReceiveBufferSize(8 * 1024 * 1024);
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);

            // Read and verify INFO

            int bytesRead = channel.read(controlBuf);
            // System.err.printf("Read %d bytes:\n", bytesRead);
            String infoString = new String(controlBuf.array()).trim();
            // System.err.println(infoString);
            assertTrue(infoString.matches("INFO \\{.+"));

            // Send CONNECT
            sendBuf.put("CONNECT {\"verbose\":false}\r\n".getBytes());
            sendBuf.flip();
            while (sendBuf.hasRemaining()) {
                channel.write(sendBuf);
            }

            String payload = "";
            String control = String.format("PUB foo %d\r\n%s\r\n", payload.length(), payload);
            byte[] buf = control.getBytes();

            int count = 2 * 1000 * 1000;

            long t0 = System.nanoTime();
            for (int i = 0; i < count; i++) {
                try {
                    sendBuf.clear();
                    sendBuf.put(buf);
                    sendBuf.flip();
                    while (sendBuf.hasRemaining()) {
                        channel.write(sendBuf);
                    }
                    published.incrementAndGet();
                } catch (IOException e) {
                    logger.error("Oops, got an exception on msg {}: {}", i, e.getMessage());
                    break;
                }
            }
            long elapsed = System.nanoTime() - t0;
            long msgPerSec = published.get() / TimeUnit.NANOSECONDS.toSeconds(elapsed);
            long bytesPerSec =
                    (published.get() * buf.length) / TimeUnit.NANOSECONDS.toSeconds(elapsed);
            String secString = String.format("%.2f", (double) elapsed / 1000000000.0);
            logger.info("Published {} msgs in {} sec (rate: {} msg/sec, {} bytes/sec)",
                    NumberFormat.getNumberInstance(Locale.US).format(published.get()), secString,
                    NumberFormat.getNumberInstance(Locale.US).format(msgPerSec),
                    NumberFormat.getNumberInstance(Locale.US).format(bytesPerSec));
            channel.close();
        }
    }

    @Test
    public void testSocketWriteSpeed() {
        InetSocketAddress hostAddress = new InetSocketAddress("localhost", 4222);
        SocketFactory factory = SocketFactory.getDefault();
        final AtomicLong published = new AtomicLong(0);
        try (NatsServer srv = runDefaultServer()) {
            try (Socket client = factory.createSocket()) {
                client.setTcpNoDelay(true);
                client.setReceiveBufferSize(2 * 1024 * 1024);
                client.setSendBufferSize(2 * 1024 * 1024);
                client.connect(hostAddress, 2000);

                OutputStream writeStream = client.getOutputStream();
                InputStream readStream = client.getInputStream();
                BufferedOutputStream bw = new BufferedOutputStream(writeStream, 65536);
                BufferedReader reader = new BufferedReader(new InputStreamReader(readStream));

                // Read and verify INFO
                String infoString = reader.readLine();
                // System.err.println(infoString);
                assertTrue(infoString.matches("INFO \\{.+"));

                // Send CONNECT
                bw.write("CONNECT {\"verbose\":false}\r\n".getBytes());
                bw.flush();
                if (reader.ready()) {
                    String info = reader.readLine();
                    // System.err.println(info);
                }
                String payload = "";
                String control = String.format("PUB foo %d\r\n%s\r\n", payload.length(), payload);
                byte[] buf = control.getBytes();

                int count = 50 * 1000 * 1000;

                long t0 = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    try {
                        bw.write(buf, 0, buf.length);
                        published.incrementAndGet();
                    } catch (Exception e) {
                        System.err.printf("Oops, got an exception on msg %d: %s\n", i,
                                e.getMessage());
                        break;
                    }
                }
                bw.flush();
                long elapsed = System.nanoTime() - t0;
                long msgPerSec = published.get() / TimeUnit.NANOSECONDS.toSeconds(elapsed);
                long bytesPerSec =
                        (published.get() * buf.length) / TimeUnit.NANOSECONDS.toSeconds(elapsed);
                String secString = String.format("%.2f", (double) elapsed / 1000000000.0);
                logger.info("Published {} msgs in {} sec (rate: {} msg/sec, {} bytes/sec)",
                        NumberFormat.getNumberInstance(Locale.US).format(published.get()),
                        secString, NumberFormat.getNumberInstance(Locale.US).format(msgPerSec),
                        NumberFormat.getNumberInstance(Locale.US).format(bytesPerSec));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testSocketWriteSpeedWithExecutor() throws InterruptedException {
        final AtomicLong published = new AtomicLong(0);
        final CountDownLatch done = new CountDownLatch(1);
        InetSocketAddress hostAddress = new InetSocketAddress("localhost", 4222);
        SocketFactory factory = SocketFactory.getDefault();
        try (NatsServer srv = runDefaultServer()) {
            try (Socket client = factory.createSocket()) {
                client.setTcpNoDelay(true);
                client.setReceiveBufferSize(2 * 1024 * 1024);
                client.setSendBufferSize(2 * 1024 * 1024);
                client.connect(hostAddress, 2000);

                final OutputStream writeStream = client.getOutputStream();
                final InputStream readStream = client.getInputStream();
                final BufferedOutputStream bw = new BufferedOutputStream(writeStream, 65536);
                final BufferedReader reader = new BufferedReader(new InputStreamReader(readStream));

                // Read and verify INFO
                String infoString = reader.readLine();
                // System.err.println(infoString);
                assertTrue(infoString.matches("INFO \\{.+"));

                // Send CONNECT
                bw.write("CONNECT {\"verbose\":false}\r\n".getBytes());
                bw.flush();
                if (reader.ready()) {
                    String response = reader.readLine();
                    logger.error(response);
                }
                String payload = "";
                String control = String.format("PUB foo %d\r\n%s\r\n", payload.length(), payload);
                final byte[] buf = control.getBytes();

                final int count = 3 * 1000 * 1000;

                long t0 = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    service.execute(new Runnable() {
                        public void run() {
                            try {
                                bw.write(buf, 0, buf.length);
                                if (published.incrementAndGet() >= count) {
                                    done.countDown();
                                }
                            } catch (Exception e) {
                                System.err.printf("Oops, got an exception on msg %d: %s\n",
                                        published.get(), e.getMessage());
                            }
                        }
                    });
                }
                done.await();
                bw.flush();
                long t1 = System.nanoTime();
                logger.info(perfString(t0, t1, published.get(), published.get() * buf.length));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    String perfString(long t0, long t1, long msgs, long bytes) {
        long elapsed = t1 - t0;
        long msgPerSec = msgs / TimeUnit.NANOSECONDS.toSeconds(elapsed);
        long bytesPerSec = bytes / TimeUnit.NANOSECONDS.toSeconds(elapsed);
        String secString = String.format("%.2f", (double) elapsed / 1000000000.0);
        String result =
                String.format("Published %s msgs in %s sec (rate: %s msg/sec, %s bytes/sec)",
                        NumberFormat.getNumberInstance(Locale.US).format(msgs), secString,
                        NumberFormat.getNumberInstance(Locale.US).format(msgPerSec),
                        NumberFormat.getNumberInstance(Locale.US).format(bytesPerSec));
        return result;
    }

    @Test
    public void testAsyncReqRepSpeed() throws Exception {
        final int count = 50000;
        final String reqSubject = "request";

        final SynchronousQueue<String> ch = new SynchronousQueue<String>();
        final ConnectionFactory cf = new ConnectionFactory();

        try (NatsServer srv = runDefaultServer()) {
            try (ConnectionImpl nc = (ConnectionImpl) cf.createConnection()) {
                final Subscription sub = nc.subscribe(reqSubject, new MessageHandler() {
                    public void onMessage(final Message msg) {
                        service.submit(new Runnable() {
                            public void run() {
                                try {
                                    ch.put("");
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                    }
                });
                final long t0 = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    nc.publish(reqSubject, null, null, true);
                    ch.take();
                }
                final long elapsed = System.nanoTime() - t0;
                logger.info("requestor connection stats: {}", nc.getStats());
                logger.info("elapsed time: {}sec",
                        String.format("%.2f", (double) elapsed / 1000000000));
                logger.info("req-rep average round trip: {}ms", String.format("%.2f",
                        (double) TimeUnit.NANOSECONDS.toMillis(elapsed) / count));
                logger.info("req/sec: {}", String.format("%.2f",
                        (double) count / TimeUnit.NANOSECONDS.toSeconds(elapsed)));
                sub.close();
            }
        }
    }

    @Test
    public void testNatsBench() throws Exception {
        try (NatsServer srv = runDefaultServer()) {
            Properties props = new Properties();
            props.setProperty("bench.nats.servers", "nats://localhost:4222");
            props.setProperty("bench.nats.secure", "false");
            props.setProperty("bench.nats.msg.count", "100000000");
            props.setProperty("bench.nats.msg.size", "0");
            props.setProperty("bench.nats.secure", "false");
            props.setProperty("bench.nats.pubs", "1");
            props.setProperty("bench.nats.subs", "0");
            props.setProperty("bench.nats.subject", "foo");

            new NatsBench(props).run();
        }
    }

    @Test
    public void testPubSpeed() throws Exception {
        int count = 100 * 1000 * 1000;
        String url = ConnectionFactory.DEFAULT_URL;
        final String subject = "foo";
        final byte[] payload = null;
        long elapsed = 0L;

        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = new ConnectionFactory(url).createConnection()) {
                final Message msg = new Message(subject, null, payload);

                final long t0 = System.nanoTime();

                for (int i = 0; i < count; i++) {
                    c.publish(subject, payload);
                    // c.publish(msg);
                }

                assertEquals(count, c.getStats().getOutMsgs());

                // Make sure they are all processed
                c.flush();

                long t1 = System.nanoTime();

                elapsed = TimeUnit.NANOSECONDS.toSeconds(t1 - t0);
                System.out.println("Elapsed time is " + elapsed + " seconds");

                System.out.printf("Published %s msgs in %d seconds ",
                        NumberFormat.getNumberInstance(Locale.US).format(count), elapsed);
                if (elapsed > 0) {
                    System.out.printf("(%s msgs/second).\n", NumberFormat
                            .getNumberInstance(Locale.US).format((int) (count / elapsed)));
                } else {
                    fail("Test not long enough to produce meaningful stats.");
                }
                logger.info("publisher connection stats: {}", c.getStats());
            }
        }
    }

    // @Test
    public void testPubSubSpeed() throws Exception {
        final int count = 10000000;
        final int MAX_BACKLOG = 32000;
        // final int count = 2000;
        final String url = ConnectionFactory.DEFAULT_URL;
        final String subject = "foo";
        final byte[] payload = "test".getBytes();
        long elapsed = 0L;

        final CountDownLatch ch = new CountDownLatch(1);
        final CountDownLatch ready = new CountDownLatch(1);
        final BlockingQueue<Long> slow = new LinkedBlockingQueue<Long>();

        final AtomicInteger received = new AtomicInteger(0);

        final ConnectionFactory cf = new ConnectionFactory(url);

        cf.setExceptionHandler(new ExceptionHandler() {
            public void onException(NATSException e) {
                logger.error("Error: {}", e.getMessage());
                Subscription sub = e.getSubscription();
                logger.error("Queued = {}", sub.getQueuedMessageCount());
                e.printStackTrace();
                // fail(e.getMessage());
            }
        });

        Runnable r = new Runnable() {
            public void run() {
                try (Connection nc1 = cf.createConnection()) {
                    Subscription sub = nc1.subscribe(subject, new MessageHandler() {
                        public void onMessage(Message msg) {
                            int numReceived = received.incrementAndGet();
                            if (numReceived >= count) {
                                ch.countDown();
                            }
                        }
                    });
                    ready.countDown();
                    while (received.get() < count) {
                        if (sub.getQueuedMessageCount() > 8192) {
                            logger.error("queued={}", sub.getQueuedMessageCount());
                            if (slow.size() == 0) {
                                slow.add((long) sub.getQueuedMessageCount());
                            }
                            sleep(1);
                        }
                    }
                    System.out.println("nc1 (subscriber):\n=======");
                    logger.info("subscriber connection stats: {}", nc1.getStats());
                } catch (IOException | TimeoutException e) {
                    fail(e.getMessage());
                }
            }
        };

        Thread subscriber = new Thread(r);
        subscriber.start();

        try (final Connection nc2 = cf.createConnection()) {
            final long t0 = System.nanoTime();
            long numSleeps = 0L;

            ready.await();
            int sleepTime = 100;

            for (int i = 0; i < count; i++) {
                nc2.publish(subject, payload);
                // Don't overrun ourselves and be a slow consumer, server will cut us off
                if ((i - received.get()) > MAX_BACKLOG) {
                    int rec = received.get();
                    int sent = i + 1;
                    int backlog = sent - rec;

                    // System.err.printf("sent=%d, received=%d, backlog=%d, ratio=%.2f,
                    // sleepInt=%d\n",
                    // sent, rec, backlog, (double) backlog / cf.getMaxPendingMsgs(),
                    // sleepTime);

                    // sleepTime += 100;
                    sleep(sleepTime);
                    numSleeps++;
                }
            }
            System.err.println("numSleeps=" + numSleeps);

            // Make sure they are all processed
            String str = String.format("Timed out waiting for delivery completion, received %d/%d",
                    received.get(), count);
            assertTrue(str, await(ch, 30, TimeUnit.SECONDS));
            assertEquals(count, received.get());
            try {
                subscriber.join();
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            long t1 = System.nanoTime();

            elapsed = TimeUnit.NANOSECONDS.toSeconds(t1 - t0);
            System.out.println("Elapsed time is " + elapsed + " seconds");

            System.out.printf("Pub/Sub %d msgs in %d seconds ", count, elapsed);
            if (elapsed > 0) {
                System.out.printf("(%d msgs/second).\n", (int) (count / elapsed));
            } else {
                fail("Test not long enough to produce meaningful stats.");
            }
            System.out.println("nc2 (publisher):\n=======");
            logger.info("publisher connection stats: {}", nc2.getStats());
        }
    }

    // @Test
    // @Category(PerfTest.class)
    public void testManyConnections() throws Exception {
        try (NatsServer s = new NatsServer()) {
            ConnectionFactory cf = new ConnectionFactory();
            List<Connection> conns = new ArrayList<Connection>();
            for (int i = 0; i < 10000; i++) {
                Connection conn = cf.createConnection();
                conns.add(conn);
                System.err.printf("Created %d connections\n", i);
            }
        }
    }
}
