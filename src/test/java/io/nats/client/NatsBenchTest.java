// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client;

import static io.nats.client.UnitTestUtilities.newDefaultConnection;
import static io.nats.client.UnitTestUtilities.runDefaultServer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.nats.examples.NatsBench;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;

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
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;

@Category(PerfTest.class)
public class NatsBenchTest extends BaseUnitTest {
    ExecutorService service;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Per-test-case setup.
     *
     * @throws Exception if something goes wrong.
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
        service = Executors.newCachedThreadPool(new NatsThreadFactory("natsbench"));
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Per-test-case cleanup.
     *
     * @throws Exception if something goes wrong.
     */
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        service.shutdownNow();
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
                    // TODO throw this?
                    System.err.printf("Got an exception on msg %d: %s\n", i, e.getMessage());
                    break;
                }
            }
            long elapsed = System.nanoTime() - t0;
            long msgPerSec = published.get() / TimeUnit.NANOSECONDS.toSeconds(elapsed);
            long bytesPerSec =
                    (published.get() * buf.length) / TimeUnit.NANOSECONDS.toSeconds(elapsed);
            String secString = String.format("%.2f", (double) elapsed / 1000000000.0);
            System.out.printf("Published %s msgs in %s sec (rate: %s msg/sec, %s bytes/sec)\n",
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
                        System.err.printf("Exception on msg %d: %s\n", i,
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
                System.out.printf("Published %s msgs in %s sec (rate: %s msg/sec, %s bytes/sec)",
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
                    System.err.println(response);
                }
                String payload = "";
                String control = String.format("PUB foo %d\r\n%s\r\n", payload.length(), payload);
                final byte[] buf = control.getBytes();

                final int count = 3 * 1000 * 1000;

                final long t0 = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    service.execute(new Runnable() {
                        public void run() {
                            try {
                                bw.write(buf, 0, buf.length);
                                if (published.incrementAndGet() >= count) {
                                    done.countDown();
                                }
                            } catch (Exception e) {
                                System.err.printf("Exception on msg %d: %s\n",
                                        published.get(), e.getMessage());
                            }
                        }
                    });
                }
                done.await();
                bw.flush();
                long t1 = System.nanoTime();
                System.out.println(perfString(t0, t1, published.get(), published.get() * buf.length));
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
        return String.format("Published %s msgs in %s sec (rate: %s msg/sec, %s bytes/sec)",
                NumberFormat.getNumberInstance(Locale.US).format(msgs), secString,
                NumberFormat.getNumberInstance(Locale.US).format(msgPerSec),
                NumberFormat.getNumberInstance(Locale.US).format(bytesPerSec));
    }

    @Test
    public void testAsyncReqRepSpeed() throws Exception {
        final int count = 50000;
        final String reqSubject = "request";

        final SynchronousQueue<String> ch = new SynchronousQueue<String>();

        try (NatsServer srv = runDefaultServer()) {
            try (ConnectionImpl nc = (ConnectionImpl) newDefaultConnection()) {
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
                Statistics s = nc.getStats();
                System.out.println("requestor connection stats:");
                System.out.printf("    Bytes  in:  %d\n", s.getInBytes());
                System.out.printf("    Msgs   in:  %d\n", s.getInMsgs());
                System.out.printf("    Bytes out:  %d\n", s.getOutBytes());
                System.out.printf("    Msgs  out:  %d\n", s.getOutMsgs());
                System.out.printf("elapsed time: %s sec\n",
                        String.format("%.2f", (double) elapsed / 1000000000));
                System.out.printf("req-rep average round trip: %.2f ms\n",
                        (double) TimeUnit.NANOSECONDS.toMillis(elapsed) / count);
                System.out.printf("req/sec: %.2f\n",
                        (double) count / TimeUnit.NANOSECONDS.toSeconds(elapsed));
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
        final String subject = "foo";
        final byte[] payload = null;
        long elapsed = 0L;

        try (NatsServer srv = runDefaultServer()) {
            try (Connection c = newDefaultConnection()) {
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
                Statistics s = c.getStats();
                System.out.println("publisher connection stats:");
                System.out.printf("    Bytes out:  %d\n", s.getOutBytes());
                System.out.printf("    Msgs  out:  %d\n", s.getOutMsgs());
            }
        }
    }
}
