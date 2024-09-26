package io.nats.RequestMany;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.NUID;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.RequestMany.RequestMany.DEFAULT_TOTAL_WAIT_TIME_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestManyTests extends TestBase {

    private static RequestMany maxResponseRequest(Connection nc) {
        return RequestMany.builder(nc).maxResponses(3).build();
    }

    @Test
    public void testMaxResponseFetch() throws Exception {
        runInServer(nc -> {
            try (Replier replier = new Replier(nc, 5)) {
                RequestMany rm = maxResponseRequest(nc);
                List<Message> list = rm.fetch(replier.subject, null);
                assertEquals(3, list.size());
                assertTrue(replier.latch.await(1, TimeUnit.SECONDS));
            }
        });
    }

    @Test
    public void testMaxResponseIterate() throws Exception {
        runInServer(nc -> {
            try (Replier replier = new Replier(nc, 5)) {
                RequestMany rm = maxResponseRequest(nc);
                LinkedBlockingQueue<Message> it = rm.iterate(replier.subject, null);
                int count = 0;
                Message m = it.poll(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                while (m != null && m != RequestMany.EOD) {
                    count++;
                    m = it.poll(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                }
                assertEquals(3, count);
                assertTrue(replier.latch.await(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
            }
        });
    }

    @Test
    public void testMaxResponseGather() throws Exception {
        runInServer(nc -> {
            try (Replier replier = new Replier(nc, 5)) {
                RequestMany rm = maxResponseRequest(nc);
                TestRequestManyHandler handler = new TestRequestManyHandler();
                rm.gather(replier.subject, null, handler);
                assertTrue(handler.eodReceived.await(3, TimeUnit.SECONDS));
                assertEquals(3, handler.msgReceived.get());
                assertTrue(replier.latch.await(1, TimeUnit.SECONDS));
             }
        });
    }

    private static RequestMany maxWaitTimeRequest(Connection nc) {
        return RequestMany.builder(nc).build();
    }

    private static RequestMany maxWaitTimeRequest(Connection nc, long totalWaitTime) {
        return RequestMany.builder(nc).totalWaitTime(totalWaitTime).build();
    }

    @Test
    public void testMaxWaitTimeFetchDefault() throws Exception {
        runInServer(nc -> {
            _testMaxWaitTimeFetch(nc, DEFAULT_TOTAL_WAIT_TIME_MS);
        });
    }

    @Test
    public void testMaxWaitTimeFetchCustom() throws Exception {
        runInServer(nc -> {
            _testMaxWaitTimeFetch(nc, 500);
        });
    }

    private static void _testMaxWaitTimeFetch(Connection nc, long wait) throws Exception {
        try (Replier replier = new Replier(nc, 1, wait + 200, 1)) {
            RequestMany rm = maxWaitTimeRequest(nc, wait);

            long start = System.currentTimeMillis();
            List<Message> list = rm.fetch(replier.subject, null);
            long elapsed = System.currentTimeMillis() - start;

            assertTrue(elapsed > wait);
            assertEquals(1, list.size());
            assertTrue(replier.latch.await(1, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testMaxWaitTimeIterate() throws Exception {
        runInServer(nc -> {
            try (Replier replier = new Replier(nc, 1, 1200, 1)) {
                RequestMany rm = maxWaitTimeRequest(nc);

                LinkedBlockingQueue<Message> it = rm.iterate(replier.subject, null);
                int received = 0;
                Message m = it.poll(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                while (m != null && m != RequestMany.EOD) {
                    received++;
                    m = it.poll(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                }

                assertEquals(1, received);
                assertTrue(replier.latch.await(1, TimeUnit.SECONDS));
            }
        });
    }

    @Test
    public void testMaxWaitTimeGather() throws Exception {
        runInServer(nc -> {
            try (Replier replier = new Replier(nc, 1, 1200, 1)) {
                RequestMany rm = maxWaitTimeRequest(nc);

                TestRequestManyHandler handler = new TestRequestManyHandler();
                long start = System.currentTimeMillis();
                rm.gather(replier.subject, null, handler);
                assertTrue(handler.eodReceived.await(DEFAULT_TOTAL_WAIT_TIME_MS * 3 / 2, TimeUnit.MILLISECONDS));
                long elapsed = System.currentTimeMillis() - start;

                assertTrue(elapsed > DEFAULT_TOTAL_WAIT_TIME_MS && elapsed < (DEFAULT_TOTAL_WAIT_TIME_MS * 2));
                assertEquals(1, handler.msgReceived.get());
                assertTrue(replier.latch.await(1, TimeUnit.SECONDS));
            }
        });
    }

    // ----------------------------------------------------------------------------------------------------
    // Support Classes
    // ----------------------------------------------------------------------------------------------------
    static class TestRequestManyHandler implements RequestManyHandler {
        public final CountDownLatch eodReceived = new CountDownLatch(1);
        public final AtomicInteger msgReceived = new AtomicInteger();

        @Override
        public boolean gather(Message m) {
            if (m == RequestMany.EOD) {
                eodReceived.countDown();
            }
            else {
                msgReceived.incrementAndGet();
            }
            return true;
        }
    }

    static class Replier implements AutoCloseable {
        final Dispatcher dispatcher;
        public final String subject;
        public final CountDownLatch latch;

        public Replier(final Connection nc, final int count) {
            this(nc, count, -1, -1);
        }

        public Replier(final Connection nc, final int count, final long pause, final int count2) {
            this.subject = NUID.nextGlobalSequence();
            latch = new CountDownLatch(count + (pause > 0 ? count2 : 0));

            dispatcher = nc.createDispatcher(m -> {
                for (int x = 0; x < count; x++) {
                    nc.publish(m.getReplyTo(), null);
                    latch.countDown();
                }
                if (pause > 0) {
                    sleep(pause);
                    for (int x = 0; x < count2; x++) {
                        nc.publish(m.getReplyTo(), null);
                        latch.countDown();
                    }
                }
            });
            dispatcher.subscribe(subject);
        }

        public void close() throws Exception {
            dispatcher.unsubscribe(subject);
        }
    }
}
