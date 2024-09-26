package io.nats.RequestMany;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.NUID;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.nats.RequestMany.RequestMany.DEFAULT_TOTAL_WAIT_TIME_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestManyTests extends TestBase {

    enum Last{ Normal, Status, Ex }

    private void assertMessages(int regularMessages, Last last, List<RequestManyMessage> list) {
        assertEquals(regularMessages + 1, list.size());
        for (int x = 0; x < regularMessages; x++) {
            assertTrue(list.get(x).isRegularMessage());
        }
        RequestManyMessage lastRmm = list.get(regularMessages);
        switch (last) {
            case Normal: assertTrue(lastRmm.isNormalEndOfData()); break;
            case Status: assertTrue(lastRmm.isStatusMessage()); break;
            case Ex: assertTrue(lastRmm.isException()); break;
        }
    }

    @Test
    public void testNoRespondersGather() throws Exception {
        runInServer(nc -> {
            String subject = subject();
            RequestMany rm = maxResponseRequest(nc);
            TestRequestManyHandler handler = new TestRequestManyHandler();
            rm.gather(subject, null, handler);
            assertTrue(handler.eodReceived.await(3, TimeUnit.SECONDS));
            assertMessages(0, Last.Status, handler.list);
        });
    }

    private static RequestMany maxResponseRequest(Connection nc) {
        return RequestMany.builder(nc).maxResponses(3).build();
    }

    @Test
    public void testMaxResponseFetch() throws Exception {
        runInServer(nc -> {
            try (Replier replier = new Replier(nc, 5)) {
                RequestMany rm = maxResponseRequest(nc);
                List<RequestManyMessage> list = rm.fetch(replier.subject, null);
                assertMessages(3, Last.Normal, list);
            }
        });
    }

    @Test
    public void testMaxResponseIterate() throws Exception {
        runInServer(nc -> {
            try (Replier replier = new Replier(nc, 5)) {
                RequestMany rm = maxResponseRequest(nc);
                LinkedBlockingQueue<RequestManyMessage> it = rm.iterate(replier.subject, null);
                List<RequestManyMessage> list = new ArrayList<>();
                RequestManyMessage m = it.poll(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                while (m != null) {
                    list.add(m);
                    if (m.isEndOfData()) {
                        break;
                    }
                    m = it.poll(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                }
                assertMessages(3, Last.Normal, list);
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
                assertMessages(3, Last.Normal, handler.list);
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

    private void _testMaxWaitTimeFetch(Connection nc, long wait) throws Exception {
        try (Replier replier = new Replier(nc, 1, wait + 200, 1)) {
            RequestMany rm = maxWaitTimeRequest(nc, wait);

            long start = System.currentTimeMillis();
            List<RequestManyMessage> list = rm.fetch(replier.subject, null);
            long elapsed = System.currentTimeMillis() - start;

            assertTrue(elapsed > wait);
            assertMessages(1, Last.Normal, list);
        }
    }

    @Test
    public void testMaxWaitTimeIterate() throws Exception {
        runInServer(nc -> {
            try (Replier replier = new Replier(nc, 1, 1200, 1)) {
                RequestMany rm = maxWaitTimeRequest(nc);

                LinkedBlockingQueue<RequestManyMessage> it = rm.iterate(replier.subject, null);
                int received = 0;
                RequestManyMessage m = it.poll(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                while (m != null && !m.isEndOfData()) {
                    received++;
                    m = it.poll(DEFAULT_TOTAL_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                }

                assertEquals(1, received);
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
                assertMessages(1, Last.Normal, handler.list);
            }
        });
    }

    // ----------------------------------------------------------------------------------------------------
    // Support Classes
    // ----------------------------------------------------------------------------------------------------
    static class TestRequestManyHandler implements RequestManyHandler {
        public final CountDownLatch eodReceived = new CountDownLatch(1);
        public List<RequestManyMessage> list = new ArrayList<>();

        @Override
        public boolean gather(RequestManyMessage rmm) {
            list.add(rmm);
            if (rmm.isEndOfData()) {
                eodReceived.countDown();
            }
            return true;
        }
    }

    static class Replier implements AutoCloseable {
        final Dispatcher dispatcher;
        public final String subject;

        public Replier(final Connection nc, final int count) {
            this(nc, count, -1, -1);
        }

        public Replier(final Connection nc, final int count, final long pause, final int count2) {
            this.subject = NUID.nextGlobalSequence();

            dispatcher = nc.createDispatcher(m -> {
                for (int x = 0; x < count; x++) {
                    nc.publish(m.getReplyTo(), null);
                }
                if (pause > 0) {
                    sleep(pause);
                    for (int x = 0; x < count2; x++) {
                        nc.publish(m.getReplyTo(), null);
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
