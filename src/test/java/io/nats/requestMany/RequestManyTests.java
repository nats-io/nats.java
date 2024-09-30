package io.nats.requestMany;

import io.nats.client.*;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.nats.client.support.NatsConstants.NANOS_PER_MILLI;
import static io.nats.requestMany.RequestMany.MAX_MILLIS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestManyTests extends TestBase {

    public static final int TEST_TWT = 1000;
    public static final int MAX_WAIT_PAUSE = 1200;
    public static final int STALL_WAIT = 300;
    public static final int STALL_PAUSE = 400;
    public static final int SHORT_CIRCUIT_TIME = 500;
    public static final int MAX_RESPONSES_RESPONDERS = 5;
    public static final int MAX_RESPONSES = 2;

    static NatsTestServer TS;
    static Connection NC;
    static long DEFAULT_TIMEOUT;

    @BeforeAll
    public static void beforeAll() {
        try {
            TS = new NatsTestServer(false, false);
            NC = standardConnection(Options.builder().server(TS.getURI()).build());
            DEFAULT_TIMEOUT = NC.getOptions().getConnectionTimeout().toMillis();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void afterAll() {
        try {
            NC.close();
        }
        catch (InterruptedException ignore) {}
        try {
            TS.close();
        }
        catch (Exception ignore) {}
    }

    enum Last{ Normal, Status, Ex, None }

    private static RequestMany.Builder builder() {
        return RequestMany.builder(NC);
    }

    private static RequestMany noResponsesRequestMany() {
        return builder().build();
    }

    private static RequestMany maxResponsesRequestMany() {
        return builder().maxResponses(MAX_RESPONSES).totalWaitTime(TEST_TWT).build();
    }

    private static RequestMany totalWaitTimeRequestMany() {
        return builder().totalWaitTime(TEST_TWT).maxStall(MAX_MILLIS).build();
    }

    private static RequestMany stallRequestMany() {
        return builder().maxStall(STALL_WAIT).totalWaitTime(TEST_TWT).build();
    }

    private void assertMessages(int regularMessages, Last last, List<RmMessage> list) {
        for (int x = 0; x < regularMessages; x++) {
            assertTrue(list.get(x).isDataMessage());
        }
        if (last == Last.None) {
            assertEquals(regularMessages, list.size());
        }
        else {
            assertEquals(regularMessages + 1, list.size());
            RmMessage lastRmm = list.get(regularMessages);
            switch (last) {
                case Normal: assertTrue(lastRmm.isNormalEndOfData()); break;
                case Status: assertTrue(lastRmm.isStatusMessage()); break;
                case Ex: assertTrue(lastRmm.isException()); break;
            }
        }
    }

    private static RmHandlerAndResult _gather(RequestMany rm, String subject) {
        RmHandlerAndResult handler = new RmHandlerAndResult();
        long start = System.currentTimeMillis();
        rm.gather(subject, null, handler);
        handler.elapsed = System.currentTimeMillis() - start;
        return handler;
    }

    private static Result _fetch(RequestMany rm, String subject) {
        Result result = new Result();
        long start = System.currentTimeMillis();
        result.list = rm.fetch(subject, null);
        result.elapsed = System.currentTimeMillis() - start;
        return result;
    }

    private static Result _iterate(RequestMany rm, String subject) throws InterruptedException {
        LinkedBlockingQueue<RmMessage> it = rm.iterate(subject, null);
        Result result = new Result();
        long start = System.nanoTime();
        long stop = start + (DEFAULT_TIMEOUT * NANOS_PER_MILLI);
        while (System.nanoTime() < stop) {
            RmMessage m = it.poll(1, TimeUnit.MILLISECONDS);
            if (m != null) {
                result.list.add(m);
                if (m.isEndOfData()) {
                    break;
                }
            }
        }
        result.elapsed = (System.nanoTime() - start) / NANOS_PER_MILLI;
        return result;
    }

    @Test
    public void testNoRespondersGather() throws Exception {
        RmHandlerAndResult result = _gather(noResponsesRequestMany(), subject());
        assertMessages(0, Last.Status, result.list);
        assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);
        assertTrue(result.eodReceived.await(SHORT_CIRCUIT_TIME, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testNoRespondersFetch() throws Exception {
        Result result = _fetch(noResponsesRequestMany(), subject());
        assertMessages(0, Last.Status, result.list);
        assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);
    }
    
    @Test
    public void testNoRespondersIterate() throws Exception {
        Result result = _iterate(noResponsesRequestMany(), subject());
        assertMessages(0, Last.Status, result.list);
        assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);
    }

    @Test
    public void testMaxResponsesGather() throws Exception {
        try (Responder responder = new Responder(MAX_RESPONSES_RESPONDERS)) {
            RmHandlerAndResult result = _gather(maxResponsesRequestMany(), responder.subject);
            assertMessages(MAX_RESPONSES, Last.Normal, result.list);
            assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);
            assertTrue(result.eodReceived.await(SHORT_CIRCUIT_TIME, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testMaxResponsesFetch() throws Exception {
        try (Responder responder = new Responder(MAX_RESPONSES_RESPONDERS)) {
            Result result = _fetch(maxResponsesRequestMany(), responder.subject);
            assertMessages(MAX_RESPONSES, Last.None, result.list);
            assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);
        }
    }
    
    @Test
    public void testMaxResponsesIterate() throws Exception {
        try (Responder responder = new Responder(MAX_RESPONSES_RESPONDERS)) {
            Result result = _iterate(maxResponsesRequestMany(), responder.subject);
            assertMessages(MAX_RESPONSES, Last.Normal, result.list);
            assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);
        }
    }

    @Test
    public void testMaxWaitGather() throws Exception {
        try (Responder responder = new Responder(1, MAX_WAIT_PAUSE, 1)) {
            RmHandlerAndResult result = _gather(totalWaitTimeRequestMany(), responder.subject);
            assertMessages(1, Last.Normal, result.list);
            assertTrue(result.elapsed >= TEST_TWT);
            assertTrue(result.eodReceived.await(TEST_TWT * 2, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void testMaxWaitFetch() throws Exception {
        try (Responder responder = new Responder(1, MAX_WAIT_PAUSE, 1)) {
            Result result = _fetch(totalWaitTimeRequestMany(), responder.subject);
            assertMessages(1, Last.None, result.list);
            assertTrue(result.elapsed >= TEST_TWT);
        }
    }

    @Test
    public void testMaxWaitIterate() throws Exception {
        try (Responder responder = new Responder(1, MAX_WAIT_PAUSE, 1)) {
            Result result = _iterate(totalWaitTimeRequestMany(), responder.subject);
            assertMessages(1, Last.Normal, result.list);
            assertTrue(result.elapsed >= TEST_TWT);
        }
    }

    @Test
    public void testStallGather() throws Exception {
        try (Responder responder = new Responder(1, STALL_PAUSE, 1)) {
            RmHandlerAndResult result = _gather(stallRequestMany(), responder.subject);
            assertMessages(1, Last.Normal, result.list);
            assertTrue(result.elapsed <= TEST_TWT);
            assertTrue(result.eodReceived.await(STALL_WAIT * 2, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void testStallFetch() throws Exception {
        try (Responder responder = new Responder(1, STALL_PAUSE, 1)) {
            Result result = _fetch(stallRequestMany(), responder.subject);
            assertMessages(1, Last.None, result.list);
            assertTrue(result.elapsed <= TEST_TWT);
        }
    }

    @Test
    public void testStallIterate() throws Exception {
        try (Responder responder = new Responder(1, STALL_PAUSE, 1)) {
            Result result = _iterate(stallRequestMany(), responder.subject);
            assertMessages(1, Last.Normal, result.list);
            assertTrue(result.elapsed <= TEST_TWT);
        }
    }

    @Test
    public void testBuilder() {
        // if you don't set total wait time or stall, both default.
        // the default total wait time is the connections options timeout
        // the default stall time is 1/10 of the default total wait time.
        assertBuilder(-1, -1, -1, builder().build());
        assertBuilder(-1, -1, -1, builder().totalWaitTime(0).build());
        assertBuilder(-1, -1, -1, builder().totalWaitTime(-1).build());
        assertBuilder(-1, -1, -1, builder().totalWaitTime(MAX_MILLIS + 1).build());

        // if you set total wait time, but not stall, stall defaults 1/10th of total wait time
        assertBuilder(1000, 100, -1, builder().totalWaitTime(1000).build());
        assertBuilder(MAX_MILLIS, MAX_MILLIS / 10, -1, builder().totalWaitTime(MAX_MILLIS).build());

        // if you set max stall to a value between 1 and MAX_MILLIS inclusive, max stall is that value
        assertBuilder(-1, 1, -1, builder().maxStall(1).build());
        assertBuilder(-1, MAX_MILLIS, -1, builder().maxStall(MAX_MILLIS).build());

        // if you set max stall to an invalid value, it's like clearing it to default behavior
        assertBuilder(-1, -1, -1, builder().maxStall(0).build());
        assertBuilder(-1, -1, -1, builder().maxStall(-1).build());
        assertBuilder(-1, -1, -1, builder().maxStall(MAX_MILLIS + 1).build());

        // if you set max responses to a value greater than 0, max responses is that value
        assertBuilder(-1, -1, 10, builder().maxResponses(10).build());

        // if you set max responses to an invalid value, max responses is Long.MAX_VALUE
        assertBuilder(-1, -1, -1, builder().maxResponses(0).build());
        assertBuilder(-1, -1, -1, builder().maxResponses(-1).build());
    }

    private void assertBuilder(long exTo, long exStall, long exResp, RequestMany rm) {
        assertEquals(exTo == -1 ? Options.DEFAULT_CONNECTION_TIMEOUT.toMillis() : exTo, rm.getTotalWaitTime());
        assertEquals(exStall == -1 ? Options.DEFAULT_CONNECTION_TIMEOUT.toMillis() / 10 : exStall, rm.getMaxStall());
        assertEquals(exResp == -1 ? Long.MAX_VALUE : exResp, rm.getMaxResponses());
    }

    // ----------------------------------------------------------------------------------------------------
    // Support Classes
    // ----------------------------------------------------------------------------------------------------

    static class Result {
        public List<RmMessage> list = new ArrayList<>();
        public long elapsed;
    }

    static class RmHandlerAndResult extends Result implements RmHandler {
        public final CountDownLatch eodReceived = new CountDownLatch(1);

        @Override
        public boolean gather(RmMessage rmm) {
            list.add(rmm);
            if (rmm.isEndOfData()) {
                eodReceived.countDown();
            }
            return true;
        }
    }

    static class Responder implements AutoCloseable {
        final Dispatcher dispatcher;
        public final String subject;

        public Responder(final int count) {
            this(count, -1, -1);
        }

        public Responder(final int count, final long pause, final int count2) {
            this.subject = NUID.nextGlobalSequence();

            dispatcher = NC.createDispatcher(m -> {
                for (int x = 0; x < count; x++) {
                    NC.publish(m.getReplyTo(), null);
                }
                if (pause > 0) {
                    sleep(pause);
                    for (int x = 0; x < count2; x++) {
                        NC.publish(m.getReplyTo(), null);
                    }
                }
            });
            dispatcher.subscribe(subject);
        }

        public void close() throws Exception {
            try {
                dispatcher.unsubscribe(subject);
            }
            catch (Exception ignore) {}
        }
    }
}
