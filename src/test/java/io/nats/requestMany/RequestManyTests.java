package io.nats.requestMany;

import io.nats.client.*;
import io.nats.client.impl.NatsMessage;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.nats.client.impl.NatsMessageTests.createStatusMessage;
import static io.nats.client.support.NatsConstants.NANOS_PER_MILLI;
import static io.nats.requestMany.RequestMany.DEFAULT_SENTINEL_STRATEGY_TOTAL_WAIT;
import static org.junit.jupiter.api.Assertions.*;

public class RequestManyTests extends TestBase {
    private static final long MAX_MILLIS = Long.MAX_VALUE / NANOS_PER_MILLI; // copied b/c I didn't want it public in the code

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

    private static RmHandlerAndResult _request(RequestMany rm, String subject) {
        RmHandlerAndResult handler = new RmHandlerAndResult();
        long start = System.currentTimeMillis();
        rm.request(subject, null, handler);
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

    private static Result _queue(RequestMany rm, String subject) throws InterruptedException {
        LinkedBlockingQueue<RmMessage> it = rm.queue(subject, null);
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
    public void testNoRespondersRequest() throws Exception {
        RequestMany rm = builder().build();
        RmHandlerAndResult result = _request(rm, subject());
        assertMessages(0, Last.Status, result.list);
        assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);
        assertTrue(result.eodReceived.await(SHORT_CIRCUIT_TIME, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testNoRespondersFetch() throws Exception {
        RequestMany rm = builder().build();
        Result result = _fetch(rm, subject());
        assertMessages(0, Last.Status, result.list);
        assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);
    }
    
    @Test
    public void testNoRespondersQueue() throws Exception {
        RequestMany rm = builder().build();
        Result result = _queue(rm, subject());
        assertMessages(0, Last.Status, result.list);
        assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);
    }

    @Test
    public void testMaxWaitRequest() throws Exception {
        try (Responder responder = new Responder(1, MAX_WAIT_PAUSE, 1)) {
            RequestMany rm = builder().totalWaitTime(TEST_TWT).build();
            RmHandlerAndResult result = _request(rm, responder.subject);
            assertMessages(1, Last.Normal, result.list);
            assertTrue(result.elapsed >= TEST_TWT);
            assertTrue(result.eodReceived.await(TEST_TWT * 2, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void testMaxWaitFetch() throws Exception {
        try (Responder responder = new Responder(1, MAX_WAIT_PAUSE, 1)) {
            RequestMany rm = builder().totalWaitTime(TEST_TWT).build();
            Result result = _fetch(rm, responder.subject);
            assertMessages(1, Last.None, result.list);
            assertTrue(result.elapsed >= TEST_TWT);
        }
    }

    @Test
    public void testMaxWaitQueue() throws Exception {
        try (Responder responder = new Responder(1, MAX_WAIT_PAUSE, 1)) {
            RequestMany rm = builder().totalWaitTime(TEST_TWT).build();
            Result result = _queue(rm, responder.subject);
            assertMessages(1, Last.Normal, result.list);
            assertTrue(result.elapsed >= TEST_TWT);
        }
    }

    @Test
    public void testStallRequest() throws Exception {
        try (Responder responder = new Responder(1, STALL_PAUSE, 1)) {
            RequestMany rm = builder().stallTime(STALL_WAIT).build();
            RmHandlerAndResult result = _request(rm, responder.subject);
            assertMessages(1, Last.Normal, result.list);
            assertTrue(result.elapsed <= DEFAULT_TIMEOUT);
            assertTrue(result.eodReceived.await(STALL_WAIT * 2, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void testStallFetch() throws Exception {
        try (Responder responder = new Responder(1, STALL_PAUSE, 1)) {
            RequestMany rm = builder().stallTime(STALL_WAIT).build();
            Result result = _fetch(rm, responder.subject);
            assertMessages(1, Last.None, result.list);
            assertTrue(result.elapsed <= DEFAULT_TIMEOUT);

            rm = RequestMany.stall(NC);
            result = _fetch(rm, responder.subject);
            assertMessages(1, Last.None, result.list);
            assertTrue(result.elapsed <= DEFAULT_TIMEOUT);
        }
    }

    @Test
    public void testStallQueue() throws Exception {
        try (Responder responder = new Responder(1, STALL_PAUSE, 1)) {
            RequestMany rm = builder().stallTime(STALL_WAIT).build();
            Result result = _queue(rm, responder.subject);
            assertMessages(1, Last.Normal, result.list);
            assertTrue(result.elapsed <= DEFAULT_TIMEOUT);

            rm = RequestMany.stall(NC);
            result = _queue(rm, responder.subject);
            assertMessages(1, Last.Normal, result.list);
            assertTrue(result.elapsed <= DEFAULT_TIMEOUT);
        }
    }

    @Test
    public void testMaxResponsesRequest() throws Exception {
        try (Responder responder = new Responder(MAX_RESPONSES_RESPONDERS)) {
            RequestMany rm = builder().maxResponses(MAX_RESPONSES).build();
            RmHandlerAndResult result = _request(rm, responder.subject);
            assertMessages(MAX_RESPONSES, Last.Normal, result.list);
            assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);
            assertTrue(result.eodReceived.await(SHORT_CIRCUIT_TIME, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testMaxResponsesFetch() throws Exception {
        try (Responder responder = new Responder(MAX_RESPONSES_RESPONDERS)) {
            RequestMany rm = builder().maxResponses(MAX_RESPONSES).build();
            Result result = _fetch(rm, responder.subject);
            assertMessages(MAX_RESPONSES, Last.None, result.list);
            assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);

            rm = RequestMany.maxResponses(NC, MAX_RESPONSES);
            result = _fetch(rm, responder.subject);
            assertMessages(MAX_RESPONSES, Last.None, result.list);
            assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);
        }
    }

    @Test
    public void testMaxResponsesQueue() throws Exception {
        try (Responder responder = new Responder(MAX_RESPONSES_RESPONDERS)) {
            RequestMany rm = builder().maxResponses(MAX_RESPONSES).build();
            Result result = _queue(rm, responder.subject);
            assertMessages(MAX_RESPONSES, Last.Normal, result.list);
            assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);

            rm = RequestMany.maxResponses(NC, MAX_RESPONSES);
            result = _queue(rm, responder.subject);
            assertMessages(MAX_RESPONSES, Last.Normal, result.list);
            assertTrue(result.elapsed < SHORT_CIRCUIT_TIME);
        }
    }

    @Test
    public void testSentinelRequest() throws Exception {
        try (Responder responder = new Responder(2, true)) {
            RequestMany rm = builder().standardSentinel().build();
            List<RmMessage> list = new ArrayList<>();
            rm.request(responder.subject, null, rmm -> {
                list.add(rmm);
                return true;
            });
            assertMessages(2, Last.Normal, list);
        }
    }

    @Test
    public void testSentinelFetch() throws Exception {
        try (Responder responder = new Responder(2, true)) {
            RequestMany rm = builder().standardSentinel().build();
            Result result = _fetch(rm, responder.subject);
            assertMessages(2, Last.None, result.list);
            assertTrue(result.elapsed <= DEFAULT_TIMEOUT);

            rm = RequestMany.standardSentinel(NC);
            result = _fetch(rm, responder.subject);
            assertMessages(2, Last.None, result.list);
            assertTrue(result.elapsed <= DEFAULT_TIMEOUT);
        }
    }

    @Test
    public void testSentinelQueue() throws Exception {
        try (Responder responder = new Responder(2, true)) {
            RequestMany rm = builder().standardSentinel().build();
            Result result = _queue(rm, responder.subject);
            assertMessages(2, Last.Normal, result.list);
            assertTrue(result.elapsed <= DEFAULT_TIMEOUT);

            rm = RequestMany.standardSentinel(NC);
            result = _queue(rm, responder.subject);
            assertMessages(2, Last.Normal, result.list);
            assertTrue(result.elapsed <= DEFAULT_TIMEOUT);
        }
    }

    @Test
    public void testUserSentinel() throws Exception {
        try (Responder responder = new Responder(MAX_RESPONSES_RESPONDERS)) {
            RequestMany rm = builder().build();
            List<RmMessage> list = new ArrayList<>();
            rm.request(responder.subject, null, rmm -> {
                list.add(rmm);
                return list.size() < 2;
            });
            assertMessages(2, Last.None, list);
        }
    }

    @Test
    public void testRequestManyBuilder() {
        // totalWaitTime
        assertBuilder(-1, -1, -1, false, builder().build());
        assertBuilder(1000, -1, -1, false, builder().totalWaitTime(1000).build());
        assertBuilder(MAX_MILLIS, -1, -1, false, builder().totalWaitTime(MAX_MILLIS).build());
        assertBuilder(MAX_MILLIS, -1, -1, false, builder().totalWaitTime(MAX_MILLIS + 1).build());
        assertBuilder(-1, -1, -1, false, builder().totalWaitTime(0).build());
        assertBuilder(-1, -1, -1, false, builder().totalWaitTime(-1).build());

        // wait strategy
        assertBuilder(-1, -1, -1, false, RequestMany.wait(NC));
        assertBuilder(1000, -1, -1, false, RequestMany.wait(NC, 1000));
        assertBuilder(MAX_MILLIS, -1, -1, false, RequestMany.wait(NC, MAX_MILLIS));
        assertBuilder(MAX_MILLIS, -1, -1, false, RequestMany.wait(NC, MAX_MILLIS + 1));
        assertBuilder(-1, -1, -1, false, RequestMany.wait(NC, 0));
        assertBuilder(-1, -1, -1, false, RequestMany.wait(NC, -1));

        // stallTime
        assertBuilder(-1, 1, -1, false, builder().stallTime(1).build());
        assertBuilder(-1, MAX_MILLIS, -1, false, builder().stallTime(MAX_MILLIS).build());
        assertBuilder(-1, MAX_MILLIS, -1, false, builder().stallTime(MAX_MILLIS + 1).build());
        assertBuilder(-1, -1, -1, false, builder().stallTime(0).build());
        assertBuilder(-1, -1, -1, false, builder().stallTime(-1).build());

        // stall strategy
        assertBuilder(-1, DEFAULT_TIMEOUT / 10, -1, false, RequestMany.stall(NC));
        assertBuilder(MAX_MILLIS, MAX_MILLIS / 10, -1, false, RequestMany.stall(NC, MAX_MILLIS));
        assertBuilder(MAX_MILLIS, MAX_MILLIS / 10, -1, false, RequestMany.stall(NC, MAX_MILLIS + 1));
        assertBuilder(-1, DEFAULT_TIMEOUT / 10, -1, false, RequestMany.stall(NC, 0));
        assertBuilder(-1, DEFAULT_TIMEOUT / 10, -1, false, RequestMany.stall(NC, -1));

        // maxResponse
        assertBuilder(-1, -1, 1, false, builder().maxResponses(1).build());
        assertBuilder(-1, -1, Long.MAX_VALUE, false, builder().maxResponses(Long.MAX_VALUE).build());
        assertBuilder(-1, -1, -1, false, builder().maxResponses(0).build());
        assertBuilder(-1, -1, -1, false, builder().maxResponses(-1).build());

        // maxResponse strategy
        assertBuilder(-1, -1, 1, false, RequestMany.maxResponses(NC, 1));
        assertBuilder(-1, -1, Long.MAX_VALUE, false, RequestMany.maxResponses(NC, Long.MAX_VALUE));
        assertBuilder(-1, -1, -1, false, RequestMany.maxResponses(NC, 0));
        assertBuilder(-1, -1, -1, false, RequestMany.maxResponses(NC, -1));

        // standardSentinel
        assertBuilder(-1, -1, -1, true, builder().standardSentinel().build());

        // standardSentinel strategy
        assertBuilder(DEFAULT_SENTINEL_STRATEGY_TOTAL_WAIT, DEFAULT_TIMEOUT, -1, true, RequestMany.standardSentinel(NC));
        assertBuilder(MAX_MILLIS, DEFAULT_TIMEOUT, -1, true, RequestMany.standardSentinel(NC, MAX_MILLIS));
    }

    private void assertBuilder(long exTo, long exStall, long exResp, boolean stdSentinel, RequestMany rm) {
        assertEquals(exTo == -1 ? Options.DEFAULT_CONNECTION_TIMEOUT.toMillis() : exTo, rm.getTotalWaitTime());
        assertEquals(exStall, rm.getStallTime());
        assertEquals(exResp, rm.getMaxResponses());
        String s = rm.toString();
        if (exStall == -1) {
            assertTrue(s.contains("<no stall>"));
        }
        else {
            assertTrue(s.contains("maxStall"));
        }
        if (exResp == -1) {
            assertTrue(s.contains("<no max>"));
        }
        else {
            assertTrue(s.contains("maxResponses"));
        }
        assertEquals(stdSentinel, rm.isStandardSentinel());
    }


    @Test
    public void testRmMessageConstruction() {
        Message m = NatsMessage.builder().subject("foo").build();
        RmMessage rmm = new RmMessage(m);
        assertTrue(rmm.isDataMessage());
        assertFalse(rmm.isStatusMessage());
        assertFalse(rmm.isException());
        assertFalse(rmm.isEndOfData());
        assertFalse(rmm.isNormalEndOfData());
        assertFalse(rmm.isAbnormalEndOfData());
        assertTrue(rmm.toString().contains("Data Message"));
        assertTrue(rmm.toString().contains("Empty Payload"));
        assertNotNull(rmm.getMessage());
        assertNull(rmm.getStatusMessage());
        assertNull(rmm.getException());

        m = NatsMessage.builder().subject("foo").data(new byte[1]).build();
        rmm = new RmMessage(m);
        assertTrue(rmm.isDataMessage());
        assertFalse(rmm.isStatusMessage());
        assertFalse(rmm.isException());
        assertFalse(rmm.isEndOfData());
        assertFalse(rmm.isNormalEndOfData());
        assertFalse(rmm.isAbnormalEndOfData());
        assertTrue(rmm.toString().contains("Data Message"));
        assertFalse(rmm.toString().contains("Empty Payload"));
        assertNotNull(rmm.getMessage());
        assertNull(rmm.getStatusMessage());
        assertNull(rmm.getException());

        m = createStatusMessage();
        rmm = new RmMessage(m);
        assertFalse(rmm.isDataMessage());
        assertTrue(rmm.isStatusMessage());
        assertFalse(rmm.isException());
        assertTrue(rmm.isEndOfData());
        assertFalse(rmm.isNormalEndOfData());
        assertTrue(rmm.isAbnormalEndOfData());
        assertTrue(rmm.toString().contains("Abnormal"));
        assertNotNull(rmm.getMessage());
        assertNotNull(rmm.getStatusMessage());
        assertNull(rmm.getException());

        rmm = new RmMessage(new Exception());
        assertFalse(rmm.isDataMessage());
        assertFalse(rmm.isStatusMessage());
        assertTrue(rmm.isException());
        assertTrue(rmm.isEndOfData());
        assertFalse(rmm.isNormalEndOfData());
        assertTrue(rmm.isAbnormalEndOfData());
        assertTrue(rmm.toString().contains("Abnormal"));
        assertNull(rmm.getMessage());
        assertNull(rmm.getStatusMessage());
        assertNotNull(rmm.getException());

        rmm = new RmMessage((Message)null);
        assertFalse(rmm.isDataMessage());
        assertFalse(rmm.isStatusMessage());
        assertFalse(rmm.isException());
        assertTrue(rmm.isEndOfData());
        assertTrue(rmm.isNormalEndOfData());
        assertFalse(rmm.isAbnormalEndOfData());
        assertTrue(rmm.toString().contains("Normal"));
        assertNull(rmm.getMessage());
        assertNull(rmm.getStatusMessage());
        assertNull(rmm.getException());
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
        public boolean handle(RmMessage rmm) {
            list.add(rmm);
            if (rmm.isEndOfData()) {
                eodReceived.countDown();
            }
            return true;
        }
    }

    static class Responder implements AutoCloseable {
        final Dispatcher dispatcher;
        final boolean sentinel;
        public final String subject;

        public Responder(final int count) {
            this(count, -1, -1, false);
        }

        public Responder(final int count, boolean sentinel) {
            this(count, -1, -1, sentinel);
        }

        public Responder(final int count, final long pause, final int count2) {
            this(count, pause, count2, false);
        }

        public Responder(final int count, final long pause, final int count2, boolean sentinel) {
            this.subject = NUID.nextGlobalSequence();
            this.sentinel = sentinel;

            dispatcher = NC.createDispatcher(m -> {
                for (int x = 0; x < count; x++) {
                    NC.publish(m.getReplyTo(), ("" + x).getBytes());
                }
                if (pause > 0) {
                    sleep(pause);
                    for (int x = 0; x < count2; x++) {
                        NC.publish(m.getReplyTo(), ("" + x).getBytes());
                    }
                }
                if (sentinel) {
                    NC.publish(m.getReplyTo(), null);
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
