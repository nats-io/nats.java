// Copyright 2021 The NATS Authors
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

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.support.IncomingHeadersProcessor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.client.support.NatsJetStreamConstants.CONSUMER_STALLED_HDR;
import static io.nats.client.support.Status.*;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("SameParameterValue")
public class MessageManagerTests extends JetStreamTestBase {

    @Test
    public void testConstruction() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = genericSub(nc);

            _pushConstruction(nc, true, true, push_hb_fc(), sub);
            _pushConstruction(nc, true, false, push_hb_xfc(), sub);

            _pushConstruction(nc, false, false, push_xhb_xfc(), sub);
        });
    }

    private void _pushConstruction(Connection conn, boolean hb, boolean fc, SubscribeOptions so, NatsJetStreamSubscription sub) {
        PushMessageManager manager = getManager(conn, so, sub, true, false);
        assertTrue(manager.isSyncMode());
        assertFalse(manager.isQueueMode());
        assertEquals(hb, manager.isHb());
        assertEquals(fc, manager.isFc());

        manager = getManager(conn, so, sub, true, true);
        assertTrue(manager.isSyncMode());
        assertTrue(manager.isQueueMode());
        assertFalse(manager.isHb());
        assertFalse(manager.isFc());
    }

    @Test
    public void testPushManagerBeforeQueueProcessorAndManage() throws Exception {
        TestHandler handler = new TestHandler();
        Options.Builder ob = new Options.Builder().errorListener(handler);
        runInJsServer(ob, nc -> {
            NatsJetStreamSubscription sub = genericSub(nc);

            PushMessageManager manager = getManager(nc, push_hb_fc(), sub, true, false);
            coverBqpAndManage(sub, handler, manager);

            handler.reset();
            manager = getManager(nc, push_hb_xfc(), sub, true, false);
            coverBqpAndManage(sub, handler, manager);

            handler.reset();
            manager = getManager(nc, push_xhb_xfc(), sub, true, false);
            coverBqpAndManage(sub, handler, manager);

            handler.reset();
            manager = getManager(nc, push_hb_fc(), sub, false, false);
            coverBqpAndManage(sub, handler, manager);

            handler.reset();
            manager = getManager(nc, push_hb_xfc(), sub, false, false);
            coverBqpAndManage(sub, handler, manager);

            handler.reset();
            manager = getManager(nc, push_xhb_xfc(), sub, false, false);
            coverBqpAndManage(sub, handler, manager);
        });
    }

    private void coverBqpAndManage(NatsJetStreamSubscription sub, TestHandler handler, PushMessageManager manager) {
        if (handler != null) {
            handler.reset();
        }

        String sid = sub.getSID();
        assertFalse(manager.beforeQueueProcessorImpl(getFlowControl(1, sid)));
        assertFalse(manager.beforeQueueProcessorImpl(getUnkStatus(sid)));
        assertTrue(manager.beforeQueueProcessorImpl(getFcHeartbeat(1, sid)));
        assertTrue(manager.beforeQueueProcessorImpl(getTestJsMessage(1, sid)));

        assertFalse(manager.manage(getTestJsMessage(1, sid)));
        assertTrue(manager.manage(getFlowControl(1, sid)));
        assertTrue(manager.manage(getFcHeartbeat(1, sid)));

        if (handler != null) {
            sleep(100);
            TestHandler.UnhandledStatusEvent use = handler.getUnhandledStatuses().get(0);
            assertSame(sub.getSID(), use.sid);
            assertEquals(100, use.status.getCode());
            assertTrue(use.status.getMessage().contains("FlowControl"));
            use = handler.getUnhandledStatuses().get(1);
            assertSame(sub.getSID(), use.sid);
            assertEquals(999, use.status.getCode());
            assertTrue(use.status.getMessage().contains("blah"));
        }
    }

    @Test
    public void test_status_handle_pull() throws Exception {
        TestHandler handler = new TestHandler();
        Options.Builder ob = new Options.Builder().errorListener(handler);
        runInJsServer(ob, nc -> {
            NatsJetStreamSubscription sub = genericSub(nc);
            PullMessageManager manager = new PullMessageManager((NatsConnection)nc, null);
            manager.startup(sub);
            String sid = sub.getSID();
            assertFalse(manager.manage(getTestJsMessage(1, sid)));
            assertTrue(manager.manage(get404(sid)));
            assertTrue(manager.manage(get408(sid)));
            // TODO SFF
//        _status_handle_throws(sub, manager, get404(sid));
//        _status_handle_throws(sub, manager, get408(sid));
//        _status_handle_throws(sub, manager, getUnkStatus(sid));
        });
    }

    @Test
    public void test_push_fc() {
        SubscribeOptions so = push_hb_fc();
        MockPublishInternal mpi = new MockPublishInternal();
        NatsDispatcher natsDispatcher = new NatsDispatcher(null, null);
        PushMessageManager pmm = new PushMessageManager(mpi, null, null, so, so.getConsumerConfiguration(), false, natsDispatcher);
        NatsJetStreamSubscription sub = mockSub(mpi, pmm);
        String sid = sub.getSID();
        pmm.startup(sub);

        assertNull(pmm.getLastFcSubject());
        pmm.manage(getFlowControl(1, sid));
        assertEquals(getFcSubject(1), pmm.getLastFcSubject());
        assertEquals(getFcSubject(1), mpi.fcSubject);
        assertEquals(1, mpi.pubCount);

        pmm.manage(getFlowControl(1, sid)); // duplicate should not call publish
        assertEquals(getFcSubject(1), pmm.getLastFcSubject());
        assertEquals(getFcSubject(1), mpi.fcSubject);
        assertEquals(1, mpi.pubCount);

        pmm.manage(getFlowControl(2, sid)); // duplicate should not call publish
        assertEquals(getFcSubject(2), pmm.getLastFcSubject());
        assertEquals(getFcSubject(2), mpi.fcSubject);
        assertEquals(2, mpi.pubCount);

        pmm.manage(getFcHeartbeat(2, sid)); // duplicate should not call publish
        assertEquals(getFcSubject(2), pmm.getLastFcSubject());
        assertEquals(getFcSubject(2), mpi.fcSubject);
        assertEquals(2, mpi.pubCount);

        pmm.manage(getFcHeartbeat(3, sid));
        assertEquals(getFcSubject(3), pmm.getLastFcSubject());
        assertEquals(getFcSubject(3), mpi.fcSubject);
        assertEquals(3, mpi.pubCount);

        pmm.manage(getHeartbeat(sid));
        assertEquals(getFcSubject(3), pmm.getLastFcSubject());
        assertEquals(getFcSubject(3), mpi.fcSubject);
        assertEquals(3, mpi.pubCount);

        // coverage sequences
        pmm.manage(getTestJsMessage(1, sid));
        assertEquals(1, pmm.getLastStreamSequence());
        assertEquals(1, pmm.getInternalConsumerSequence());

        pmm.manage(getTestJsMessage(2, sid));
        assertEquals(2, pmm.getLastStreamSequence());
        assertEquals(2, pmm.getInternalConsumerSequence());

        coverBqpAndManage(sub, null, pmm);

        // coverage extractFcSubject
        assertNull(pmm.extractFcSubject(getTestJsMessage(4, sid)));
        assertNull(pmm.extractFcSubject(getHeartbeat(sid)));
        assertNotNull(pmm.extractFcSubject(getFcHeartbeat(9, sid)));
    }

    @Test
    public void test_push_xfc() {
        _push_xfc(push_hb_xfc());
        _push_xfc(push_xhb_xfc());
    }

    private void _push_xfc(SubscribeOptions so) {
        MockPublishInternal mpi = new MockPublishInternal();
        PushMessageManager pmm = new PushMessageManager(mpi, null, null, so, so.getConsumerConfiguration(), false, new NatsDispatcher(null, null));
        NatsJetStreamSubscription sub = mockSub(mpi, pmm);
        String sid = sub.getSID();
        pmm.startup(sub);
        assertNull(pmm.getLastFcSubject());

        pmm.manage(getFlowControl(1, sid));
        assertNull(pmm.getLastFcSubject());
        assertNull(mpi.fcSubject);
        assertEquals(0, mpi.pubCount);

        pmm.manage(getHeartbeat(sid));
        assertNull(pmm.getLastFcSubject());
        assertNull(mpi.fcSubject);
        assertEquals(0, mpi.pubCount);

        // coverage sequences
        pmm.manage(getTestJsMessage(1, sid));
        assertEquals(1, pmm.getLastStreamSequence());
        assertEquals(1, pmm.getInternalConsumerSequence());

        pmm.manage(getTestJsMessage(2, sid));
        assertEquals(2, pmm.getLastStreamSequence());
        assertEquals(2, pmm.getInternalConsumerSequence());

        // coverage beforeQueueProcessor
        assertFalse(pmm.beforeQueueProcessorImpl(getFlowControl(1, sid)));
        assertFalse(pmm.beforeQueueProcessorImpl(getUnkStatus(sid)));
        assertTrue(pmm.beforeQueueProcessorImpl(getFcHeartbeat(1, sid)));
        assertTrue(pmm.beforeQueueProcessorImpl(getTestJsMessage(1, sid)));

        // coverage manager
        assertFalse(pmm.manage(getTestJsMessage(1, sid)));
        assertTrue(pmm.manage(getFlowControl(1, sid)));
        assertTrue(pmm.manage(getFcHeartbeat(1, sid)));

        // coverage beforeQueueProcessor
//        assertTrue(pmm.beforeQueueProcessorImpl(getTestJsMessage(3, sid)));
//        assertTrue(pmm.beforeQueueProcessorImpl(get408(sid)));
//        assertTrue(pmm.beforeQueueProcessorImpl(getFcHeartbeat(9, sid)));
//        assertFalse(pmm.beforeQueueProcessorImpl(getHeartbeat(sid)));

        // coverage extractFcSubject
        assertNull(pmm.extractFcSubject(getTestJsMessage()));
        assertNull(pmm.extractFcSubject(getHeartbeat(sid)));
        assertNotNull(pmm.extractFcSubject(getFcHeartbeat(9, sid)));
    }

    @Test
    public void test_received_time() throws Exception {
        runInJsServer(nc -> {
            createDefaultTestStream(nc);
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            _received_time_yes(push_hb_fc(), js);
            _received_time_yes(push_hb_xfc(), js);
            _received_time_no(js, jsm, js.subscribe(SUBJECT, push_xhb_xfc()));
        });
    }

    private void _received_time_yes(PushSubscribeOptions so, JetStream js) throws Exception {
        long before = System.currentTimeMillis();
        NatsJetStreamSubscription sub = (NatsJetStreamSubscription) js.subscribe(SUBJECT, so);

        // during the sleep, the heartbeat is delivered and is checked
        // by the heartbeat listener and recorded as received
        sleep(1050); // slightly longer than the idle heartbeat

        long preTime = findStatusManager(sub).getLastMsgReceived();
        assertTrue(preTime > before);
        sub.unsubscribe();
    }

    PushMessageManager findStatusManager(NatsJetStreamSubscription sub) {
        MessageManager mm = sub.getManager();
        if (mm instanceof PushMessageManager) {
            return (PushMessageManager)mm;
        }
        return null;
    };

    private void _received_time_no(JetStream js, JetStreamManagement jsm, JetStreamSubscription sub) throws IOException, JetStreamApiException, InterruptedException {
        js.publish(SUBJECT, dataBytes(0));
        sub.nextMessage(1000);
        NatsJetStreamSubscription nsub = (NatsJetStreamSubscription)sub;
        assertTrue(findStatusManager(nsub).getLastMsgReceived() <= System.currentTimeMillis());
        jsm.purgeStream(STREAM);
        sub.unsubscribe();
    }

    @Test
    public void test_hb_yes_settings() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = genericSub(nc);

            ConsumerConfiguration cc = ConsumerConfiguration.builder().idleHeartbeat(1000).build();

            // MessageAlarmTime default
            PushSubscribeOptions so = new PushSubscribeOptions.Builder().configuration(cc).build();
            PushMessageManager manager = getManager(nc, so, sub);
            assertEquals(1000, manager.getIdleHeartbeatSetting());
            assertEquals(3000, manager.getAlarmPeriodSetting());

            // MessageAlarmTime < idleHeartbeat
            so = new PushSubscribeOptions.Builder().configuration(cc).messageAlarmTime(999).build();
            manager = getManager(nc, so, sub);
            assertEquals(1000, manager.getIdleHeartbeatSetting());
            assertEquals(3000, manager.getAlarmPeriodSetting());

            // MessageAlarmTime == idleHeartbeat
            so = new PushSubscribeOptions.Builder().configuration(cc).messageAlarmTime(1000).build();
            manager = getManager(nc, so, sub);
            assertEquals(1000, manager.getIdleHeartbeatSetting());
            assertEquals(1000, manager.getAlarmPeriodSetting());

            // MessageAlarmTime > idleHeartbeat
            so = new PushSubscribeOptions.Builder().configuration(cc).messageAlarmTime(2000).build();
            manager = getManager(nc, so, sub);
            assertEquals(1000, manager.getIdleHeartbeatSetting());
            assertEquals(2000, manager.getAlarmPeriodSetting());
        });
    }

    @Test
    public void test_hb_no_settings() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = genericSub(nc);
            SubscribeOptions so = push_xhb_xfc();
            PushMessageManager manager = getManager(nc, so, sub);
            assertEquals(0, manager.getIdleHeartbeatSetting());
            assertEquals(0, manager.getAlarmPeriodSetting());
        });
    }

    private ConsumerConfiguration cc_fc_hb() {
        return ConsumerConfiguration.builder().flowControl(1000).build();
    }

    private ConsumerConfiguration cc_xfc_hb() {
        return ConsumerConfiguration.builder().idleHeartbeat(1000).build();
    }

    private ConsumerConfiguration cc_xfc_xhb() {
        return ConsumerConfiguration.builder().build();
    }

    private PushSubscribeOptions push_hb_fc() {
        return new PushSubscribeOptions.Builder().configuration(cc_fc_hb()).build();
    }

    private PushSubscribeOptions push_hb_xfc() {
        return new PushSubscribeOptions.Builder().configuration(cc_xfc_hb()).build();
    }

    private PushSubscribeOptions push_xhb_xfc() {
        return new PushSubscribeOptions.Builder().configuration(cc_xfc_xhb()).build();
    }

    private PushMessageManager getManager(Connection conn, SubscribeOptions so, NatsJetStreamSubscription sub) {
        return getManager(conn, so, sub, true, false);
    }

    private PushMessageManager getManager(Connection conn, SubscribeOptions so, NatsJetStreamSubscription sub, boolean syncMode, boolean queueMode) {
        NatsDispatcher natsDispatcher = syncMode ? null : new NatsDispatcher(null, null);
        PushMessageManager asm = new PushMessageManager((NatsConnection)conn, null, null, so, so.getConsumerConfiguration(), queueMode, natsDispatcher);
        asm.startup(sub);
        return asm;
    }

    private NatsMessage getFlowControl(int replyToId, String sid) {
        IncomingMessageFactory imf = new IncomingMessageFactory(sid, "subj", getFcSubject(replyToId), 0, false);
        imf.setHeaders(new IncomingHeadersProcessor(("NATS/1.0 " + FLOW_OR_HEARTBEAT_STATUS_CODE + " " + FLOW_CONTROL_TEXT + "\r\n").getBytes()));
        return imf.getMessage();
    }

    private String getFcSubject(int id) {
        return "fcSubject." + id;
    }

    private NatsMessage getFcHeartbeat(int replyToId, String sid) {
        IncomingMessageFactory imf = new IncomingMessageFactory(sid, "subj", null, 0, false);
        String s = "NATS/1.0 " + FLOW_OR_HEARTBEAT_STATUS_CODE + " " + HEARTBEAT_TEXT + "\r\n" + CONSUMER_STALLED_HDR + ":" + getFcSubject(replyToId) + "\r\n\r\n";
        imf.setHeaders(new IncomingHeadersProcessor(s.getBytes()));
        return imf.getMessage();
    }

    private NatsMessage getHeartbeat(String sid) {
        IncomingMessageFactory imf = new IncomingMessageFactory(mockSid(), "subj", null, 0, false);
        String s = "NATS/1.0 " + FLOW_OR_HEARTBEAT_STATUS_CODE + " " + HEARTBEAT_TEXT + "\r\n";
        imf.setHeaders(new IncomingHeadersProcessor(s.getBytes()));
        return imf.getMessage();
    }

    private NatsMessage get404(String sid) {
        return getStatus(404, "not found", sid);
    }

    private NatsMessage get408(String sid) {
        return getStatus(408, "expired", sid);
    }

    private NatsMessage getUnkStatus(String sid) {
        return getStatus(999, "blah blah", sid);
    }

    private NatsMessage getStatus(int code, String message, String sid) {
        IncomingMessageFactory imf = new IncomingMessageFactory(sid, "subj", null, 0, false);
        imf.setHeaders(new IncomingHeadersProcessor(("NATS/1.0 " + code + " " + message + "\r\n").getBytes()));
        return imf.getMessage();
    }

    static class MockPublishInternal extends NatsConnection {
        int pubCount;
        String fcSubject;

        public MockPublishInternal() {
            this(new Options.Builder().build());
        }

        public MockPublishInternal(Options options) {
            super(options);
        }

        @Override
        void publishInternal(String subject, String replyTo, Headers headers, byte[] data) {
            fcSubject = subject;
            ++pubCount;
        }
    }

    static AtomicInteger ID = new AtomicInteger();
    private static NatsJetStreamSubscription genericSub(Connection nc) throws IOException, JetStreamApiException {
        String id = "-" + ID.incrementAndGet() + "-" + System.currentTimeMillis();
        String stream = STREAM + id;
        String subject = STREAM + id;
        createMemoryStream(nc, stream, subject);
        JetStream js = nc.jetStream();
        return (NatsJetStreamSubscription) js.subscribe(subject);
    }

    private static NatsJetStreamSubscription mockSub(NatsConnection connection, MessageManager manager) {
        return new NatsJetStreamSubscription(mockSid(), null, null,
            connection, null /* dispatcher */,
            null /* js */,
            null, null, manager);
    }

    static class TestMessageManager extends MessageManager {
        public TestMessageManager() {
            super(null, null);
        }

        @Override
        protected boolean manage(Message msg) {
            return false;
        }

        @Override
        protected void startup(NatsJetStreamSubscription sub) {
            this.sub = sub;
        }

        @Override
        protected void shutdown() {}

        NatsJetStreamSubscription getSub() { return sub; }
    }

    @Test
    public void testMessageManagerInterfaceDefaultImplCoverage() {
        TestMessageManager tmm = new TestMessageManager();
        NatsJetStreamSubscription sub =
            new NatsJetStreamSubscription(mockSid(), "sub", null, null, null, null, "stream", "con", tmm);
        tmm.startup(sub);
        assertSame(sub, tmm.getSub());
    }
}
