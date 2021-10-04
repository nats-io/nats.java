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
import io.nats.client.support.Status;
import org.junit.jupiter.api.Test;

import static io.nats.client.support.NatsJetStreamConstants.CONSUMER_STALLED_HDR;
import static io.nats.client.support.Status.*;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("SameParameterValue")
public class AutoStatusManagerTests extends JetStreamTestBase {

    @Test
    public void testConstruction() {
        _construction(true, true,  false, false, true,  pull_asm_gap());
        _construction(true, true,  false, false, false, pull_asm_xgap());
        _construction(true, false, false, false, true,  pull_xasm_gap());
        _construction(true, false, false, false, false, pull_xasm_xgap_noop());

        _construction(false, true, true, true,  true,  push_asm_hb_fc_gap());
        _construction(false, true, true, true,  false, push_asm_hb_fc_xgap());
        _construction(false, true, true, false, true,  push_asm_hb_xfc_gap());
        _construction(false, true, true, false, false, push_asm_hb_xfc_xgap());

        _construction(false, true, false, false, true,  push_asm_xhb_xfc_gap());
        _construction(false, true, false, false, false, push_asm_xhb_xfc_xgap());

        _construction(false, false, false, false, true,  push_xasm_gap());
        _construction(false, false, false, false, false, push_xasm_xgap_noop());
    }

    private void _construction(boolean pull, boolean asm, boolean hb, boolean fc, boolean gap, SubscribeOptions so) {
        AutoStatusManager manager = getManager(null, so, null, true, false);
        assertTrue(manager.isSyncMode());
        assertFalse(manager.isQueueMode());
        assertEquals(pull, manager.isPull());
        assertEquals(asm, manager.isAsm());
        assertEquals(hb, manager.isHb());
        assertEquals(fc, manager.isFc());
        assertEquals(gap, manager.isGap());
        assertEquals(!(asm || hb || fc || gap), manager.isNoOp());

        // queue mode
        if (!pull) {
            manager = getManager(null, so, null, true, true);
            assertTrue(manager.isSyncMode());
            assertTrue(manager.isQueueMode());
            assertEquals(pull, manager.isPull());
            assertEquals(asm, manager.isAsm());
            assertFalse(manager.isHb());
            assertFalse(manager.isFc());
            assertFalse(manager.isGap());
            assertEquals(!asm, manager.isNoOp());
        }
    }

    @Test
    public void test_status_pass_through() {
        _status_pass_through(pull_xasm_gap());
        _status_pass_through(pull_xasm_xgap_noop());
        _status_pass_through(push_xasm_gap());
        _status_pass_through(push_xasm_xgap_noop());
    }

    private void _status_pass_through(SubscribeOptions so) {
        AutoStatusManager manager = getManager(so);
        assertFalse(manager.preProcess(getTestJsMessage(1)));
        assertFalse(manager.preProcess(get404()));
        assertFalse(manager.preProcess(get408()));
        assertFalse(manager.preProcess(getFlowControl(1)));
        assertFalse(manager.preProcess(getFcHeartbeat(1)));
        assertFalse(manager.preProcess(getUnkStatus()));
    }

    @Test
    public void test_status_handle_pull() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();
            NatsJetStreamSubscription sub = (NatsJetStreamSubscription) js.subscribe(SUBJECT);
            _status_handle_pull(nc, sub, pull_asm_gap());
            _status_handle_pull(nc, sub, pull_asm_xgap());
        });
    }

    private void _status_handle_pull(Connection conn, NatsJetStreamSubscription sub, SubscribeOptions so) {
        AutoStatusManager manager = getManager((NatsConnection) conn, so, sub, true, false);
        assertFalse(manager.preProcess(getTestJsMessage(1)));
        assertTrue(manager.preProcess(get404()));
        assertTrue(manager.preProcess(get408()));
        _status_handle_throws(sub, manager, getFlowControl(1));
        _status_handle_throws(sub, manager, getFcHeartbeat(1));
        _status_handle_throws(sub, manager, getUnkStatus());
    }

    @Test
    public void test_status_handle_pushSync() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();
            NatsJetStreamSubscription sub = (NatsJetStreamSubscription) js.subscribe(SUBJECT);
            _status_handle_pushSync(nc, sub, push_asm_hb_fc_gap());
            _status_handle_pushSync(nc, sub, push_asm_hb_fc_xgap());
            _status_handle_pushSync(nc, sub, push_asm_hb_xfc_gap());
            _status_handle_pushSync(nc, sub, push_asm_hb_xfc_xgap());
            _status_handle_pushSync(nc, sub, push_asm_xhb_xfc_gap());
            _status_handle_pushSync(nc, sub, push_asm_xhb_xfc_xgap());
        });
    }

    private void _status_handle_pushSync(Connection conn, NatsJetStreamSubscription sub, SubscribeOptions so) {
        AutoStatusManager manager = getManager((NatsConnection) conn, so, sub, true, false);
        assertFalse(manager.preProcess(getTestJsMessage(1)));
        assertTrue(manager.preProcess(getFlowControl(1)));
        assertTrue(manager.preProcess(getFcHeartbeat(1)));
        _status_handle_throws(sub, manager, get404());
        _status_handle_throws(sub, manager, get408());
        _status_handle_throws(sub, manager, getUnkStatus());
    }

    private void _status_handle_throws(NatsJetStreamSubscription sub, AutoStatusManager pre, Message m) {
        JetStreamStatusException jsse = assertThrows(JetStreamStatusException.class, () -> pre.preProcess(m));
        assertSame(sub, jsse.getSubscription());
        assertSame(m.getStatus(), jsse.getStatus());
    }

    @Test
    public void test_status_handle_pushAsync() throws Exception {
        PreEl el = new PreEl();
        runInJsServer(new Options.Builder().errorListener(el), nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();
            NatsJetStreamSubscription sub = (NatsJetStreamSubscription) js.subscribe(SUBJECT);
            _status_handle_pushAsync(el, nc, sub, push_asm_hb_fc_gap());
            _status_handle_pushAsync(el, nc, sub, push_asm_hb_fc_xgap());
            _status_handle_pushAsync(el, nc, sub, push_asm_hb_xfc_gap());
            _status_handle_pushAsync(el, nc, sub, push_asm_hb_xfc_xgap());
            _status_handle_pushAsync(el, nc, sub, push_asm_xhb_xfc_gap());
            _status_handle_pushAsync(el, nc, sub, push_asm_xhb_xfc_xgap());
        });

        // no error listener for coverage
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();
            NatsJetStreamSubscription sub = (NatsJetStreamSubscription) js.subscribe(SUBJECT);
            _status_handle_pushAsync(null, nc, sub, push_asm_hb_fc_gap());
            _status_handle_pushAsync(null, nc, sub, push_asm_hb_fc_xgap());
            _status_handle_pushAsync(null, nc, sub, push_asm_hb_xfc_gap());
            _status_handle_pushAsync(null, nc, sub, push_asm_hb_xfc_xgap());
            _status_handle_pushAsync(null, nc, sub, push_asm_xhb_xfc_gap());
            _status_handle_pushAsync(null, nc, sub, push_asm_xhb_xfc_xgap());
        });
    }

    private void _status_handle_pushAsync(PreEl el, Connection conn, NatsJetStreamSubscription sub, SubscribeOptions so) {
        AutoStatusManager manager = getManager((NatsConnection) conn, so, sub, false, false);
        if (el != null) {
            el.reset();
        }
        assertFalse(manager.preProcess(getTestJsMessage(1)));
        assertTrue(manager.preProcess(getFlowControl(1)));
        assertTrue(manager.preProcess(getFcHeartbeat(1)));

        Message m = get404();
        assertTrue(manager.preProcess(m));
        if (el != null) {
            assertSame(sub, el.sub);
            assertSame(m.getStatus(), el.status);
        }

        m = get408();
        assertTrue(manager.preProcess(m));
        if (el != null) {
            assertSame(sub, el.sub);
            assertSame(m.getStatus(), el.status);
        }

        m = getUnkStatus();
        assertTrue(manager.preProcess(m));
        if (el != null) {
            assertSame(sub, el.sub);
            assertSame(m.getStatus(), el.status);
        }
    }

    @Test
    public void test_gap_pull_pushSync() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();
            NatsJetStreamSubscription sub = (NatsJetStreamSubscription) js.subscribe(SUBJECT);
            _gap_pull_pushSync(nc, sub, pull_asm_gap());
            _gap_pull_pushSync(nc, sub, push_asm_hb_fc_gap());
            _gap_pull_pushSync(nc, sub, push_asm_hb_xfc_gap());
            _gap_pull_pushSync(nc, sub, push_asm_xhb_xfc_gap());
            _gap_pull_pushSync(nc, sub, pull_xasm_gap());
            _gap_pull_pushSync(nc, sub, push_xasm_gap());
        });
    }

    private void _gap_pull_pushSync(Connection conn, NatsJetStreamSubscription sub, SubscribeOptions so) {
        AutoStatusManager manager = getManager((NatsConnection) conn, so, sub, true, false);
        assertEquals(-1, manager.getExpectedConsumerSequence());
        manager.preProcess(getTestJsMessage(1));
        assertEquals(2, manager.getExpectedConsumerSequence());
        manager.preProcess(getTestJsMessage(2));
        assertEquals(3, manager.getExpectedConsumerSequence());
        JetStreamGapException jsge = assertThrows(JetStreamGapException.class, () -> manager.preProcess(getTestJsMessage(4)));
        assertSame(sub, jsge.getSubscription());
        assertEquals(3, jsge.getExpectedConsumerSeq());
        assertEquals(4, jsge.getReceivedConsumerSeq());
    }

    @Test
    public void test_gap_pushAsync() throws Exception {
        PreEl el = new PreEl();
        runInJsServer(new Options.Builder().errorListener(el), nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();
            NatsJetStreamSubscription sub = (NatsJetStreamSubscription) js.subscribe(SUBJECT);
            _gap_pushAsync(el, nc, sub, push_asm_hb_fc_gap());
            _gap_pushAsync(el, nc, sub, push_asm_hb_xfc_gap());
            _gap_pushAsync(el, nc, sub, push_asm_xhb_xfc_gap());
            _gap_pushAsync(el, nc, sub, push_xasm_gap());
        });

        // no error listener for coverage
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();
            NatsJetStreamSubscription sub = (NatsJetStreamSubscription) js.subscribe(SUBJECT);
            _gap_pushAsync(null, nc, sub, push_asm_hb_fc_gap());
            _gap_pushAsync(null, nc, sub, push_asm_hb_xfc_gap());
            _gap_pushAsync(null, nc, sub, push_asm_xhb_xfc_gap());
            _gap_pushAsync(null, nc, sub, push_xasm_gap());
        });
    }

    private void _gap_pushAsync(PreEl el, Connection conn, NatsJetStreamSubscription sub, SubscribeOptions so) {
        AutoStatusManager manager = getManager((NatsConnection) conn, so, sub, false, false);
        if (el != null) {
            el.reset();
        }
        assertEquals(-1, manager.getExpectedConsumerSequence());
        manager.preProcess(getTestJsMessage(1));
        assertEquals(2, manager.getExpectedConsumerSequence());
        manager.preProcess(getTestJsMessage(2));
        assertEquals(3, manager.getExpectedConsumerSequence());
        manager.preProcess(getTestJsMessage(4));
        if (el != null) {
            assertSame(sub, el.sub);
            assertEquals(3, el.expectedConsumerSeq);
            assertEquals(4, el.receivedConsumerSeq);
        }
    }

    static class MockPublishInternal extends NatsConnection {
        int pubCount;
        String fcSubject;

        public MockPublishInternal(Options options) {
            super(options);
        }

        @Override
        void publishInternal(String subject, String replyTo, Headers headers, byte[] data, boolean utf8mode) {
            fcSubject = subject;
            ++pubCount;
        }
    }

    @Test
    public void test_push_fc() {
        _push_fc(push_asm_hb_fc_gap());
        _push_fc(push_asm_hb_fc_xgap());
    }

    private void _push_fc(SubscribeOptions so) {
        MockPublishInternal mc = new MockPublishInternal(new Options.Builder().build());
        AutoStatusManager pre = new AutoStatusManager(mc, so, so.getConsumerConfiguration(), null, false, true);
        assertNull(pre.getLastFcSubject());
        pre.preProcess(getFlowControl(1));
        assertEquals(getFcSubject(1), pre.getLastFcSubject());
        assertEquals(getFcSubject(1), mc.fcSubject);
        assertEquals(1, mc.pubCount);

        pre.preProcess(getFlowControl(1)); // duplicate should not call publish
        assertEquals(getFcSubject(1), pre.getLastFcSubject());
        assertEquals(getFcSubject(1), mc.fcSubject);
        assertEquals(1, mc.pubCount);

        pre.preProcess(getFlowControl(2)); // duplicate should not call publish
        assertEquals(getFcSubject(2), pre.getLastFcSubject());
        assertEquals(getFcSubject(2), mc.fcSubject);
        assertEquals(2, mc.pubCount);

        pre.preProcess(getFcHeartbeat(2)); // duplicate should not call publish
        assertEquals(getFcSubject(2), pre.getLastFcSubject());
        assertEquals(getFcSubject(2), mc.fcSubject);
        assertEquals(2, mc.pubCount);

        pre.preProcess(getFcHeartbeat(3));
        assertEquals(getFcSubject(3), pre.getLastFcSubject());
        assertEquals(getFcSubject(3), mc.fcSubject);
        assertEquals(3, mc.pubCount);
    }

    @Test
    public void test_push_xfc() {
        _push_xfc(push_asm_hb_xfc_gap());
        _push_xfc(push_asm_hb_xfc_xgap());
        _push_xfc(push_asm_xhb_xfc_gap());
        _push_xfc(push_asm_xhb_xfc_xgap());
    }

    private void _push_xfc(SubscribeOptions so) {
        MockPublishInternal mc = new MockPublishInternal(new Options.Builder().build());
        AutoStatusManager pre = new AutoStatusManager(mc, so, so.getConsumerConfiguration(), null, false, true);
        assertNull(pre.getLastFcSubject());

        pre.preProcess(getFlowControl(1));
        assertNull(pre.getLastFcSubject());
        assertNull(mc.fcSubject);
        assertEquals(0, mc.pubCount);

        pre.preProcess(getHeartbeat());
        assertNull(pre.getLastFcSubject());
        assertNull(mc.fcSubject);
        assertEquals(0, mc.pubCount);
    }

    @Test
    public void test_received_time() {
        _received_time_yes(push_asm_hb_fc_gap());
        _received_time_yes(push_asm_hb_fc_xgap());
        _received_time_yes(push_asm_hb_xfc_gap());
        _received_time_yes(push_asm_hb_xfc_xgap());

        _received_time_no(pull_asm_gap());
        _received_time_no(pull_asm_xgap());
        _received_time_no(push_asm_xhb_xfc_gap());
        _received_time_no(push_asm_xhb_xfc_xgap());
        _received_time_no(pull_xasm_gap());
        _received_time_no(push_xasm_gap());
        _received_time_no(pull_xasm_xgap_noop());
        _received_time_no(push_xasm_xgap_noop());
    }

    private void _received_time_yes(SubscribeOptions so) {
        AutoStatusManager manager = getManager(so);
        long before = System.currentTimeMillis();
        manager.preProcess(getTestJsMessage(1));
        long after = System.currentTimeMillis();
        long preTime = manager.getLastMessageReceivedTime();
        assertTrue(preTime >= before && preTime <= after);
    }

    private void _received_time_no(SubscribeOptions so) {
        AutoStatusManager manager = getManager(so);
        manager.preProcess(getTestJsMessage(1));
        assertEquals(0, manager.getLastMessageReceivedTime());
    }

    @Test
    public void test_hb_yes_settings() {
        ConsumerConfiguration cc = ConsumerConfiguration.builder().idleHeartbeat(1000).build();

        // MessageAlarmTime default
        PushSubscribeOptions so = new PushSubscribeOptions.Builder().configuration(cc).build();
        AutoStatusManager manager = getManager(so);
        assertEquals(1000, manager.getIdleHeartbeatSetting());
        assertEquals(3000, manager.getAlarmPeriodSetting());

        // MessageAlarmTime < idleHeartbeat
        so = new PushSubscribeOptions.Builder().configuration(cc).messageAlarmTime(999).build();
        manager = getManager(so);
        assertEquals(1000, manager.getIdleHeartbeatSetting());
        assertEquals(3000, manager.getAlarmPeriodSetting());

        // MessageAlarmTime == idleHeartbeat
        so = new PushSubscribeOptions.Builder().configuration(cc).messageAlarmTime(1000).build();
        manager = getManager(so);
        assertEquals(1000, manager.getIdleHeartbeatSetting());
        assertEquals(1000, manager.getAlarmPeriodSetting());

        // MessageAlarmTime > idleHeartbeat
        so = new PushSubscribeOptions.Builder().configuration(cc).messageAlarmTime(2000).build();
        manager = getManager(so);
        assertEquals(1000, manager.getIdleHeartbeatSetting());
        assertEquals(2000, manager.getAlarmPeriodSetting());
    }

    @Test
    public void test_hb_no_settings() {
        _settings_hb_no(push_asm_xhb_xfc_gap());
        _settings_hb_no(push_asm_xhb_xfc_xgap());
        _settings_hb_no(push_xasm_gap());
        _settings_hb_no(push_xasm_xgap_noop());
    }

    private void _settings_hb_no(SubscribeOptions so) {
        AutoStatusManager manager = getManager(so);
        assertEquals(0, manager.getIdleHeartbeatSetting());
        assertEquals(0, manager.getAlarmPeriodSetting());
    }

    private ConsumerConfiguration cc_fc_hb() {
        return ConsumerConfiguration.builder().flowControl(true).idleHeartbeat(1000).build();
    }

    private ConsumerConfiguration cc_xfc_hb() {
        return ConsumerConfiguration.builder().idleHeartbeat(1000).build();
    }

    private ConsumerConfiguration cc_xfc_xhb() {
        return ConsumerConfiguration.builder().build();
    }

    private SubscribeOptions pull_asm_gap() {
        return new PullSubscribeOptions.Builder().durable(DURABLE).build();
    }

    private SubscribeOptions pull_asm_xgap() {
        return new PullSubscribeOptions.Builder().durable(DURABLE).autoGapDetect(false).build();
    }

    private SubscribeOptions pull_xasm_gap() {
        return new PullSubscribeOptions.Builder().durable(DURABLE).autoStatusManage(false).build();
    }

    private SubscribeOptions pull_xasm_xgap_noop() {
        return new PullSubscribeOptions.Builder().durable(DURABLE).autoStatusManage(false).autoGapDetect(false).build();
    }

    private SubscribeOptions push_asm_hb_fc_gap() {
        return new PushSubscribeOptions.Builder().configuration(cc_fc_hb()).build();
    }

    private SubscribeOptions push_asm_hb_fc_xgap() {
        return new PushSubscribeOptions.Builder().configuration(cc_fc_hb()).autoGapDetect(false).build();
    }

    private SubscribeOptions push_asm_hb_xfc_gap() {
        return new PushSubscribeOptions.Builder().configuration(cc_xfc_hb()).build();
    }

    private SubscribeOptions push_asm_hb_xfc_xgap() {
        return new PushSubscribeOptions.Builder().configuration(cc_xfc_hb()).autoGapDetect(false).build();
    }

    private SubscribeOptions push_asm_xhb_xfc_gap() {
        return new PushSubscribeOptions.Builder().configuration(cc_xfc_xhb()).build();
    }

    private SubscribeOptions push_asm_xhb_xfc_xgap() {
        return new PushSubscribeOptions.Builder().configuration(cc_xfc_xhb()).autoGapDetect(false).build();
    }

    private SubscribeOptions push_xasm_gap() {
        return new PushSubscribeOptions.Builder().autoStatusManage(false).build();
    }

    private SubscribeOptions push_xasm_xgap_noop() {
        return new PushSubscribeOptions.Builder().autoStatusManage(false).autoGapDetect(false).build();
    }

    private AutoStatusManager getManager(SubscribeOptions so) {
        return getManager(null, so, null, true, false);
    }

    private AutoStatusManager getManager(NatsConnection conn, SubscribeOptions so, NatsJetStreamSubscription sub, boolean syncMode, boolean queueMode) {
        return new AutoStatusManager(conn, so, so.getConsumerConfiguration(), sub, queueMode, syncMode);
    }

    private NatsMessage getFlowControl(int replyToId) {
        NatsMessage.InternalMessageFactory imf = new NatsMessage.InternalMessageFactory("sid", "subj", getFcSubject(replyToId), 0, false);
        imf.setHeaders(new IncomingHeadersProcessor(("NATS/1.0 " + FLOW_OR_HEARTBEAT_STATUS_CODE + " " + FLOW_CONTROL_TEXT + "\r\n").getBytes()));
        return imf.getMessage();
    }

    private String getFcSubject(int id) {
        return "fcSubject." + id;
    }

    private NatsMessage getFcHeartbeat(int replyToId) {
        NatsMessage.InternalMessageFactory imf = new NatsMessage.InternalMessageFactory("sid", "subj", null, 0, false);
        String s = "NATS/1.0 " + FLOW_OR_HEARTBEAT_STATUS_CODE + " " + HEARTBEAT_TEXT + "\r\n" + CONSUMER_STALLED_HDR + ":" + getFcSubject(replyToId) + "\r\n\r\n";
        imf.setHeaders(new IncomingHeadersProcessor(s.getBytes()));
        return imf.getMessage();
    }

    private NatsMessage getHeartbeat() {
        NatsMessage.InternalMessageFactory imf = new NatsMessage.InternalMessageFactory("sid", "subj", null, 0, false);
        String s = "NATS/1.0 " + FLOW_OR_HEARTBEAT_STATUS_CODE + " " + HEARTBEAT_TEXT + "\r\n";
        imf.setHeaders(new IncomingHeadersProcessor(s.getBytes()));
        return imf.getMessage();
    }

    private NatsMessage get404() {
        return getStatus(404, "not found");
    }

    private NatsMessage get408() {
        return getStatus(408, "expired");
    }

    private NatsMessage getUnkStatus() {
        return getStatus(999, "blah blah");
    }

    private NatsMessage getStatus(int code, String message) {
        NatsMessage.InternalMessageFactory imf = new NatsMessage.InternalMessageFactory("sid", "subj", null, 0, false);
        imf.setHeaders(new IncomingHeadersProcessor(("NATS/1.0 " + code + " " + message + "\r\n").getBytes()));
        return imf.getMessage();
    }

    static class PreEl implements ErrorListener {
        JetStreamSubscription sub;
        long lastStreamSequence = -1; // TODO add checking these in tests
        long lastConsumerSequence = -1;
        long expectedConsumerSeq = -1;
        long receivedConsumerSeq = -1;
        Status status;

        public void reset() {
            sub = null;
            expectedConsumerSeq = -1;
            receivedConsumerSeq = -1;
            status = null;
        }

        @Override
        public void errorOccurred(Connection conn, String error) {}

        @Override
        public void exceptionOccurred(Connection conn, Exception exp) {}

        @Override
        public void slowConsumerDetected(Connection conn, Consumer consumer) {}

        @Override
        public void messageGapDetected(Connection conn, JetStreamSubscription sub,
                                       long lastStreamSequence, long lastConsumerSequence,
                                       long expectedConsumerSequence, long receivedConsumerSequence) {
            this.sub = sub;
            this.lastStreamSequence = lastStreamSequence;
            this.lastConsumerSequence = lastConsumerSequence;
            this.expectedConsumerSeq = expectedConsumerSequence;
            this.receivedConsumerSeq = receivedConsumerSequence;
        }

        @Override
        public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
            this.sub = sub;
            this.status = status;
        }
    }

    @Test
    public void test_NatsJetStreamSubscriptionMessageHandler_isNecessary() throws Exception {
        SubscribeOptions noop = push_xasm_xgap_noop();
        SubscribeOptions yesop = push_xasm_gap();

                    // | autoAck | op    | necessary
        _isNecessaryAck( true,     noop,   true);
        _isNecessaryAck( false,    noop,   false);
        _isNecessaryAck( true,     yesop,  true);
        _isNecessaryAck( false,    yesop,  true);

                      // | queueMode | op    | necessary
        _isNecessaryQMode( false,      noop,   false);
        _isNecessaryQMode( true,       noop,   false);
        _isNecessaryQMode( false,      yesop,  true);
        _isNecessaryQMode( true,       yesop,  false);

        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();
            Dispatcher d = nc.createDispatcher();

                                   // | autoAck | op    | necessary
            _handlerCreatedOrNot(js, d, true,     noop,   true);
            _handlerCreatedOrNot(js, d, false,    noop,   false);
            _handlerCreatedOrNot(js, d, true,     yesop,  true);
            _handlerCreatedOrNot(js, d, false,    yesop,  true);
        });
    }

    private void _isNecessaryAck(boolean autoAck, SubscribeOptions so, boolean necessary) {
        NatsJetStreamSubscriptionMessageHandler mh =
            new NatsJetStreamSubscriptionMessageHandler(null, msg -> {}, autoAck, false, so, so.getConsumerConfiguration());
        assertEquals(necessary, mh.isNecessary());
    }

    private void _handlerCreatedOrNot(JetStream js, Dispatcher d, boolean autoAck, SubscribeOptions so, boolean necessary) {
        try {
            MessageHandler userMh = msg -> {};
            NatsJetStreamSubscription sub = (NatsJetStreamSubscription)js.subscribe(SUBJECT, d, userMh, autoAck, (PushSubscribeOptions) so);
            MessageHandler mh = sub.getNatsDispatcher().getSubscriptionHandlers().get(sub.getSID());
            if (necessary) {
                assertTrue(mh instanceof NatsJetStreamSubscriptionMessageHandler);
            }
            else {
                assertFalse(mh instanceof NatsJetStreamSubscriptionMessageHandler);
                assertSame(mh, userMh);
            }
        } catch (Exception e) {
            fail();
        }
    }

    private void _isNecessaryQMode(boolean queueMode, SubscribeOptions so, boolean necessary) {
        NatsJetStreamSubscriptionMessageHandler mh =
            new NatsJetStreamSubscriptionMessageHandler(null, msg -> {}, false, queueMode, so, so.getConsumerConfiguration());
        assertEquals(necessary, mh.isNecessary());
    }

    @Test
    public void test_NatsJetStreamSubscriptionMessageHandler_ackIsCalledOrNot() {
        SubscribeOptions noop = push_xasm_xgap_noop();

        NatsJetStreamSubscriptionMessageHandler autoAckMh =
            new NatsJetStreamSubscriptionMessageHandler(null, msg -> {}, true, false, noop, noop.getConsumerConfiguration());

        // +autoAck +JetStream
        assertThrows(IllegalStateException.class, () -> autoAckMh.onMessage(getTestJsMessage(1)));

        // +autoAck -JetStream (status message)
        try {
            autoAckMh.onMessage(getHeartbeat());
        } catch (InterruptedException e) {
            fail();
        }

        NatsJetStreamSubscriptionMessageHandler xautoAckMh =
            new NatsJetStreamSubscriptionMessageHandler(null, msg -> {}, false, false, noop, noop.getConsumerConfiguration());

        // -autoAck message doesn't matter
        try {
            xautoAckMh.onMessage(getTestJsMessage(1));
            xautoAckMh.onMessage(getHeartbeat());
        } catch (InterruptedException e) {
            fail();
        }
    }
}
