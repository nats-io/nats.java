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

import java.io.IOException;

import static io.nats.client.support.NatsJetStreamConstants.CONSUMER_STALLED_HDR;
import static io.nats.client.support.Status.*;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("SameParameterValue")
public class NatsJetStreamAutoStatusManagerTests extends JetStreamTestBase {

    @Test
    public void testConstruction() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);

            _construction(nc, true, false, false, true, pull_gap(), sub);
            _construction(nc, true, false, false, false, pull_xgap(), sub);

            _construction(nc, false, true, true, true, push_hb_fc_gap(), sub);
            _construction(nc, false, true, true, false, push_hb_fc_xgap(), sub);
            _construction(nc, false, true, false, true, push_hb_xfc_gap(), sub);
            _construction(nc, false, true, false, false, push_hb_xfc_xgap(), sub);

            _construction(nc, false, false, false, true, push_xhb_xfc_gap(), sub);
            _construction(nc, false, false, false, false, push_xhb_xfc_xgap(), sub);
        });
    }

    private void _construction(Connection conn, boolean pull, boolean hb, boolean fc, boolean gap, SubscribeOptions so, NatsJetStreamSubscription sub) {
        NatsJetStreamAutoStatusManager manager = getManager(conn, so, sub, true, false);
        assertTrue(manager.isSyncMode());
        assertFalse(manager.isQueueMode());
        assertEquals(pull, manager.isPull());
        assertEquals(hb, manager.isHb());
        assertEquals(fc, manager.isFc());
        assertEquals(gap, manager.isGap());

        // queue mode
        if (!pull) {
            manager = getManager(conn, so, sub, true, true);
            assertTrue(manager.isSyncMode());
            assertTrue(manager.isQueueMode());
            assertEquals(pull, manager.isPull());
            assertFalse(manager.isHb());
            assertFalse(manager.isFc());
            assertFalse(manager.isGap());
        }
    }

    @Test
    public void test_status_handle_pull() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);
            _status_handle_pull(nc, sub, pull_gap());
            _status_handle_pull(nc, sub, pull_xgap());
        });
    }

    private void _status_handle_pull(Connection conn, NatsJetStreamSubscription sub, SubscribeOptions so) {
        NatsJetStreamAutoStatusManager manager = getManager(conn, so, sub, true, false);
        assertFalse(manager.manage(getTestJsMessage(1)));
        assertTrue(manager.manage(get404()));
        assertTrue(manager.manage(get408()));
        _status_handle_throws(sub, manager, getFlowControl(1));
        _status_handle_throws(sub, manager, getFcHeartbeat(1));
        _status_handle_throws(sub, manager, getUnkStatus());
    }

    @Test
    public void test_status_handle_pushSync() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);
            _status_handle_pushSync(nc, sub, push_hb_fc_gap());
            _status_handle_pushSync(nc, sub, push_hb_fc_xgap());
            _status_handle_pushSync(nc, sub, push_hb_xfc_gap());
            _status_handle_pushSync(nc, sub, push_hb_xfc_xgap());
            _status_handle_pushSync(nc, sub, push_xhb_xfc_gap());
            _status_handle_pushSync(nc, sub, push_xhb_xfc_xgap());
        });
    }

    private void _status_handle_pushSync(Connection conn, NatsJetStreamSubscription sub, SubscribeOptions so) {
        NatsJetStreamAutoStatusManager manager = getManager(conn, so, sub, true, false);
        assertFalse(manager.manage(getTestJsMessage(1)));
        assertTrue(manager.manage(getFlowControl(1)));
        assertTrue(manager.manage(getFcHeartbeat(1)));
        _status_handle_throws(sub, manager, get404());
        _status_handle_throws(sub, manager, get408());
        _status_handle_throws(sub, manager, getUnkStatus());
    }

    private void _status_handle_throws(NatsJetStreamSubscription sub, NatsJetStreamAutoStatusManager asm, Message m) {
        JetStreamStatusException jsse = assertThrows(JetStreamStatusException.class, () -> asm.manage(m));
        assertSame(sub, jsse.getSubscription());
        assertSame(m.getStatus(), jsse.getStatus());
    }

    @Test
    public void test_status_handle_pushAsync() throws Exception {
        AsmEl el = new AsmEl();
        runInJsServer(optsWithEl(el), nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);
            _status_handle_pushAsync(el, nc, sub, push_hb_fc_gap());
            _status_handle_pushAsync(el, nc, sub, push_hb_fc_xgap());
            _status_handle_pushAsync(el, nc, sub, push_hb_xfc_gap());
            _status_handle_pushAsync(el, nc, sub, push_hb_xfc_xgap());
            _status_handle_pushAsync(el, nc, sub, push_xhb_xfc_gap());
            _status_handle_pushAsync(el, nc, sub, push_xhb_xfc_xgap());
        });
    }

    private void _status_handle_pushAsync(AsmEl el, Connection conn, NatsJetStreamSubscription sub, SubscribeOptions so) {
        NatsJetStreamAutoStatusManager manager = getManager(conn, so, sub, false, false);
        el.reset();
        assertFalse(manager.manage(getTestJsMessage(1)));
        assertTrue(manager.manage(getFlowControl(1)));
        assertTrue(manager.manage(getFcHeartbeat(1)));

        Message m = get404();
        assertTrue(manager.manage(m));
        assertSame(sub, el.sub);
        assertSame(m.getStatus(), el.status);

        m = get408();
        assertTrue(manager.manage(m));
        assertSame(sub, el.sub);
        assertSame(m.getStatus(), el.status);

        m = getUnkStatus();
        assertTrue(manager.manage(m));
        assertSame(sub, el.sub);
        assertSame(m.getStatus(), el.status);
    }

    @Test
    public void test_gap_pull_pushSync() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);
            _gap_pull_pushSync(nc, sub, pull_gap());
            _gap_pull_pushSync(nc, sub, push_hb_fc_gap());
            _gap_pull_pushSync(nc, sub, push_hb_xfc_gap());
            _gap_pull_pushSync(nc, sub, push_xhb_xfc_gap());
        });
    }

    private void _gap_pull_pushSync(Connection conn, NatsJetStreamSubscription sub, SubscribeOptions so) {
        NatsJetStreamAutoStatusManager manager = getManager(conn, so, sub, true, false);
        assertEquals(-1, manager.getLastStreamSequence());
        assertEquals(-1, manager.getLastConsumerSequence());
        assertEquals(-1, manager.getExpectedConsumerSequence());
        manager.manage(getTestJsMessage(1));
        assertEquals(1, manager.getLastStreamSequence());
        assertEquals(1, manager.getLastConsumerSequence());
        assertEquals(2, manager.getExpectedConsumerSequence());
        manager.manage(getTestJsMessage(2));
        assertEquals(2, manager.getLastStreamSequence());
        assertEquals(2, manager.getLastConsumerSequence());
        assertEquals(3, manager.getExpectedConsumerSequence());
        JetStreamGapException jsge = assertThrows(JetStreamGapException.class, () -> manager.manage(getTestJsMessage(4)));
        assertSame(sub, jsge.getSubscription());
        assertEquals(2, manager.getLastStreamSequence());
        assertEquals(2, manager.getLastConsumerSequence());
        assertEquals(3, jsge.getExpectedConsumerSeq());
        assertEquals(4, jsge.getReceivedConsumerSeq());
    }

    @Test
    public void test_gap_pushAsync() throws Exception {
        AsmEl el = new AsmEl();
        runInJsServer(optsWithEl(el), nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);
            _gap_pushAsync(el, nc, sub, push_hb_fc_gap());
            _gap_pushAsync(el, nc, sub, push_hb_xfc_gap());
            _gap_pushAsync(el, nc, sub, push_xhb_xfc_gap());
        });

        // no error listener for coverage
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);
            _gap_pushAsync(null, nc, sub, push_hb_fc_gap());
            _gap_pushAsync(null, nc, sub, push_hb_xfc_gap());
            _gap_pushAsync(null, nc, sub, push_xhb_xfc_gap());
        });
    }

    private void _gap_pushAsync(AsmEl el, Connection conn, NatsJetStreamSubscription sub, SubscribeOptions so) {
        NatsJetStreamAutoStatusManager manager = getManager(conn, so, sub, false, false);
        if (el != null) {
            el.reset();
        }
        assertEquals(-1, manager.getExpectedConsumerSequence());
        manager.manage(getTestJsMessage(1));
        assertEquals(2, manager.getExpectedConsumerSequence());
        manager.manage(getTestJsMessage(2));
        assertEquals(3, manager.getExpectedConsumerSequence());
        manager.manage(getTestJsMessage(4));
        if (el != null) {
            assertSame(sub, el.sub);
            assertEquals(3, el.expectedConsumerSeq);
            assertEquals(4, el.receivedConsumerSeq);
        }
    }

    @Test
    public void test_push_fc() {
        _push_fc(push_hb_fc_gap());
        _push_fc(push_hb_fc_xgap());
    }

    private void _push_fc(SubscribeOptions so) {
        MockPublishInternal mc = new MockPublishInternal();
        NatsJetStreamAutoStatusManager asm = new NatsJetStreamAutoStatusManager(mc, so, so.getConsumerConfiguration(), false, true);
        assertNull(asm.getLastFcSubject());
        asm.manage(getFlowControl(1));
        assertEquals(getFcSubject(1), asm.getLastFcSubject());
        assertEquals(getFcSubject(1), mc.fcSubject);
        assertEquals(1, mc.pubCount);

        asm.manage(getFlowControl(1)); // duplicate should not call publish
        assertEquals(getFcSubject(1), asm.getLastFcSubject());
        assertEquals(getFcSubject(1), mc.fcSubject);
        assertEquals(1, mc.pubCount);

        asm.manage(getFlowControl(2)); // duplicate should not call publish
        assertEquals(getFcSubject(2), asm.getLastFcSubject());
        assertEquals(getFcSubject(2), mc.fcSubject);
        assertEquals(2, mc.pubCount);

        asm.manage(getFcHeartbeat(2)); // duplicate should not call publish
        assertEquals(getFcSubject(2), asm.getLastFcSubject());
        assertEquals(getFcSubject(2), mc.fcSubject);
        assertEquals(2, mc.pubCount);

        asm.manage(getFcHeartbeat(3));
        assertEquals(getFcSubject(3), asm.getLastFcSubject());
        assertEquals(getFcSubject(3), mc.fcSubject);
        assertEquals(3, mc.pubCount);
    }

    @Test
    public void test_push_xfc() {
        _push_xfc(push_hb_xfc_gap());
        _push_xfc(push_hb_xfc_xgap());
        _push_xfc(push_xhb_xfc_gap());
        _push_xfc(push_xhb_xfc_xgap());
    }

    private void _push_xfc(SubscribeOptions so) {
        MockPublishInternal mc = new MockPublishInternal();
        NatsJetStreamAutoStatusManager asm = new NatsJetStreamAutoStatusManager(mc, so, so.getConsumerConfiguration(), false, true);
        assertNull(asm.getLastFcSubject());

        asm.manage(getFlowControl(1));
        assertNull(asm.getLastFcSubject());
        assertNull(mc.fcSubject);
        assertEquals(0, mc.pubCount);

        asm.manage(getHeartbeat());
        assertNull(asm.getLastFcSubject());
        assertNull(mc.fcSubject);
        assertEquals(0, mc.pubCount);
    }

    @Test
    public void test_received_time() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            _received_time_yes(push_hb_fc_gap(), js);
            _received_time_yes(push_hb_fc_xgap(), js);
            _received_time_yes(push_hb_xfc_gap(), js);
            _received_time_yes(push_hb_xfc_xgap(), js);

            _received_time_no(js, jsm, js.subscribe(SUBJECT, pull_gap()));
            _received_time_no(js, jsm, js.subscribe(SUBJECT, pull_xgap()));
            _received_time_no(js, jsm, js.subscribe(SUBJECT, push_xhb_xfc_gap()));
            _received_time_no(js, jsm, js.subscribe(SUBJECT, push_xhb_xfc_xgap()));
        });
    }

    private void _received_time_yes(PushSubscribeOptions so, JetStream js) throws Exception {
        long before = System.currentTimeMillis();
        NatsJetStreamSubscription sub = (NatsJetStreamSubscription) js.subscribe(SUBJECT, so);

        // during the sleep, the heartbeat is delivered and is checked
        // by the heartbeat listener and recorded as received
        sleep(1050); // slightly longer than the idle heartbeat

        long preTime = sub.getAsm().getLastMsgReceived();
        assertTrue(preTime > before);
        sub.unsubscribe();
    }

    private void _received_time_no(JetStream js, JetStreamManagement jsm, JetStreamSubscription sub) throws IOException, JetStreamApiException, InterruptedException {
        js.publish(SUBJECT, dataBytes(0));
        sub.nextMessage(1000);
        assertEquals(0, ((NatsJetStreamSubscription)sub).getAsm().getLastMsgReceived());
        jsm.purgeStream(STREAM);
        sub.unsubscribe();
    }

    @Test
    public void test_hb_yes_settings() throws Exception {
        runInJsServer(nc -> {
            NatsJetStreamSubscription sub = mockSub(nc);

            ConsumerConfiguration cc = ConsumerConfiguration.builder().idleHeartbeat(1000).build();

            // MessageAlarmTime default
            PushSubscribeOptions so = new PushSubscribeOptions.Builder().configuration(cc).build();
            NatsJetStreamAutoStatusManager manager = getManager(nc, so, sub);
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
            NatsJetStreamSubscription sub = mockSub(nc);
            _settings_hb_no(nc, push_xhb_xfc_gap(), sub);
            _settings_hb_no(nc, push_xhb_xfc_xgap(), sub);
        });
    }

    private void _settings_hb_no(Connection conn, SubscribeOptions so, NatsJetStreamSubscription sub) {
        NatsJetStreamAutoStatusManager manager = getManager(conn, so, sub);
        assertEquals(0, manager.getIdleHeartbeatSetting());
        assertEquals(0, manager.getAlarmPeriodSetting());
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

    private PullSubscribeOptions pull_gap() {
        return new PullSubscribeOptions.Builder().durable(DURABLE).detectGaps(true).build();
    }

    private PullSubscribeOptions pull_xgap() {
        return new PullSubscribeOptions.Builder().durable(DURABLE).detectGaps(false).build();
    }

    private PushSubscribeOptions push_hb_fc_gap() {
        return new PushSubscribeOptions.Builder().configuration(cc_fc_hb()).detectGaps(true).build();
    }

    private PushSubscribeOptions push_hb_fc_xgap() {
        return new PushSubscribeOptions.Builder().configuration(cc_fc_hb()).detectGaps(false).build();
    }

    private PushSubscribeOptions push_hb_xfc_gap() {
        return new PushSubscribeOptions.Builder().configuration(cc_xfc_hb()).detectGaps(true).build();
    }

    private PushSubscribeOptions push_hb_xfc_xgap() {
        return new PushSubscribeOptions.Builder().configuration(cc_xfc_hb()).detectGaps(false).build();
    }

    private PushSubscribeOptions push_xhb_xfc_gap() {
        return new PushSubscribeOptions.Builder().configuration(cc_xfc_xhb()).detectGaps(true).build();
    }

    private PushSubscribeOptions push_xhb_xfc_xgap() {
        return new PushSubscribeOptions.Builder().configuration(cc_xfc_xhb()).detectGaps(false).build();
    }

    private NatsJetStreamAutoStatusManager getManager(Connection conn, SubscribeOptions so, NatsJetStreamSubscription sub) {
        return getManager(conn, so, sub, true, false);
    }

    private NatsJetStreamAutoStatusManager getManager(Connection conn, SubscribeOptions so, NatsJetStreamSubscription sub, boolean syncMode, boolean queueMode) {
        NatsJetStreamAutoStatusManager asm = new NatsJetStreamAutoStatusManager((NatsConnection)conn, so, so.getConsumerConfiguration(), queueMode, syncMode);
        asm.setSub(sub);
        return asm;
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

    static class AsmEl implements ErrorListener {
        JetStreamSubscription sub;
        long lastStreamSequence = -1;
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
        void publishInternal(String subject, String replyTo, Headers headers, byte[] data, boolean utf8mode) {
            fcSubject = subject;
            ++pubCount;
        }
    }

    private NatsJetStreamSubscription mockSub(Connection nc) throws IOException, JetStreamApiException {
        createTestStream(nc);
        JetStream js = nc.jetStream();
        return (NatsJetStreamSubscription) js.subscribe(SUBJECT);
    }
}
