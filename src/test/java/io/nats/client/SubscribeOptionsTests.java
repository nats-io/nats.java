// Copyright 2020 The NATS Authors
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

import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import static io.nats.client.SubscribeOptions.DEFAULT_ORDERED_HEARTBEAT;
import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.support.NatsJetStreamClientError.*;
import static org.junit.jupiter.api.Assertions.*;

public class SubscribeOptionsTests extends TestBase {
    private static final String[] badNames = {HAS_DOT, GT_NOT_SEGMENT, STAR_NOT_SEGMENT, HAS_FWD_SLASH, HAS_BACK_SLASH};

    @Test
    public void testPushAffirmative() {
        PushSubscribeOptions so = PushSubscribeOptions.builder().build();

        // starts out all null which is fine
        assertNull(so.getStream());
        assertNull(so.getDurable());
        assertNull(so.getName());
        assertNull(so.getDeliverSubject());

        String stream = random();
        String durable = random();
        String deliver = random();
        so = PushSubscribeOptions.builder()
                .stream(stream).durable(durable).deliverSubject(deliver).build();

        assertEquals(stream, so.getStream());
        assertEquals(durable, so.getDurable());
        assertEquals(durable, so.getName());
        assertEquals(deliver, so.getDeliverSubject());

        // demonstrate that you can clear the builder
        so = PushSubscribeOptions.builder()
                .stream(null).deliverSubject(null).durable(null).build();
        assertNull(so.getStream());
        assertNull(so.getDurable());
        assertNull(so.getName());
        assertNull(so.getDeliverSubject());
        assertFalse(so.isPull());

        assertNotNull(so.toString()); // COVERAGE
    }

    @Test
    public void testDurableValidation() {
        // push
        PushSubscribeOptions uso = PushSubscribeOptions.builder()
            .durable(null)
            .configuration(ConsumerConfiguration.builder().durable(null).build())
            .build();
        assertNull(uso.getDurable());

        uso = PushSubscribeOptions.builder()
            .durable(null)
            .configuration(ConsumerConfiguration.builder().durable("y").build())
            .build();
        assertEquals("y", uso.getDurable());
        assertEquals("y", uso.getName());

        uso = PushSubscribeOptions.builder()
            .durable("x")
            .configuration(ConsumerConfiguration.builder().durable(null).build())
            .build();
        assertEquals("x", uso.getDurable());
        assertEquals("x", uso.getName());

        uso = PushSubscribeOptions.builder()
            .durable("x")
            .configuration(ConsumerConfiguration.builder().durable("x").build())
            .build();
        assertEquals("x", uso.getDurable());
        assertEquals("x", uso.getName());

        assertClientError(JsSoDurableMismatch, () -> PushSubscribeOptions.builder()
                .durable("x")
                .configuration(ConsumerConfiguration.builder().durable("y").build())
                .build());

        assertNull(PushSubscribeOptions.builder().build().getDurable());

        // pull
        PullSubscribeOptions lso = PullSubscribeOptions.builder()
            .durable(null)
            .configuration(ConsumerConfiguration.builder().durable("y").build())
            .build();
        assertEquals("y", lso.getDurable());
        assertEquals("y", lso.getName());

        lso = PullSubscribeOptions.builder()
            .durable("x")
            .configuration(ConsumerConfiguration.builder().durable(null).build())
            .build();
        assertEquals("x", lso.getDurable());
        assertEquals("x", lso.getName());

        lso = PullSubscribeOptions.builder()
            .durable("x")
            .configuration(ConsumerConfiguration.builder().durable("x").build())
            .build();
        assertEquals("x", lso.getDurable());
        assertEquals("x", lso.getName());

        assertClientError(JsSoDurableMismatch, () -> PullSubscribeOptions.builder()
                .durable("x")
                .configuration(ConsumerConfiguration.builder().durable("y").build())
                .build());
    }

    @Test
    public void testDeliverGroupValidation() {
        assertNull(PushSubscribeOptions.builder()
                .deliverGroup(null)
                .configuration(ConsumerConfiguration.builder().deliverGroup(null).build())
                .build()
                .getDeliverGroup());

        assertEquals("y", PushSubscribeOptions.builder()
                .deliverGroup(null)
                .configuration(ConsumerConfiguration.builder().deliverGroup("y").build())
                .build()
                .getDeliverGroup());

        assertEquals("x", PushSubscribeOptions.builder()
                .deliverGroup("x")
                .configuration(ConsumerConfiguration.builder().deliverGroup(null).build())
                .build()
                .getDeliverGroup());

        assertEquals("x", PushSubscribeOptions.builder()
                .deliverGroup("x")
                .configuration(ConsumerConfiguration.builder().deliverGroup("x").build())
                .build()
                .getDeliverGroup());

        assertClientError(JsSoDeliverGroupMismatch, () -> PushSubscribeOptions.builder()
                .deliverGroup("x")
                .configuration(ConsumerConfiguration.builder().deliverGroup("y").build())
                .build());
    }

    @Test
    public void testDeliverSubjectValidation() {
        assertNull(PushSubscribeOptions.builder()
                .deliverSubject(null)
                .configuration(ConsumerConfiguration.builder().deliverSubject(null).build())
                .build()
                .getDeliverSubject());

        assertEquals("y", PushSubscribeOptions.builder()
                .deliverSubject(null)
                .configuration(ConsumerConfiguration.builder().deliverSubject("y").build())
                .build()
                .getDeliverSubject());

        assertEquals("x", PushSubscribeOptions.builder()
                .deliverSubject("x")
                .configuration(ConsumerConfiguration.builder().deliverSubject(null).build())
                .build()
                .getDeliverSubject());

        assertEquals("x", PushSubscribeOptions.builder()
                .deliverSubject("x")
                .configuration(ConsumerConfiguration.builder().deliverSubject("x").build())
                .build()
                .getDeliverSubject());

        assertClientError(JsSoDeliverSubjectMismatch, () -> PushSubscribeOptions.builder()
                .deliverSubject("x")
                .configuration(ConsumerConfiguration.builder().deliverSubject("y").build())
                .build());
    }

    @Test
    public void testPullAffirmative() {
        String stream = random();
        String durable = random();
        PullSubscribeOptions.Builder builder = PullSubscribeOptions.builder()
                .stream(stream)
                .durable(durable);

        PullSubscribeOptions so = builder.build();
        assertEquals(stream, so.getStream());
        assertEquals(durable, so.getDurable());
        assertEquals(durable, so.getName());
        assertTrue(so.isPull());

        assertNotNull(so.toString()); // COVERAGE
    }

    @Test
    public void testPushFieldValidation() {
        for (String bad : badNames) {
            PushSubscribeOptions.Builder pushBuilder = PushSubscribeOptions.builder();
            assertThrows(IllegalArgumentException.class, () -> pushBuilder.stream(bad).build());
            assertThrows(IllegalArgumentException.class, () -> pushBuilder.durable(bad).build());
            assertThrows(IllegalArgumentException.class, () -> pushBuilder.name(bad).build());
            assertThrows(IllegalArgumentException.class, () -> ConsumerConfiguration.builder().durable(bad).build());
            assertThrows(IllegalArgumentException.class, () -> ConsumerConfiguration.builder().name(bad).build());
        }

        String name = random();
        String durable = random();
        assertClientError(JsConsumerNameDurableMismatch, () -> PushSubscribeOptions.builder().name(name).durable(durable).build());

        // durable directly
        PushSubscribeOptions uso = PushSubscribeOptions.builder().durable(durable).build();
        assertEquals(durable, uso.getDurable());
        assertEquals(durable, uso.getName());
        uso = PushSubscribeOptions.builder().name(name).build();
        assertNull(uso.getDurable());
        assertEquals(name, uso.getName());

        // in configuration
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(durable).build();
        uso = PushSubscribeOptions.builder().configuration(cc).build();
        assertEquals(durable, uso.getDurable());
        assertEquals(durable, uso.getName());
        cc = ConsumerConfiguration.builder().name(name).build();
        uso = PushSubscribeOptions.builder().configuration(cc).build();
        assertNull(uso.getDurable());
        assertEquals(name, uso.getName());

        // new helper
        ConsumerConfiguration.builder().durable(durable).buildPushSubscribeOptions();
        ConsumerConfiguration.builder().name(name).buildPushSubscribeOptions();
    }

    @Test
    public void testPullFieldValidation() {
        for (String bad : badNames) {
            PullSubscribeOptions.Builder pullBuilder = PullSubscribeOptions.builder();
            assertThrows(IllegalArgumentException.class, () -> pullBuilder.stream(bad).build());
            assertThrows(IllegalArgumentException.class, () -> pullBuilder.durable(bad).build());
            assertThrows(IllegalArgumentException.class, () -> pullBuilder.name(bad).build());
            assertThrows(IllegalArgumentException.class, () -> ConsumerConfiguration.builder().durable(bad).build());
            assertThrows(IllegalArgumentException.class, () -> ConsumerConfiguration.builder().name(bad).build());
        }

        String durable = random();
        assertClientError(JsConsumerNameDurableMismatch, () -> PullSubscribeOptions.builder().name(random()).durable(durable).build());

        // durable directly
        PullSubscribeOptions lso = PullSubscribeOptions.builder().durable(durable).build();
        assertEquals(durable, lso.getDurable());
        assertEquals(durable, lso.getName());

        // in configuration
        ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(durable).build();
        lso = PullSubscribeOptions.builder().configuration(cc).build();
        assertEquals(durable, lso.getDurable());
        assertEquals(durable, lso.getName());

        // new helper
        lso = ConsumerConfiguration.builder().durable(durable).buildPullSubscribeOptions();
        assertEquals(durable, lso.getDurable());
        assertEquals(durable, lso.getName());
    }

    @Test
    public void testCreationErrors() {
        String durable1 = random();
        String durable2 = random();
        ConsumerConfiguration cc1 = ConsumerConfiguration.builder().durable(durable1).build();
        assertClientError(JsSoDurableMismatch,
            () -> PushSubscribeOptions.builder().configuration(cc1).durable(durable2).build());

        String deliver1 = random();
        String deliver2 = random();
        ConsumerConfiguration cc2 = ConsumerConfiguration.builder().deliverGroup(deliver1).build();
        assertClientError(JsSoDeliverGroupMismatch,
            () -> PushSubscribeOptions.builder().configuration(cc2).deliverGroup(deliver2).build());

        String name1 = random();
        String name2 = random();
        ConsumerConfiguration cc3 = ConsumerConfiguration.builder().name(name1).build();
        assertClientError(JsSoNameMismatch,
            () -> PushSubscribeOptions.builder().configuration(cc3).name(name2).build());
    }

    @Test
    public void testBindCreationErrors() {
        String random = random();

        // bind
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.bind(null, random));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.bind(EMPTY, random));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.bind(random, null));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.bind(random, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(random).bind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(EMPTY).durable(random).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().durable(random).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(random).durable(EMPTY).bind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(EMPTY).name(random).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().name(random).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PushSubscribeOptions.builder().stream(random).name(EMPTY).bind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.bind(null, random));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.bind(EMPTY, random));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.bind(random, null));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.bind(random, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(random).bind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(EMPTY).durable(random).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().durable(random).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(random).durable(EMPTY).bind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(EMPTY).name(random).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().name(random).bind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(random).name(EMPTY).bind(true).build());

        // fast bind
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.fastBind(null, random));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.fastBind(EMPTY, random));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.fastBind(random, null));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.fastBind(random, EMPTY));
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(random).fastBind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(EMPTY).durable(random).fastBind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().durable(random).fastBind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(random).durable(EMPTY).fastBind(true).build());

        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(EMPTY).name(random).fastBind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().name(random).fastBind(true).build());
        assertThrows(IllegalArgumentException.class, () -> PullSubscribeOptions.builder().stream(random).name(EMPTY).fastBind(true).build());
    }

    @Test
    public void testOrderedCreation() {
        ConsumerConfiguration ccEmpty = ConsumerConfiguration.builder().build();
        PushSubscribeOptions.builder().configuration(ccEmpty).ordered(true).build();

        String random = random();
        assertClientError(JsSoOrderedNotAllowedWithBind,
            () -> PushSubscribeOptions.builder().stream(random).bind(true).ordered(true).build());

        assertClientError(JsSoOrderedNotAllowedWithDeliverGroup,
            () -> PushSubscribeOptions.builder().deliverGroup(random).ordered(true).build());

        assertClientError(JsSoOrderedNotAllowedWithDurable,
            () -> PushSubscribeOptions.builder().durable(random).ordered(true).build());

        assertClientError(JsSoOrderedNotAllowedWithDeliverSubject,
            () -> PushSubscribeOptions.builder().deliverSubject(random).ordered(true).build());

        ConsumerConfiguration ccAckNotNone1 = ConsumerConfiguration.builder().ackPolicy(AckPolicy.All).build();
        assertClientError(JsSoOrderedRequiresAckPolicyNone,
            () -> PushSubscribeOptions.builder().configuration(ccAckNotNone1).ordered(true).build());

        ConsumerConfiguration ccAckNotNone2 = ConsumerConfiguration.builder().ackPolicy(AckPolicy.Explicit).build();
        assertClientError(JsSoOrderedRequiresAckPolicyNone,
            () -> PushSubscribeOptions.builder().configuration(ccAckNotNone2).ordered(true).build());

        ConsumerConfiguration ccAckNoneOk = ConsumerConfiguration.builder().ackPolicy(AckPolicy.None).build();
        PushSubscribeOptions.builder().configuration(ccAckNoneOk).ordered(true).build();

        ConsumerConfiguration ccMax = ConsumerConfiguration.builder().maxDeliver(2).build();
        assertClientError(JsSoOrderedRequiresMaxDeliverOfOne,
            () -> PushSubscribeOptions.builder().configuration(ccMax).ordered(true).build());

        ConsumerConfiguration ccHb = ConsumerConfiguration.builder().idleHeartbeat(100).build();
        PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(ccHb).ordered(true).build();
        assertNotNull(pso.getConsumerConfiguration().getIdleHeartbeat());
        assertEquals(100, pso.getConsumerConfiguration().getIdleHeartbeat().toMillis());

        ccHb = ConsumerConfiguration.builder().idleHeartbeat(DEFAULT_ORDERED_HEARTBEAT + 1).build();
        pso = PushSubscribeOptions.builder().configuration(ccHb).ordered(true).build();
        assertNotNull(pso.getConsumerConfiguration().getIdleHeartbeat());
        assertEquals(DEFAULT_ORDERED_HEARTBEAT + 1, pso.getConsumerConfiguration().getIdleHeartbeat().toMillis());

        // okay if you set it to true
        ConsumerConfiguration cc = ConsumerConfiguration.builder().memStorage(true).build();
        PushSubscribeOptions.builder().configuration(cc).ordered(true).build();

        // okay if you set it to 1
        cc = ConsumerConfiguration.builder().numReplicas(1).build();
        PushSubscribeOptions.builder().configuration(cc).ordered(true).build();

        // not okay if you set it to false
        ConsumerConfiguration ccMs = ConsumerConfiguration.builder().memStorage(false).build();
        assertClientError(JsSoOrderedMemStorageNotSuppliedOrTrue,
            () -> PushSubscribeOptions.builder().configuration(ccMs).ordered(true).build());

        // not okay if you set it to something other than 1
        ConsumerConfiguration ccR = ConsumerConfiguration.builder().numReplicas(3).build();
        assertClientError(JsSoOrderedReplicasNotSuppliedOrOne,
            () -> PushSubscribeOptions.builder().configuration(ccR).ordered(true).build());
    }
}
