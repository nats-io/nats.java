// Copyright 2026 The NATS Authors
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

import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPullIteratorTests extends JetStreamTestBase {
    @ParameterizedTest
    @CsvSource({"0, false", "1, false", "3, false", "0, true", "1, true", "3, true"})
    public void testNextWithoutHasNext(int bufferedCount, boolean millisOverload) throws Exception {
        jsServer.run(nc -> {
            JetStream js = nc.jetStream();
            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            JetStreamSubscription sub = js.subscribe(tsc.subject(), PullSubscribeOptions.builder().build());
            for (int i = 0; i < 3; i++) {
                js.publish(tsc.subject(), Integer.toString(i).getBytes(StandardCharsets.UTF_8));
            }
            if (bufferedCount > 0) {
                sub.pull(bufferedCount);
                assertTrueByTimeout(5000, () -> sub.getPendingMessageCount() == bufferedCount);
            }

            Iterator<Message> iterator = iterate(sub, 3, millisOverload);
            assertMessage(iterator.next(), "0");
            assertTrue(iterator.hasNext());
            assertTrue(iterator.hasNext());
            assertMessage(iterator.next(), "1");
            assertMessage(iterator.next(), "2");
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
            assertThrows(NoSuchElementException.class, iterator::next);
            assertThrows(UnsupportedOperationException.class, iterator::remove);
        });
    }

    @ParameterizedTest
    @CsvSource({"0, false", "1, false", "0, true", "1, true"})
    public void testNextAfterPullExpires(int availableCount, boolean millisOverload) throws Exception {
        jsServer.run(nc -> {
            JetStream js = nc.jetStream();
            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            JetStreamSubscription sub = js.subscribe(tsc.subject(), PullSubscribeOptions.builder().build());
            if (availableCount > 0) {
                js.publish(tsc.subject(), "0".getBytes(StandardCharsets.UTF_8));
            }

            Iterator<Message> iterator = iterate(sub, 2, millisOverload);
            if (availableCount > 0) {
                assertMessage(iterator.next(), "0");
            }
            assertThrows(NoSuchElementException.class, iterator::next);
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testForEachRemaining(boolean millisOverload) throws Exception {
        jsServer.run(nc -> {
            JetStream js = nc.jetStream();
            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            JetStreamSubscription sub = js.subscribe(tsc.subject(), PullSubscribeOptions.builder().build());
            for (int i = 0; i < 3; i++) {
                js.publish(tsc.subject(), Integer.toString(i).getBytes(StandardCharsets.UTF_8));
            }

            Iterator<Message> iterator = iterate(sub, 3, millisOverload);
            List<Message> messages = new ArrayList<>();
            iterator.forEachRemaining(messages::add);
            assertEquals(3, messages.size());
            for (int i = 0; i < messages.size(); i++) {
                assertMessage(messages.get(i), Integer.toString(i));
            }
            iterator.forEachRemaining(message -> fail("Iterator must stay exhausted"));
        });
    }

    private Iterator<Message> iterate(JetStreamSubscription sub, int batchSize, boolean millisOverload) {
        return millisOverload ? sub.iterate(batchSize, 1000) : sub.iterate(batchSize, Duration.ofSeconds(1));
    }

    private void assertMessage(Message message, String expected) {
        assertNotNull(message);
        assertEquals(expected, new String(message.getData(), StandardCharsets.UTF_8));
        message.ack();
    }
}
