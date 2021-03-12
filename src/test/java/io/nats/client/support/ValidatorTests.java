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

package io.nats.client.support;

import io.nats.client.impl.ConsumerConfiguration;
import io.nats.client.impl.Validator;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.nats.client.JetStreamSubscription.MAX_PULL_SIZE;
import static io.nats.client.impl.Validator.*;
import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

public class ValidatorTests {

    @Test
    public void testValidateMessageSubjectRequired() {
        allowed(Validator::validateMessageSubjectRequired, PLAIN, HAS_SPACE, HAS_DASH, HAS_DOT, HAS_STAR, HAS_GT);
        notAllowed(Validator::validateMessageSubjectRequired, null, EMPTY);
    }

    @Test
    public void testValidateJsSubscribeSubjectRequired() {
        allowed(Validator::validateJsSubscribeSubjectRequired, PLAIN, HAS_DASH, HAS_DOT, HAS_STAR, HAS_GT);
        notAllowed(Validator::validateJsSubscribeSubjectRequired, null, EMPTY, HAS_SPACE);
    }

    @Test
    public void testValidateQueueNameRequired() {
        allowed(Validator::validateQueueNameRequired, PLAIN, HAS_DASH, HAS_DOT, HAS_STAR, HAS_GT);
        notAllowed(Validator::validateQueueNameRequired, null, EMPTY, HAS_SPACE);
    }

    @Test
    public void testValidateReplyTo() {
        allowed(Validator::validateReplyToNullButNotEmpty, null, PLAIN, HAS_SPACE, HAS_DASH, HAS_DOT, HAS_STAR, HAS_GT);
        notAllowed(Validator::validateReplyToNullButNotEmpty, EMPTY);
    }

    @Test
    public void testValidateStreamName() {
        allowed(Validator::validateStreamName, null, EMPTY, PLAIN, HAS_SPACE, HAS_DASH);
        notAllowed(Validator::validateStreamName, HAS_DOT, HAS_STAR, HAS_GT);
    }

    @Test
    public void testValidateStreamNameRequired() {
        allowed(Validator::validateStreamNameRequired, PLAIN, HAS_SPACE, HAS_DASH);
        notAllowed(Validator::validateStreamNameRequired, null, EMPTY, HAS_DOT, HAS_STAR, HAS_GT);
    }

    @Test
    public void testValidateStreamNameOrEmptyAsNull() {
        allowed(Validator::validateStreamNameOrEmptyAsNull, PLAIN, HAS_SPACE, HAS_DASH);
        allowedEmptyAsNull(Validator::validateStreamNameOrEmptyAsNull, null, EMPTY);
        notAllowed(Validator::validateStreamNameOrEmptyAsNull, HAS_DOT, HAS_STAR, HAS_GT);
    }

    @Test
    public void testValidateDurableOrEmptyAsNull() {
        allowed(Validator::validateDurableOrEmptyAsNull, PLAIN, HAS_SPACE, HAS_DASH);
        allowedEmptyAsNull(Validator::validateDurableOrEmptyAsNull, null, EMPTY);
        notAllowed(Validator::validateDurableOrEmptyAsNull, HAS_DOT, HAS_STAR, HAS_GT);
    }

    @Test
    public void testValidateDurableRequired() {
        allowed(Validator::validateDurableRequired, PLAIN, HAS_SPACE, HAS_DASH);
        notAllowed(Validator::validateDurableRequired, null, EMPTY, HAS_DOT, HAS_STAR, HAS_GT);
        assertThrows(IllegalArgumentException.class, () -> validateDurableRequired(null, null));
        assertThrows(IllegalArgumentException.class, () -> validateDurableRequired(HAS_DOT, null));
        ConsumerConfiguration cc1 = ConsumerConfiguration.builder().build();
        assertThrows(IllegalArgumentException.class, () -> validateDurableRequired(null, cc1));
        ConsumerConfiguration cc2 = ConsumerConfiguration.builder().durable(HAS_DOT).build();
        assertThrows(IllegalArgumentException.class, () -> validateDurableRequired(null, cc2));
    }

    @Test
    public void testValidatePullBatchSize() {
        assertEquals(1, validatePullBatchSize(1));
        assertEquals(MAX_PULL_SIZE, validatePullBatchSize(MAX_PULL_SIZE));
        assertThrows(IllegalArgumentException.class, () -> validatePullBatchSize(0));
        assertThrows(IllegalArgumentException.class, () -> validatePullBatchSize(-1));
        assertThrows(IllegalArgumentException.class, () -> validatePullBatchSize(MAX_PULL_SIZE + 1));
    }

    @Test
    public void testValidateMaxConsumers() {
        assertEquals(1, validateMaxConsumers(1));
        assertEquals(-1, validateMaxConsumers(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxConsumers(0));
    }

    @Test
    public void testValidateMaxMessages() {
        assertEquals(1, validateMaxMessages(1));
        assertEquals(-1, validateMaxMessages(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessages(0));
    }

    @Test
    public void testValidateMaxBytes() {
        assertEquals(1, validateMaxBytes(1));
        assertEquals(-1, validateMaxBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxBytes(0));
    }

    @Test
    public void testValidateMaxMessageSize() {
        assertEquals(1, validateMaxMessageSize(1));
        assertEquals(-1, validateMaxMessageSize(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessageSize(0));
    }

    @Test
    public void testValidateNumberOfReplicas() {
        assertEquals(1, validateNumberOfReplicas(1));
        assertEquals(5, validateNumberOfReplicas(5));
        assertThrows(IllegalArgumentException.class, () -> validateNumberOfReplicas(-1));
        assertThrows(IllegalArgumentException.class, () -> validateNumberOfReplicas(0));
        assertThrows(IllegalArgumentException.class, () -> validateNumberOfReplicas(7));
    }

    @Test
    public void testValidateDurationRequired() {
        assertEquals(Duration.ofNanos(1), validateDurationRequired(Duration.ofNanos(1)));
        assertEquals(Duration.ofSeconds(1), validateDurationRequired(Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> validateDurationRequired(null));
        assertThrows(IllegalArgumentException.class, () -> validateDurationRequired(Duration.ofNanos(0)));
        assertThrows(IllegalArgumentException.class, () -> validateDurationRequired(Duration.ofSeconds(0)));
        assertThrows(IllegalArgumentException.class, () -> validateDurationRequired(Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> validateDurationRequired(Duration.ofSeconds(-1)));
    }

    @Test
    public void testValidateDurationNotRequiredGtOrEqZero() {
        assertEquals(Duration.ZERO, validateDurationNotRequiredGtOrEqZero(null));
        assertEquals(Duration.ZERO, validateDurationNotRequiredGtOrEqZero(null));
        assertEquals(Duration.ZERO, validateDurationNotRequiredGtOrEqZero(Duration.ZERO));
        assertEquals(Duration.ZERO, validateDurationNotRequiredGtOrEqZero(Duration.ZERO));
        assertEquals(Duration.ofNanos(1), validateDurationNotRequiredGtOrEqZero(Duration.ofNanos(1)));
        assertEquals(Duration.ofSeconds(1), validateDurationNotRequiredGtOrEqZero(Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> validateDurationNotRequiredGtOrEqZero(Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> validateDurationNotRequiredGtOrEqZero(Duration.ofSeconds(-1)));
    }

    @Test
    public void testValidateJetStreamPrefix() {
        assertThrows(IllegalArgumentException.class, () -> validateJetStreamPrefix(HAS_STAR));
        assertThrows(IllegalArgumentException.class, () -> validateJetStreamPrefix(HAS_GT));
        assertThrows(IllegalArgumentException.class, () -> validateJetStreamPrefix(HAS_DOLLAR));
        assertThrows(IllegalArgumentException.class, () -> validateJetStreamPrefix(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> validateJetStreamPrefix(HAS_TAB));
    }

    @Test
    public void testNotNull() {
        final Object o1 = null;
        final String s1 = null;
        assertThrows(IllegalArgumentException.class, () -> validateNotNull(o1, "fieldName"));
        assertThrows(IllegalArgumentException.class, () -> validateNotNull(s1, "fieldName"));
        final Object o2 = new Object();
        final String s2 = "";
        assertEquals(o2, validateNotNull(o2, "fieldName"));
        assertEquals(s2, validateNotNull(s2, "fieldName"));
    }

    @Test
    public void testZeroOrLtMinus1() {
        assertTrue(zeroOrLtMinus1(0));
        assertTrue(zeroOrLtMinus1(-2));
        assertFalse(zeroOrLtMinus1(1));
        assertFalse(zeroOrLtMinus1(-1));
    }

    interface StringTest { String validate(String s); }

    private void allowed(StringTest test, String... strings) {
        for (String s : strings) {
            assertEquals(s, test.validate(s), allowedMessage(s));
        }
    }

    private void allowedEmptyAsNull(StringTest test, String... strings) {
        for (String s : strings) {
            assertNull(test.validate(s), allowedMessage(s));
        }
    }

    private void notAllowed(StringTest test, String... strings) {
        for (String s : strings) {
            assertThrows(IllegalArgumentException.class, () -> test.validate(s), notAllowedMessage(s));
        }
    }

    private String allowedMessage(String s) {
        return "Testing [" + s + "] as allowed.";
    }

    private String notAllowedMessage(String s) {
        return "Testing [" + s + "] as not allowed.";
    }
}