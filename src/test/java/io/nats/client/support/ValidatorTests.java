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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.support.NatsJetStreamConstants.MAX_PULL_SIZE;
import static io.nats.client.support.Validator.*;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

public class ValidatorTests {
    private static List<String> UTF_ONLY_STRINGS;

    @BeforeAll
    public static void beforeAll() {
        UTF_ONLY_STRINGS = dataAsLines("utf8-only-no-ws-test-strings.txt");
    }

    @Test
    public void testValidateSubject() {
        allowedRequired(Validator::validateSubject, Arrays.asList(PLAIN, HAS_PRINTABLE, HAS_DOT, HAS_STAR, HAS_GT, HAS_DOLLAR));
        notAllowedRequired(Validator::validateSubject, Arrays.asList(null, EMPTY, HAS_SPACE, HAS_LOW, HAS_127));
        notAllowedRequired(Validator::validateSubject, UTF_ONLY_STRINGS);
        allowedNotRequiredEmptyAsNull(Validator::validateSubject, Arrays.asList(null, EMPTY));

        notAllowedRequired(Validator::validateSubject, Arrays.asList(null, EMPTY, HAS_SPACE, HAS_LOW, HAS_127));
        allowedNotRequiredEmptyAsNull(Validator::validateSubject, Arrays.asList(null, EMPTY));
    }

    @Test
    public void testValidateReplyTo() {
        allowedRequired(Validator::validateReplyTo, Arrays.asList(PLAIN, HAS_PRINTABLE, HAS_DOT, HAS_DOLLAR));
        notAllowedRequired(Validator::validateReplyTo, Arrays.asList(null, EMPTY, HAS_SPACE, HAS_STAR, HAS_GT, HAS_LOW, HAS_127));
        notAllowedRequired(Validator::validateReplyTo, UTF_ONLY_STRINGS);
        allowedNotRequiredEmptyAsNull(Validator::validateReplyTo, Arrays.asList(null, EMPTY));
    }

    @Test
    public void testValidateQueueName() {
        // validateQueueName(String s, boolean required)
        allowedRequired(Validator::validateQueueName, Arrays.asList(PLAIN, HAS_PRINTABLE, HAS_DOLLAR));
        notAllowedRequired(Validator::validateQueueName, Arrays.asList(null, EMPTY, HAS_SPACE, HAS_DOT, HAS_STAR, HAS_GT, HAS_LOW, HAS_127));
        notAllowedRequired(Validator::validateQueueName, UTF_ONLY_STRINGS);
        allowedNotRequiredEmptyAsNull(Validator::validateQueueName, Arrays.asList(null, EMPTY));
    }

    @Test
    public void testValidateStreamName() {
        allowedRequired(Validator::validateStreamName, Arrays.asList(PLAIN, HAS_PRINTABLE, HAS_DOLLAR));
        notAllowedRequired(Validator::validateStreamName, Arrays.asList(null, EMPTY, HAS_SPACE, HAS_DOT, HAS_STAR, HAS_GT, HAS_LOW, HAS_127));
        notAllowedRequired(Validator::validateStreamName, UTF_ONLY_STRINGS);
        allowedNotRequiredEmptyAsNull(Validator::validateStreamName, Arrays.asList(null, EMPTY));
    }

    @Test
    public void testValidateDurable() {
        allowedRequired(Validator::validateDurable, Arrays.asList(PLAIN, HAS_PRINTABLE, HAS_DOLLAR));
        notAllowedRequired(Validator::validateDurable, Arrays.asList(null, EMPTY, HAS_SPACE, HAS_DOT, HAS_STAR, HAS_GT, HAS_LOW, HAS_127));
        notAllowedRequired(Validator::validateDurable, UTF_ONLY_STRINGS);
        allowedNotRequiredEmptyAsNull(Validator::validateDurable, Arrays.asList(null, EMPTY));
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
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessages(-2));
    }

    @Test
    public void testValidateMaxMessages() {
        assertEquals(1, validateMaxMessages(1));
        assertEquals(-1, validateMaxMessages(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessages(0));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessages(-2));
    }

    @Test
    public void testValidateMaxBucketValues() {
        assertEquals(1, validateMaxBucketValues(1));
        assertEquals(-1, validateMaxBucketValues(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxBucketValues(0));
        assertThrows(IllegalArgumentException.class, () -> validateMaxBucketValues(-2));
    }

    @Test
    public void testValidateMaxMessagesPerSubject() {
        assertEquals(1, validateMaxMessagesPerSubject(1));
        assertEquals(-1, validateMaxMessagesPerSubject(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessagesPerSubject(0));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessagesPerSubject(-2));
    }

    @Test
    public void testValidateMaxHistory() {
        assertEquals(1, validateMaxHistory(1));
        assertEquals(1, validateMaxHistory(0));
        assertEquals(1, validateMaxHistory(-1));
        assertEquals(1, validateMaxHistory(-2));
        assertEquals(64, validateMaxHistory(64));
        assertThrows(IllegalArgumentException.class, () -> validateMaxHistory(65));
    }

    @Test
    public void testValidateMaxBytes() {
        assertEquals(1, validateMaxBytes(1));
        assertEquals(-1, validateMaxBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxBytes(0));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessages(-2));
    }

    @Test
    public void testValidateMaxBucketBytes() {
        assertEquals(1, validateMaxBucketBytes(1));
        assertEquals(-1, validateMaxBucketBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxBucketBytes(0));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessages(-2));
    }

    @Test
    public void testValidateMaxMessageSize() {
        assertEquals(1, validateMaxMessageSize(1));
        assertEquals(-1, validateMaxMessageSize(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessageSize(0));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessages(-2));
    }

    @Test
    public void testValidateMaxValueBytes() {
        assertEquals(1, validateMaxValueBytes(1));
        assertEquals(-1, validateMaxValueBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxValueBytes(0));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessages(-2));
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
        Duration ifNull = Duration.ofMillis(999);
        assertEquals(ifNull, validateDurationNotRequiredGtOrEqZero(null, ifNull));
        assertEquals(Duration.ZERO, validateDurationNotRequiredGtOrEqZero(Duration.ZERO, ifNull));
        assertEquals(Duration.ofNanos(1), validateDurationNotRequiredGtOrEqZero(Duration.ofNanos(1), ifNull));
        assertThrows(IllegalArgumentException.class, () -> validateDurationNotRequiredGtOrEqZero(Duration.ofNanos(-1), ifNull));

        assertEquals(Duration.ZERO, validateDurationNotRequiredGtOrEqZero(0));
        assertEquals(Duration.ofMillis(1), validateDurationNotRequiredGtOrEqZero(1));
        assertEquals(Duration.ofSeconds(1), validateDurationNotRequiredGtOrEqZero(1000));
        assertThrows(IllegalArgumentException.class, () -> validateDurationNotRequiredGtOrEqZero(-1));
    }

    @Test
    public void testValidateDurationNotRequiredNotLessThanMin() {
        Duration min = Duration.ofMillis(99);
        Duration less = Duration.ofMillis(9);
        Duration more = Duration.ofMillis(9999);

        assertNull(validateDurationNotRequiredNotLessThanMin(null, min));
        assertEquals(more, validateDurationNotRequiredNotLessThanMin(more, min));

        assertThrows(IllegalArgumentException.class, () -> validateDurationNotRequiredNotLessThanMin(less, min));
    }

    @Test
    public void testValidateGtEqZero() {
        assertEquals(0, validateGtEqZero(0, "test"));
        assertEquals(1, validateGtEqZero(1, "test"));
        assertThrows(IllegalArgumentException.class, () -> validateGtEqZero(-1, "test"));
    }

    @Test
    public void testEnsureDuration() {
        assertEquals(Duration.ofMillis(10), ensureNotNullAndNotLessThanMin(null, Duration.ofMillis(2), Duration.ofMillis(10)));
        assertEquals(Duration.ofMillis(10), ensureNotNullAndNotLessThanMin(Duration.ofMillis(1), Duration.ofMillis(2), Duration.ofMillis(10)));
        assertEquals(Duration.ofMillis(100), ensureNotNullAndNotLessThanMin(Duration.ofMillis(100), Duration.ofMillis(2), Duration.ofMillis(10)));
        assertEquals(Duration.ofMillis(10), ensureDurationNotLessThanMin(1, Duration.ofMillis(2), Duration.ofMillis(10)));
        assertEquals(Duration.ofMillis(100), ensureDurationNotLessThanMin(100, Duration.ofMillis(2), Duration.ofMillis(10)));
    }

    @Test
    public void testValidateBucketNameRequired() {
        validateKvBucketNameRequired(PLAIN);
        validateKvBucketNameRequired(PLAIN.toUpperCase());
        validateKvBucketNameRequired(HAS_DASH);
        validateKvBucketNameRequired(HAS_UNDER);
        validateKvBucketNameRequired("numbers9ok");
        assertThrows(IllegalArgumentException.class, () -> validateKvBucketNameRequired(null));
        assertThrows(IllegalArgumentException.class, () -> validateKvBucketNameRequired(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> validateKvBucketNameRequired(HAS_DOT));
        assertThrows(IllegalArgumentException.class, () -> validateKvBucketNameRequired(HAS_STAR));
        assertThrows(IllegalArgumentException.class, () -> validateKvBucketNameRequired(HAS_GT));
        assertThrows(IllegalArgumentException.class, () -> validateKvBucketNameRequired(HAS_DOLLAR));
        assertThrows(IllegalArgumentException.class, () -> validateKvBucketNameRequired(HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> validateKvBucketNameRequired(HAS_127));
        assertThrows(IllegalArgumentException.class, () -> validateKvBucketNameRequired(HAS_FWD_SLASH));
        assertThrows(IllegalArgumentException.class, () -> validateKvBucketNameRequired(HAS_EQUALS));
        assertThrows(IllegalArgumentException.class, () -> validateKvBucketNameRequired(HAS_TIC));
    }

    @Test
    public void testValidateWildcardKeyRequired() {
        validateWildcardKvKeyRequired(PLAIN);
        validateWildcardKvKeyRequired(PLAIN.toUpperCase());
        validateWildcardKvKeyRequired(HAS_DASH);
        validateWildcardKvKeyRequired(HAS_UNDER);
        validateWildcardKvKeyRequired(HAS_FWD_SLASH);
        validateWildcardKvKeyRequired(HAS_EQUALS);
        validateWildcardKvKeyRequired(HAS_DOT);
        validateWildcardKvKeyRequired(HAS_STAR);
        validateWildcardKvKeyRequired(HAS_GT);
        validateWildcardKvKeyRequired("numbers9ok");
        assertThrows(IllegalArgumentException.class, () -> validateWildcardKvKeyRequired(null));
        assertThrows(IllegalArgumentException.class, () -> validateWildcardKvKeyRequired(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> validateWildcardKvKeyRequired(HAS_DOLLAR));
        assertThrows(IllegalArgumentException.class, () -> validateWildcardKvKeyRequired(HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> validateWildcardKvKeyRequired(HAS_127));
        assertThrows(IllegalArgumentException.class, () -> validateWildcardKvKeyRequired(HAS_TIC));
        assertThrows(IllegalArgumentException.class, () -> validateWildcardKvKeyRequired("colon:isbetween9andA"));
        assertThrows(IllegalArgumentException.class, () -> validateWildcardKvKeyRequired(".starts.with.dot.not.allowed"));
    }

    @Test
    public void testValidateNonWildcardKeyRequired() {
        validateNonWildcardKvKeyRequired(PLAIN);
        validateNonWildcardKvKeyRequired(PLAIN.toUpperCase());
        validateNonWildcardKvKeyRequired(HAS_DASH);
        validateNonWildcardKvKeyRequired(HAS_UNDER);
        validateNonWildcardKvKeyRequired(HAS_FWD_SLASH);
        validateNonWildcardKvKeyRequired(HAS_EQUALS);
        validateNonWildcardKvKeyRequired(HAS_DOT);
        validateNonWildcardKvKeyRequired("numbers9ok");
        assertThrows(IllegalArgumentException.class, () -> validateNonWildcardKvKeyRequired(null));
        assertThrows(IllegalArgumentException.class, () -> validateNonWildcardKvKeyRequired(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> validateNonWildcardKvKeyRequired(HAS_STAR));
        assertThrows(IllegalArgumentException.class, () -> validateNonWildcardKvKeyRequired(HAS_GT));
        assertThrows(IllegalArgumentException.class, () -> validateNonWildcardKvKeyRequired(HAS_DOLLAR));
        assertThrows(IllegalArgumentException.class, () -> validateNonWildcardKvKeyRequired(HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> validateNonWildcardKvKeyRequired(HAS_127));
        assertThrows(IllegalArgumentException.class, () -> validateNonWildcardKvKeyRequired(HAS_TIC));
        assertThrows(IllegalArgumentException.class, () -> validateNonWildcardKvKeyRequired("colon:isbetween9andA"));
        assertThrows(IllegalArgumentException.class, () -> validateNonWildcardKvKeyRequired(".starts.with.dot.not.allowed"));
    }

    @Test
    public void testValidateMustMatchIfBothSupplied() {
        NatsJetStreamClientError err = new NatsJetStreamClientError("TEST", 999999, "desc");
        assertNull(validateMustMatchIfBothSupplied(null, null, err));
        assertEquals("y", validateMustMatchIfBothSupplied(null, "y", err));
        assertEquals("y", validateMustMatchIfBothSupplied("", "y", err));
        assertEquals("x", validateMustMatchIfBothSupplied("x", null, err));
        assertEquals("x", validateMustMatchIfBothSupplied("x", " ", err));
        assertEquals("x", validateMustMatchIfBothSupplied("x", "x", err));
        assertThrows(IllegalArgumentException.class, () -> validateMustMatchIfBothSupplied("x", "y", err));
    }

    @Test
    public void testValidateMaxLength() {
        validateMaxLength("test", 5, true, "label");
        validateMaxLength(null, 5, false, "label");
        assertThrows(IllegalArgumentException.class, () -> validateMaxLength("test", 3, true, "label"));
        assertThrows(IllegalArgumentException.class, () -> validateMaxLength(null, 5, true, "label"));
    }

    @Test
    public void testNotNull() {
        Object o = new Object();
        validateNotNull(o, "fieldName");
        assertThrows(IllegalArgumentException.class, () -> validateNotNull(null, "fieldName"));
    }

    @Test
    public void testZeroOrLtMinus1() {
        assertTrue(zeroOrLtMinus1(0));
        assertTrue(zeroOrLtMinus1(-2));
        assertFalse(zeroOrLtMinus1(1));
        assertFalse(zeroOrLtMinus1(-1));
    }

    @Test
    public void testValidateGtZeroOrMinus1() {
        assertEquals(1, validateGtZeroOrMinus1(1, "test"));
        assertEquals(-1, validateGtZeroOrMinus1(-1, "test"));
        assertThrows(IllegalArgumentException.class, () -> validateGtZeroOrMinus1(0, "test"));
    }

    @Test
    public void testValidateGtEqMinus1() {
        assertEquals(1, validateGtEqMinus1(1, "test"));
        assertEquals(0, validateGtEqMinus1(0, "test"));
        assertEquals(-1, validateGtEqMinus1(-1, "test"));
        assertThrows(IllegalArgumentException.class, () -> validateGtEqMinus1(-2, "test"));
    }

    @Test
    public void testValidateNotNegative() {
        assertEquals(0, validateNotNegative(0, "test"));
        assertEquals(1, validateNotNegative(1, "test"));
        assertThrows(IllegalArgumentException.class, () -> validateNotNegative(-1, "test"));
    }

    @Test
    public void testEmptyAsNull() {
        assertEquals("test", emptyAsNull("test"));
        assertNull(emptyAsNull(null));
        assertNull(emptyAsNull(""));
        assertNull(emptyAsNull(" "));
        assertNull(emptyAsNull("\t"));
    }

    @Test
    public void testEmptyOrNullAs() {
        assertEquals("test", emptyOrNullAs("test", null));
        assertNull(emptyOrNullAs(null, null));
        assertNull(emptyOrNullAs("", null));
        assertNull(emptyOrNullAs(" ", null));
        assertNull(emptyOrNullAs("\t", null));

        assertEquals("test", emptyOrNullAs("test", "as"));
        assertEquals("as", emptyOrNullAs(null, "as"));
        assertEquals("as", emptyOrNullAs("", "as"));
        assertEquals("as", emptyOrNullAs(" ", "as"));
        assertEquals("as", emptyOrNullAs("\t", "as"));
    }

    interface StringTest { String validate(String s, boolean required); }

    private void allowedRequired(StringTest test, List<String> strings) {
        for (String s : strings) {
            assertEquals(s, test.validate(s, true), allowedMessage(s));
        }
    }

    private void allowedNotRequiredEmptyAsNull(StringTest test, List<String> strings) {
        for (String s : strings) {
            assertNull(test.validate(s, false), allowedMessage(s));
        }
    }

    private void notAllowedRequired(StringTest test, List<String> strings) {
        for (String s : strings) {
            assertThrows(IllegalArgumentException.class, () -> test.validate(s, true), notAllowedMessage(s));
        }
    }

    private String allowedMessage(String s) {
        return "Testing [" + s + "] as allowed.";
    }

    private String notAllowedMessage(String s) {
        return "Testing [" + s + "] as not allowed.";
    }

    @Test
    public void testNatsJetStreamClientError() {
        // coverage
        NatsJetStreamClientError err = new NatsJetStreamClientError("TEST", 999999, "desc");
        assertEquals("[TEST-999999] desc", err.message());
    }
}