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
import java.util.*;

import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.support.NatsJetStreamConstants.NATS_META_KEY_PREFIX;
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
        // subject is required
        allowedRequired(Validator::validateSubject, Arrays.asList(PLAIN, HAS_PRINTABLE, HAS_DOT, HAS_DOLLAR, HAS_LOW, HAS_127));
        allowedRequired(Validator::validateSubject, UTF_ONLY_STRINGS);
        allowedRequired(Validator::validateSubject, Arrays.asList(STAR_SEGMENT, GT_LAST_SEGMENT));
        notAllowedRequired(Validator::validateSubject, Arrays.asList(null, EMPTY, HAS_SPACE, HAS_CR, HAS_LF));
        notAllowedRequired(Validator::validateSubject, Arrays.asList(STARTS_WITH_DOT, STAR_NOT_SEGMENT, GT_NOT_SEGMENT, EMPTY_SEGMENT, GT_NOT_LAST_SEGMENT));
        notAllowedRequired(Validator::validateSubject, Arrays.asList(ENDS_WITH_DOT, ENDS_WITH_DOT_SPACE, ENDS_WITH_CR, ENDS_WITH_LF, ENDS_WITH_TAB));

        // subject not required, null and empty both mean not supplied
        allowedNotRequiredEmptyAsNull(Validator::validateSubject, Arrays.asList(null, EMPTY));
        allowedNotRequired(Validator::validateSubject, Arrays.asList(PLAIN, HAS_PRINTABLE, HAS_DOT, HAS_DOLLAR, HAS_LOW, HAS_127));
        allowedNotRequired(Validator::validateSubject, UTF_ONLY_STRINGS);
        allowedNotRequired(Validator::validateSubject, Arrays.asList(STAR_SEGMENT, GT_LAST_SEGMENT));
        notAllowedNotRequired(Validator::validateSubject, Arrays.asList(HAS_SPACE, HAS_CR, HAS_LF));
        notAllowedNotRequired(Validator::validateSubject, Arrays.asList(STARTS_WITH_DOT, STAR_NOT_SEGMENT, GT_NOT_SEGMENT, EMPTY_SEGMENT, GT_NOT_LAST_SEGMENT));
        notAllowedNotRequired(Validator::validateSubject, Arrays.asList(ENDS_WITH_DOT, ENDS_WITH_DOT_SPACE, ENDS_WITH_CR, ENDS_WITH_LF, ENDS_WITH_TAB));
    }

    @Test
    public void testValidateReplyTo() {
        allowedRequired(Validator::validateReplyTo, Arrays.asList(PLAIN, HAS_PRINTABLE, HAS_DOT, HAS_DOLLAR));
        notAllowedRequired(Validator::validateReplyTo, Arrays.asList(null, EMPTY, HAS_SPACE, STAR_NOT_SEGMENT, GT_NOT_SEGMENT, HAS_LOW, HAS_127));
        notAllowedRequired(Validator::validateReplyTo, UTF_ONLY_STRINGS);
        allowedNotRequiredEmptyAsNull(Validator::validateReplyTo, Arrays.asList(null, EMPTY));
    }

    @Test
    public void testValidateQueueName() {
        // validateQueueName(String s, boolean required)
        allowedRequired(Validator::validateQueueName, Arrays.asList(PLAIN, HAS_PRINTABLE, HAS_DOLLAR, HAS_DOT, HAS_LOW, HAS_127));
        notAllowedRequired(Validator::validateQueueName, Arrays.asList(null, EMPTY, HAS_SPACE, STAR_NOT_SEGMENT, GT_NOT_SEGMENT));
        allowedRequired(Validator::validateQueueName, UTF_ONLY_STRINGS);
        allowedNotRequiredEmptyAsNull(Validator::validateQueueName, Arrays.asList(null, EMPTY));
    }

    @Test
    public void testValidateStreamName() {
        allowedRequired(Validator::validateStreamName, Arrays.asList(PLAIN, HAS_PRINTABLE, HAS_DOLLAR));
        notAllowedRequired(Validator::validateStreamName, Arrays.asList(null, EMPTY, HAS_SPACE, HAS_DOT, STAR_NOT_SEGMENT, GT_NOT_SEGMENT, HAS_LOW, HAS_127));
        notAllowedRequired(Validator::validateStreamName, UTF_ONLY_STRINGS);
        allowedNotRequiredEmptyAsNull(Validator::validateStreamName, Arrays.asList(null, EMPTY));
    }

    @Test
    public void testValidateDurable() {
        allowedRequired(Validator::validateDurable, Arrays.asList(PLAIN, HAS_PRINTABLE, HAS_DOLLAR));
        notAllowedRequired(Validator::validateDurable, Arrays.asList(null, EMPTY, HAS_SPACE, HAS_DOT, STAR_NOT_SEGMENT, GT_NOT_SEGMENT, HAS_LOW, HAS_127));
        notAllowedRequired(Validator::validateDurable, UTF_ONLY_STRINGS);
        allowedNotRequiredEmptyAsNull(Validator::validateDurable, Arrays.asList(null, EMPTY));
    }

    @Test
    public void testValidatePrintable() {
        validatePrintable(PLAIN, "label", true);
        validatePrintable(HAS_PRINTABLE, "label", true);
        validatePrintable(HAS_DOT, "label", true);
        validatePrintable(STAR_NOT_SEGMENT, "label", true);
        validatePrintable(GT_NOT_SEGMENT, "label", true);
        validatePrintable(HAS_DASH, "label", true);
        validatePrintable(HAS_UNDER, "label", true);
        validatePrintable(HAS_DOLLAR, "label", true);
        validatePrintable(HAS_FWD_SLASH, "label", true);
        validatePrintable(HAS_BACK_SLASH, "label", true);
        validatePrintable(HAS_EQUALS, "label", true);
        validatePrintable(HAS_TIC, "label", true);

        assertThrows(IllegalArgumentException.class, () -> validatePrintable(HAS_SPACE, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintable(HAS_127, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintable(HAS_LOW, "label", true));

        validatePrintableExceptWildDotGt(PLAIN, "label", true);
        validatePrintableExceptWildDotGt(HAS_PRINTABLE, "label", true);
        validatePrintableExceptWildDotGt(HAS_DASH, "label", true);
        validatePrintableExceptWildDotGt(HAS_UNDER, "label", true);
        validatePrintableExceptWildDotGt(HAS_DOLLAR, "label", true);
        validatePrintableExceptWildDotGt(HAS_FWD_SLASH, "label", true);
        validatePrintableExceptWildDotGt(HAS_BACK_SLASH, "label", true);
        validatePrintableExceptWildDotGt(HAS_EQUALS, "label", true);
        validatePrintableExceptWildDotGt(HAS_TIC, "label", true);

        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGt(HAS_DOT, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGt(STAR_NOT_SEGMENT, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGt(GT_NOT_SEGMENT, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGt(HAS_SPACE, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGt(HAS_127, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGt(HAS_LOW, "label", true));

        validatePrintableExceptWildDotGtSlashes(PLAIN, "label", true);
        validatePrintableExceptWildDotGtSlashes(HAS_PRINTABLE, "label", true);
        validatePrintableExceptWildDotGtSlashes(HAS_DASH, "label", true);
        validatePrintableExceptWildDotGtSlashes(HAS_UNDER, "label", true);
        validatePrintableExceptWildDotGtSlashes(HAS_DOLLAR, "label", true);
        validatePrintableExceptWildDotGtSlashes(HAS_EQUALS, "label", true);
        validatePrintableExceptWildDotGtSlashes(HAS_TIC, "label", true);

        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGtSlashes(HAS_FWD_SLASH, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGtSlashes(HAS_BACK_SLASH, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGtSlashes(HAS_DOT, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGtSlashes(STAR_NOT_SEGMENT, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGtSlashes(GT_NOT_SEGMENT, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGtSlashes(HAS_SPACE, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGtSlashes(HAS_127, "label", true));
        assertThrows(IllegalArgumentException.class, () -> validatePrintableExceptWildDotGtSlashes(HAS_LOW, "label", true));
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
    public void testValidateMaxMessagesPerSubject() {
        assertEquals(1, validateMaxMessagesPerSubject(1));
        assertEquals(-1, validateMaxMessagesPerSubject(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessagesPerSubject(0));
        assertThrows(IllegalArgumentException.class, () -> validateMaxMessagesPerSubject(-2));
    }

    @Test
    public void testValidateMaxHistory() {
        assertEquals(1, validateMaxHistory(1));
        assertEquals(64, validateMaxHistory(64));
        assertThrows(IllegalArgumentException.class, () -> validateMaxHistory(0));
        assertThrows(IllegalArgumentException.class, () -> validateMaxHistory(-1));
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
        assertEquals(1, validateMaxValueSize(1));
        assertEquals(-1, validateMaxValueSize(-1));
        assertThrows(IllegalArgumentException.class, () -> validateMaxValueSize(0));
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
    public void testValidateDurationGtOrEqSeconds() {
        Duration ifNull = Duration.ofMillis(999);
        assertEquals(ifNull, validateDurationNotRequiredGtOrEqSeconds(1, null, ifNull, ""));
        assertEquals(Duration.ofSeconds(1), validateDurationNotRequiredGtOrEqSeconds(1, Duration.ofSeconds(1), ifNull, ""));
        assertThrows(IllegalArgumentException.class, () -> validateDurationNotRequiredGtOrEqSeconds(1, Duration.ofMillis(999), ifNull, ""));

        assertEquals(Duration.ofSeconds(1), validateDurationGtOrEqSeconds(1, 1000, ""));
        assertThrows(IllegalArgumentException.class, () -> validateDurationGtOrEqSeconds(1, 999, ""));
    }

    @Test
    public void testIsGtEqZero() {
        assertTrue(Validator.isGtEqZero(0));
        assertTrue(Validator.isGtEqZero(1));
        assertFalse(Validator.isGtEqZero(-1));
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
    public void testValidateBucketName() {
        validateBucketName(PLAIN, true);
        validateBucketName(PLAIN.toUpperCase(), true);
        validateBucketName(HAS_DASH, true);
        validateBucketName(HAS_UNDER, true);
        validateBucketName("numbers9ok", true);
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(null, true));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_SPACE, true));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_DOT, true));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(STAR_NOT_SEGMENT, true));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(GT_NOT_SEGMENT, true));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_DOLLAR, true));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_LOW, true));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_127, true));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_FWD_SLASH, true));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_EQUALS, true));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_TIC, true));

        validateBucketName(PLAIN, false);
        validateBucketName(PLAIN.toUpperCase(), false);
        validateBucketName(HAS_DASH, false);
        validateBucketName(HAS_UNDER, false);
        validateBucketName("numbers9ok", false);
        validateBucketName(null, false);
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_SPACE, false));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_DOT, false));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(STAR_NOT_SEGMENT, false));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(GT_NOT_SEGMENT, false));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_DOLLAR, false));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_LOW, false));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_127, false));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_FWD_SLASH, false));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_EQUALS, false));
        assertThrows(IllegalArgumentException.class, () -> validateBucketName(HAS_TIC, false));
    }

    @Test
    public void testValidateKvKeyWildcardAllowedRequired() {
        validateKvKeyWildcardAllowedRequired(PLAIN);
        validateKvKeyWildcardAllowedRequired(PLAIN.toUpperCase());
        validateKvKeyWildcardAllowedRequired(HAS_DASH);
        validateKvKeyWildcardAllowedRequired(HAS_UNDER);
        validateKvKeyWildcardAllowedRequired(HAS_FWD_SLASH);
        validateKvKeyWildcardAllowedRequired(HAS_EQUALS);
        validateKvKeyWildcardAllowedRequired(HAS_DOT);
        validateKvKeyWildcardAllowedRequired(STAR_NOT_SEGMENT);
        validateKvKeyWildcardAllowedRequired(GT_NOT_SEGMENT);
        validateKvKeyWildcardAllowedRequired("numbers9ok");
        String nullKey = null;
        assertThrows(IllegalArgumentException.class, () -> validateKvKeyWildcardAllowedRequired(nullKey));
        assertThrows(IllegalArgumentException.class, () -> validateKvKeyWildcardAllowedRequired(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> validateKvKeyWildcardAllowedRequired(HAS_DOLLAR));
        assertThrows(IllegalArgumentException.class, () -> validateKvKeyWildcardAllowedRequired(HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> validateKvKeyWildcardAllowedRequired(HAS_127));
        assertThrows(IllegalArgumentException.class, () -> validateKvKeyWildcardAllowedRequired(HAS_TIC));
        assertThrows(IllegalArgumentException.class, () -> validateKvKeyWildcardAllowedRequired("colon:isbetween9andA"));
        assertThrows(IllegalArgumentException.class, () -> validateKvKeyWildcardAllowedRequired(".starts.with.dot.not.allowed"));

        List<String> nullList = null;
        //noinspection ConstantValue
        assertThrows(IllegalArgumentException.class, () -> validateKvKeysWildcardAllowedRequired(nullList));
        assertThrows(IllegalArgumentException.class, () -> validateKvKeysWildcardAllowedRequired(Collections.singletonList(null)));
        assertThrows(IllegalArgumentException.class, () -> validateKvKeysWildcardAllowedRequired(Collections.singletonList(HAS_SPACE)));
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
        assertThrows(IllegalArgumentException.class, () -> validateNonWildcardKvKeyRequired(STAR_NOT_SEGMENT));
        assertThrows(IllegalArgumentException.class, () -> validateNonWildcardKvKeyRequired(GT_NOT_SEGMENT));
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

    @SuppressWarnings({"ObviousNullCheck", "rawtypes"})
    @Test
    public void testValidateRequired() {
        required("required", "label");
        required("required1", "required2", "label");
        required(new Object(), "label");
        required(Collections.singletonList("list"), "label");
        required(Collections.singletonMap("key", "value"), "label");

        assertThrows(IllegalArgumentException.class, () -> required((String)null, "label"));
        assertThrows(IllegalArgumentException.class, () -> required("no-second", null, "label"));
        assertThrows(IllegalArgumentException.class, () -> required(null, "no-first", "label"));
        assertThrows(IllegalArgumentException.class, () -> required(EMPTY, "label"));
        assertThrows(IllegalArgumentException.class, () -> required((Object)null, "label"));
        assertThrows(IllegalArgumentException.class, () -> required((List)null, "label"));
        assertThrows(IllegalArgumentException.class, () -> required(new ArrayList<>(), "label"));
        assertThrows(IllegalArgumentException.class, () -> required((Map)null, "label"));
        assertThrows(IllegalArgumentException.class, () -> required(new HashMap<>(), "label"));
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
    public void testValidateGtZero() {
        assertEquals(1, validateGtZero(1, "test"));
        assertThrows(IllegalArgumentException.class, () -> validateGtZero(0, "test"));
        assertThrows(IllegalArgumentException.class, () -> validateGtZero(-1, "test"));
        assertEquals(1, validateGtZero(1L, "test"));
        assertThrows(IllegalArgumentException.class, () -> validateGtZero(0L, "test"));
        assertThrows(IllegalArgumentException.class, () -> validateGtZero(-1L, "test"));
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

    private void notAllowedRequired(StringTest test, List<String> strings) {
        for (String s : strings) {
            assertThrows(IllegalArgumentException.class, () -> test.validate(s, true), notAllowedMessage(s));
        }
    }

    private void allowedNotRequired(StringTest test, List<String> strings) {
        for (String s : strings) {
            assertEquals(s, test.validate(s, false), allowedMessage(s));
        }
    }

    private void allowedNotRequiredEmptyAsNull(StringTest test, List<String> strings) {
        for (String s : strings) {
            assertNull(test.validate(s, false), allowedMessage(s));
        }
    }

    private void notAllowedNotRequired(StringTest test, List<String> strings) {
        for (String s : strings) {
            assertThrows(IllegalArgumentException.class, () -> test.validate(s, false), notAllowedMessage(s));
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

    @Test
    public void testSemver() {
        String label = "Version";
        validateSemVer("0.0.4", label, true);
        validateSemVer("1.2.3", label, true);
        validateSemVer("10.20.30", label, true);
        validateSemVer("1.1.2-prerelease+meta", label, true);
        validateSemVer("1.1.2+meta", label, true);
        validateSemVer("1.1.2+meta-valid", label, true);
        validateSemVer("1.0.0-alpha", label, true);
        validateSemVer("1.0.0-beta", label, true);
        validateSemVer("1.0.0-alpha.beta", label, true);
        validateSemVer("1.0.0-alpha.beta.1", label, true);
        validateSemVer("1.0.0-alpha.1", label, true);
        validateSemVer("1.0.0-alpha0.valid", label, true);
        validateSemVer("1.0.0-alpha.0valid", label, true);
        validateSemVer("1.0.0-alpha-a.b-c-somethinglong+build.1-aef.1-its-okay", label, true);
        validateSemVer("1.0.0-rc.1+build.1", label, true);
        validateSemVer("2.0.0-rc.1+build.123", label, true);
        validateSemVer("1.2.3-beta", label, true);
        validateSemVer("10.2.3-DEV-SNAPSHOT", label, true);
        validateSemVer("1.2.3-SNAPSHOT-123", label, true);
        validateSemVer("1.0.0", label, true);
        validateSemVer("2.0.0", label, true);
        validateSemVer("1.1.7", label, true);
        validateSemVer("2.0.0+build.1848", label, true);
        validateSemVer("2.0.1-alpha.1227", label, true);
        validateSemVer("1.0.0-alpha+beta", label, true);
        validateSemVer("1.2.3----RC-SNAPSHOT.12.9.1--.12+788", label, true);
        validateSemVer("1.2.3----R-S.12.9.1--.12+meta", label, true);
        validateSemVer("1.2.3----RC-SNAPSHOT.12.9.1--.12", label, true);
        validateSemVer("1.0.0+0.build.1-rc.10000aaa-kk-0.1", label, true);
        validateSemVer("99999999999999999999999.999999999999999999.99999999999999999", label, true);
        validateSemVer("1.0.0-0A.is.legal", label, true);

        assertNull(validateSemVer(null, label, false));
        assertNull(validateSemVer("", label, false));
        
        assertThrows(IllegalArgumentException.class, () -> validateSemVer(null, label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("", label, true));

        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.2", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.2.3-0123", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.2.3-0123.0123", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.1.2+.123", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("+invalid", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("-invalid", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("-invalid+invalid", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("-invalid.01", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("alpha", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("alpha.beta", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("alpha.beta.1", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("alpha.1", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("alpha+beta", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("alpha_beta", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("alpha.", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("alpha..", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("beta", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.0.0-alpha_beta", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("-alpha.", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.0.0-alpha..", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.0.0-alpha..1", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.0.0-alpha...1", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.0.0-alpha....1", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.0.0-alpha.....1", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.0.0-alpha......1", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.0.0-alpha.......1", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("01.1.1", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.01.1", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.1.01", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.2", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.2.3.DEV", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.2-SNAPSHOT", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.2.31.2.3----RC-SNAPSHOT.12.09.1--..12+788", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("1.2-RC-SNAPSHOT", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("-1.0.3-gamma+b7718", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("+justmeta", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("9.8.7+meta+meta", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("9.8.7-whatever+meta+meta", label, true));
        assertThrows(IllegalArgumentException.class, () -> validateSemVer("99999999999999999999999.999999999999999999.99999999999999999----RC-SNAPSHOT.12.09.1--------------------------------..12", label, true));
    }

    @Test
    public void testListsAreEquivalent() {
        List<String> l1 = Arrays.asList("one", "two");
        List<String> l2 = Arrays.asList("two", "one");
        List<String> l3 = Arrays.asList("one", "not");
        List<String> l4 = Collections.singletonList("three");
        List<String> l5 = null;
        List<String> l6 = new ArrayList<>();

        assertTrue(listsAreEquivalent(l1, l1));
        assertTrue(listsAreEquivalent(l1, l2));
        assertFalse(listsAreEquivalent(l1, l3));
        assertFalse(listsAreEquivalent(l1, l4));
        assertFalse(listsAreEquivalent(l1, l5));
        assertFalse(listsAreEquivalent(l1, l6));

        assertTrue(listsAreEquivalent(l2, l1));
        assertTrue(listsAreEquivalent(l2, l2));
        assertFalse(listsAreEquivalent(l2, l3));
        assertFalse(listsAreEquivalent(l2, l4));
        assertFalse(listsAreEquivalent(l2, l5));
        assertFalse(listsAreEquivalent(l2, l6));

        assertFalse(listsAreEquivalent(l3, l1));
        assertFalse(listsAreEquivalent(l3, l2));
        assertTrue(listsAreEquivalent(l3, l3));
        assertFalse(listsAreEquivalent(l3, l4));
        assertFalse(listsAreEquivalent(l3, l5));
        assertFalse(listsAreEquivalent(l3, l6));

        assertFalse(listsAreEquivalent(l4, l1));
        assertFalse(listsAreEquivalent(l4, l2));
        assertFalse(listsAreEquivalent(l4, l3));
        assertTrue(listsAreEquivalent(l4, l4));
        assertFalse(listsAreEquivalent(l4, l5));
        assertFalse(listsAreEquivalent(l4, l6));

        assertFalse(listsAreEquivalent(l5, l1));
        assertFalse(listsAreEquivalent(l5, l2));
        assertFalse(listsAreEquivalent(l5, l3));
        assertFalse(listsAreEquivalent(l5, l4));
        assertTrue(listsAreEquivalent(l5, l5));
        assertTrue(listsAreEquivalent(l5, l6));

        assertFalse(listsAreEquivalent(l6, l1));
        assertFalse(listsAreEquivalent(l6, l2));
        assertFalse(listsAreEquivalent(l6, l3));
        assertFalse(listsAreEquivalent(l6, l4));
        assertTrue(listsAreEquivalent(l6, l5));
        assertTrue(listsAreEquivalent(l6, l6));
    }

    @Test
    public void testMapsAreEqual() {
        Map<String, String> m1 = new HashMap<>();
        m1.put("one", "1");
        m1.put("two", "2");

        Map<String, String> m2 = new HashMap<>();
        m2.put("two", "2");
        m2.put("one", "1");

        Map<String, String> m3 = new HashMap<>();
        m3.put("one", "1");
        m3.put("two", "not");

        Map<String, String> m4 = new HashMap<>();
        m4.put("one", "1");
        m4.put("not", "not");

        Map<String, String> m5 = new HashMap<>();
        m5.put("five", "5");

        Map<String, String> m6 = null;

        Map<String, String> m7 = new HashMap<>();

        assertTrue(mapsAreEquivalent(m1, m1));
        assertTrue(mapsAreEquivalent(m1, m2));
        assertFalse(mapsAreEquivalent(m1, m3));
        assertFalse(mapsAreEquivalent(m1, m4));
        assertFalse(mapsAreEquivalent(m1, m5));
        assertFalse(mapsAreEquivalent(m1, m6));
        assertFalse(mapsAreEquivalent(m1, m7));

        assertTrue(mapsAreEquivalent(m2, m1));
        assertTrue(mapsAreEquivalent(m2, m2));
        assertFalse(mapsAreEquivalent(m2, m3));
        assertFalse(mapsAreEquivalent(m2, m4));
        assertFalse(mapsAreEquivalent(m2, m5));
        assertFalse(mapsAreEquivalent(m2, m6));
        assertFalse(mapsAreEquivalent(m2, m7));

        assertFalse(mapsAreEquivalent(m3, m1));
        assertFalse(mapsAreEquivalent(m3, m2));
        assertTrue(mapsAreEquivalent(m3, m3));
        assertFalse(mapsAreEquivalent(m3, m4));
        assertFalse(mapsAreEquivalent(m3, m5));
        assertFalse(mapsAreEquivalent(m3, m6));
        assertFalse(mapsAreEquivalent(m3, m7));

        assertFalse(mapsAreEquivalent(m4, m1));
        assertFalse(mapsAreEquivalent(m4, m2));
        assertFalse(mapsAreEquivalent(m4, m3));
        assertTrue(mapsAreEquivalent(m4, m4));
        assertFalse(mapsAreEquivalent(m4, m5));
        assertFalse(mapsAreEquivalent(m4, m6));
        assertFalse(mapsAreEquivalent(m4, m7));

        assertFalse(mapsAreEquivalent(m5, m1));
        assertFalse(mapsAreEquivalent(m5, m2));
        assertFalse(mapsAreEquivalent(m5, m3));
        assertFalse(mapsAreEquivalent(m5, m4));
        assertTrue(mapsAreEquivalent(m5, m5));
        assertFalse(mapsAreEquivalent(m5, m6));
        assertFalse(mapsAreEquivalent(m5, m7));

        assertFalse(mapsAreEquivalent(m6, m1));
        assertFalse(mapsAreEquivalent(m6, m2));
        assertFalse(mapsAreEquivalent(m6, m3));
        assertFalse(mapsAreEquivalent(m6, m4));
        assertFalse(mapsAreEquivalent(m6, m5));
        assertTrue(mapsAreEquivalent(m6, m6));
        assertTrue(mapsAreEquivalent(m6, m7));

        assertFalse(mapsAreEquivalent(m7, m1));
        assertFalse(mapsAreEquivalent(m7, m2));
        assertFalse(mapsAreEquivalent(m7, m3));
        assertFalse(mapsAreEquivalent(m7, m5));
        assertTrue(mapsAreEquivalent(m7, m6));
        assertTrue(mapsAreEquivalent(m7, m7));
    }

    @Test
    public void testMetaIsEquivalent() {
        Map<String, String> m1 = new HashMap<>();
        Map<String, String> m2 = new HashMap<>();

        assertTrue(Validator.metaIsEquivalent(null, null));
        assertTrue(Validator.metaIsEquivalent(null, m1));
        assertTrue(Validator.metaIsEquivalent(m1, null));
        assertTrue(Validator.metaIsEquivalent(m1, m2));

        m1.put("A", "a");
        m1.put(NATS_META_KEY_PREFIX + "foo", "foo");
        assertFalse(Validator.metaIsEquivalent(m1, m2));

        m2.put("A", "a");
        m2.put(NATS_META_KEY_PREFIX + "bar", "bar");
        assertTrue(Validator.metaIsEquivalent(m1, m2));

        m1.put("B", "b");
        assertFalse(Validator.metaIsEquivalent(m1, m2));

        m2.put("B", "b");
        assertTrue(Validator.metaIsEquivalent(m1, m2));

        m2.put("C", "C");
        assertFalse(Validator.metaIsEquivalent(m1, m2));
    }
}