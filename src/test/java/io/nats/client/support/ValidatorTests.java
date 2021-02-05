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

import org.junit.jupiter.api.Test;

import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.support.Validator.validateNotNull;
import static io.nats.client.support.Validator.validatePullBatchSize;
import static org.junit.jupiter.api.Assertions.*;

public class ValidatorTests {

    private static final String PLAIN     = "plain";
    private static final String HAS_SPACE = "has space";
    private static final String HAS_DASH  = "has-dash";
    private static final String HAS_DOT   = "has.dot";
    private static final String HAS_STAR  = "has*star";
    private static final String HAS_GT    = "has>gt";

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
    public void testValidateQueueNameOrEmptyAsNull() {
        allowed(Validator::validateQueueNameOrEmptyAsNull, PLAIN, HAS_DASH, HAS_DOT, HAS_STAR, HAS_GT);
        allowedEmptyAsNull(Validator::validateQueueNameOrEmptyAsNull, null, EMPTY);
        notAllowed(Validator::validateQueueNameOrEmptyAsNull, HAS_SPACE);
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
        allowedEmptyAsNull(Validator::validateQueueNameOrEmptyAsNull, null, EMPTY);
        notAllowed(Validator::validateStreamNameOrEmptyAsNull, HAS_DOT, HAS_STAR, HAS_GT);
    }

    @Test
    public void testValidateDurableOrEmptyAsNull() {
        allowed(Validator::validateDurableOrEmptyAsNull, PLAIN, HAS_SPACE, HAS_DASH);
        allowedEmptyAsNull(Validator::validateDurableOrEmptyAsNull, null, EMPTY);
        notAllowed(Validator::validateDurableOrEmptyAsNull, HAS_DOT, HAS_STAR, HAS_GT);
    }

    @Test
    public void testValidateDeliverSubjectOrEmptyAsNull() {
        allowed(Validator::validateDeliverSubjectOrEmptyAsNull, PLAIN, HAS_SPACE, HAS_DASH, HAS_DOT, HAS_STAR, HAS_GT);
        allowedEmptyAsNull(Validator::validateDurableOrEmptyAsNull, null, EMPTY);
    }

    @Test
    public void testValidatePullBatchSize() {
        assertEquals(1, validatePullBatchSize(1));
        assertEquals(Validator.MAX_PULL_SIZE, validatePullBatchSize(Validator.MAX_PULL_SIZE));
        assertThrows(IllegalArgumentException.class, () -> validatePullBatchSize(0));
        assertThrows(IllegalArgumentException.class, () -> validatePullBatchSize(-1));
        assertThrows(IllegalArgumentException.class, () -> validatePullBatchSize(Validator.MAX_PULL_SIZE + 1));
    }

    @Test
    public void testNotNull() {
        Object o = null;
        String s = null;
        assertThrows(IllegalArgumentException.class, () -> validateNotNull(o, "fieldName"));
        assertThrows(IllegalArgumentException.class, () -> validateNotNull(s, "fieldName"));
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