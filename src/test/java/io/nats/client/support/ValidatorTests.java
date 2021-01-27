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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ValidatorTests {

    private static final String PLAIN     = "plain";
    private static final String HAS_SPACE = "has space";
    private static final String HAS_DASH  = "has-dash";
    private static final String HAS_DOT   = "has.dot";
    private static final String HAS_STAR  = "has*star";
    private static final String HAS_GT    = "has>gt";

    @Test
    public void testValidateMessageSubject() {
        allowed(Validator::validateMessageSubject, PLAIN, HAS_SPACE, HAS_DASH, HAS_DOT, HAS_STAR, HAS_GT);
        notAllowed(Validator::validateMessageSubject, null, EMPTY);
    }

    @Test
    public void testValidateJsSubscribeSubject() {
        allowed(Validator::validateJsSubscribeSubject, null, EMPTY, PLAIN, HAS_DASH, HAS_DOT, HAS_STAR, HAS_GT);
        notAllowed(Validator::validateJsSubscribeSubject, HAS_SPACE);
    }

    @Test
    public void testValidateQueueName() {
        allowed(Validator::validateQueueName, PLAIN, HAS_DASH, HAS_DOT, HAS_STAR, HAS_GT);
        notAllowed(Validator::validateQueueName, null, EMPTY, HAS_SPACE);
    }

    @Test
    public void testValidateReplyTo() {
        allowed(Validator::validateReplyTo, null, PLAIN, HAS_SPACE, HAS_DASH, HAS_DOT, HAS_STAR, HAS_GT);
        notAllowed(Validator::validateReplyTo, EMPTY);
    }

    @Test
    public void testValidateStreamName() {
        allowed(Validator::validateStreamName, PLAIN, HAS_SPACE, HAS_DASH);
        notAllowed(Validator::validateStreamName, null, EMPTY, HAS_DOT, HAS_STAR, HAS_GT);
    }

    @Test
    public void testValidateConsumer() {
        allowed(Validator::validateConsumer, null, PLAIN, HAS_SPACE, HAS_DASH);
        notAllowed(Validator::validateConsumer, EMPTY, HAS_DOT, HAS_STAR, HAS_GT);
    }

    @Test
    public void testValidateDurable() {
        allowed(Validator::validateDurable, PLAIN, HAS_SPACE, HAS_DASH);
        notAllowed(Validator::validateDurable, null, EMPTY, HAS_DOT, HAS_STAR, HAS_GT);
    }

    @Test
    public void testValidateDeliverSubject() {
        allowed(Validator::validateDeliverSubject, null, PLAIN, HAS_SPACE, HAS_DASH, HAS_DOT, HAS_STAR, HAS_GT);
        notAllowed(Validator::validateDeliverSubject, EMPTY);
    }

    @Test
    public void testValidatePullBatchSize() {
        assertEquals(0, validatePullBatchSize(0));
        assertEquals(1, validatePullBatchSize(1));
        assertThrows(IllegalArgumentException.class, () -> validatePullBatchSize(-1));
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
            assertEquals(s, test.validate(s));
        }
    }

    private void notAllowed(StringTest test, String... strings) {
        for (String s : strings) {
            assertThrows(IllegalArgumentException.class, () -> test.validate(s));
        }
    }
}