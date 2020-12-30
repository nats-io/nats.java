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

import java.util.regex.Pattern;

public class Validator {

    static final Pattern STREAM_PATTERN = Pattern.compile("^[a-zA-Z0-9-]+$");

    public static String validateSubject(String s) {
        if (nullOrEmpty(s)) {
            throw new IllegalArgumentException("Subject cannot be null or empty.");
        }
        return s;
    }

    public static String validateSubjectStrict(String s) {
        if (nullOrEmpty(s) || notStrict(s)) {
            throw new IllegalArgumentException("Subject cannot be null or empty or have whitespace, '.', '>' or '*'.");
        }

        return s;
    }

    public static String validateQueueName(String s) {
        if (nullOrEmpty(s)) {
            throw new IllegalArgumentException("Queue name cannot be null or empty or have whitespace.");
        }

        return s;
    }

    public static String validateReplyTo(String s) {
        if (notNullButEmpty(s)) {
            throw new IllegalArgumentException("ReplyTo cannot blank when provided.");
        }
        return s;
    }

    public static String validateStreamName(String s) {
        if (s != null && doesNotMatch(STREAM_PATTERN, s)) {
            throw new IllegalArgumentException("Stream is required and must be alpha, numeric or dash.");
        }
        return s;
    }

    public static String validateConsumer(String s) {
        if (notNullButEmpty(s)) {
            throw new IllegalArgumentException("Consumer cannot be blank when provided.");
        }
        return s;
    }

    public static String validateDurable(String s) {
        if (notNullButEmpty(s)) {
            throw new IllegalArgumentException("Durable cannot be blank when provided.");
        }
        return s;
    }

    public static String validateDeliverSubject(String s) {
        if (notNullButEmpty(s)) {
            throw new IllegalArgumentException("Deliver Subject cannot be blank when provided.");
        }
        return s;
    }

    public static long validatePullBatchSize(long l) {
        if (l < 0) {
            throw new IllegalArgumentException("Batch size must be greater than or equal to zero");
        }
        return l;
    }

    // ----------------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------------

    public static Object validateNotNull(Object o, String fieldName) {
        if (o == null) {
            throw new IllegalArgumentException(fieldName + "cannot be null");
        }
        return o;
    }

    public static String validateNotNull(String s, String fieldName) {
        if (s == null) {
            throw new IllegalArgumentException(fieldName + "cannot be null");
        }
        return s;
    }

    public static boolean matches(Pattern pattern, String stream) {
        return pattern.matcher(stream).matches();
    }

    public static boolean doesNotMatch(Pattern pattern, String stream) {
        return !pattern.matcher(stream).matches();
    }

    public static boolean nullOrEmpty(String s) {
        return s == null || s.length() == 0;
    }

    public static boolean notNullButEmpty(String s) {
        return s != null && s.length() == 0;
    }

    public static boolean containsWhitespace(String s) {
        for (int i = 0; i < s.length(); i++){
            if (Character.isWhitespace(s.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    public static boolean notStrict(String s) {
        for (int i = 0; i < s.length(); i++){
            char c = s.charAt(i);
            if (c == '.' || c == '*' || c == '>' || Character.isWhitespace(c)) {
                return true;
            }
        }
        return false;
    }
}
