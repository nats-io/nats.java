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

public abstract class Validator {
    private Validator() {} /* for Jacoco */

    public static String validateMessageSubjectRequired(String s) {
        if (nullOrEmpty(s)) {
            throw new IllegalArgumentException("Subject cannot be null or empty [" + s + "]");
        }
        return s;
    }

    public static String validateJsSubscribeSubject(String s) {
        if (containsWhitespace(s)) {
            throw new IllegalArgumentException("Subject cannot have whitespace if provided [" + s + "]");
        }
        return s;
    }

    public static String validateQueueNameRequired(String s) {
        if (nullOrEmpty(s) || containsWhitespace(s)) {
            throw new IllegalArgumentException("Queue cannot be null or empty or have whitespace [" + s + "]");
        }
        return s;
    }

    public static String validateReplyToNullButNotEmpty(String s) {
        if (notNullButEmpty(s)) {
            throw new IllegalArgumentException("ReplyTo cannot be blank when provided [" + s + "]");
        }
        return s;
    }

    public static String validateStreamName(String s) {
        if (containsDotWildGt(s)) {
            throw new IllegalArgumentException("Stream cannot contain a '.', '*' or '>' [" + s + "]");
        }
        return s;
    }

    public static String validateStreamNameNullButNotEmpty(String s) {
        if (notNullButEmpty(s)) {
            throw new IllegalArgumentException("Stream cannot contain a '.', '*' or '>' [" + s + "]");
        }
        return validateStreamName(s);
    }

    public static String validateStreamNameRequired(String s) {
        if (nullOrEmpty(s)) {
            throw new IllegalArgumentException("Stream cannot be null or empty and cannot contain a '.', '*' or '>' [" + s + "]");
        }
        return validateStreamName(s);
    }

    public static String validateConsumerNullButNotEmpty(String s) {
        if (notNullButEmpty(s) || containsDotWildGt(s)) {
            throw new IllegalArgumentException("Consumer cannot be blank when provided and cannot contain a '.', '*' or '>' [" + s + "]");
        }
        return s;
    }

    public static String validateDurableRequired(String s) {
        if (nullOrEmpty(s) || containsDotWildGt(s)) {
            throw new IllegalArgumentException("Durable cannot be blank and cannot contain a '.', '*' or '>' [" + s + "]");
        }
        return s;
    }

    public static String validateDeliverSubjectRequired(String s) {
        if (nullOrEmpty(s)) {
            throw new IllegalArgumentException("Deliver Subject cannot be blank when provided [" + s + "]");
        }
        return s;
    }

    public static long validatePullBatchSize(long l) {
        if (l < 0) {
            throw new IllegalArgumentException("Batch size must be greater than or equal to zero [" + l + "]");
        }
        return l;
    }

    public static String validateJetStreamPrefix(String s) {
        if (containsWildGt(s)) {
            throw new IllegalArgumentException("Prefix cannot contain a wildcard [" + s + "]");
        }
        return s;
    }

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

    // ----------------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------------
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
        if (s != null) {
            for (int i = 0; i < s.length(); i++) {
                if (Character.isWhitespace(s.charAt(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean containsDotWildGt(String s) {
        if (s != null) {
            for (int i = 0; i < s.length(); i++) {
                switch (s.charAt(i)) {
                    case '.':
                    case '*':
                    case '>':
                        return true;
                }
            }
        }
        return false;
    }

    public static boolean containsWildGt(String s) {
        if (s != null) {
            for (int i = 0; i < s.length(); i++) {
                switch (s.charAt(i)) {
                    case '*':
                    case '>':
                        return true;
                }
            }
        }
        return false;
    }

    public static boolean containsDot(String s) {
        return s != null && s.indexOf('.') > -1;
    }
}
