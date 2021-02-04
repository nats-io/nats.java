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

    public static String validateJsSubscribeSubjectRequired(String s) {
        if (nullOrEmpty(s) || containsWhitespace(s)) {
            throw new IllegalArgumentException("Subject cannot have whitespace if provided [" + s + "]");
        }
        return s;
    }

    public static String validateQueueNameNotRequired(String s) {
        if (containsWhitespace(s)) {
            throw new IllegalArgumentException("Queue have whitespace [" + s + "]");
        }
        return emptyAsNull(s);
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
        if (s == null || containsDotWildGt(s)) {
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

    public static String validateDurableRequired(String s) {
        if (nullOrEmpty(s) || containsDotWildGt(s)) {
            throw new IllegalArgumentException("Consumer cannot be blank when provided and cannot contain a '.', '*' or '>' [" + s + "]");
        }
        return s;
    }

    public static String validateDeliverSubjectRequired(String s) {
        if (nullOrEmpty(s)) {
            throw new IllegalArgumentException("Deliver Subject cannot be blank when provided [" + s + "]");
        }
        return s;
    }

    public static final int MAX_PULL_SIZE = 256;
    public static long validatePullBatchSize(long l) {
        if (l < 1 || l > MAX_PULL_SIZE) {
            throw new IllegalArgumentException("Pull Batch Size must be betweeen 1 and " + MAX_PULL_SIZE + " inclusive [" + l + "]");
        }
        return l;
    }

    public static String validateJetStreamPrefix(String s) {
        if (containsWildGt(s)) {
            throw new IllegalArgumentException("Prefix cannot contain a wildcard [" + s + "]");
        }
        return s;
    }

    public static void mustBeNull(String theOther, String message) {
        if (theOther != null) {
            throw new IllegalArgumentException(message);
        }
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

    public static String emptyAsNull(String s) {
        return nullOrEmpty(s) ? null : s;
    }
}
