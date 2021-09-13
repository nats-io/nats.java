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

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static io.nats.client.support.NatsJetStreamConstants.MAX_PULL_SIZE;

public abstract class Validator {
    private Validator() {
    } /* ensures cannot be constructed */

    public static String validateSubject(String s, boolean required) {
        return validatePrintable(s, "Subject", required);
    }

    public static String validateReplyTo(String s, boolean required) {
        return validatePrintableExceptWildGt(s, "Reply To", required);
    }

    public static String validateQueueName(String s, boolean required) {
        return validatePrintableExceptWildDotGt(s, "Queue", required);
    }

    public static String validateStreamName(String s, boolean required) {
        return validatePrintableExceptWildDotGt(s, "Stream", required);
    }

    public static String validateDurable(String s, boolean required) {
        return validatePrintableExceptWildDotGt(s, "Durable", required);
    }

    public static String validateJetStreamPrefix(String s) {
        return validatePrintableExceptWildGtDollar(s, "Prefix", false);
    }

    public static String validateBucketNameRequired(String s) {
        return validateKvBucketName(s, "Bucket name", true);
    }

    public static String validateKeyRequired(String s) {
        return validateKvKey(s, "Key", true);
    }

    public static String validateMustMatchIfBothSupplied(String s1, String s2, String label1, String label2) {
        // s1   | s2   || result
        // ---- | ---- || --------------
        // null | null || valid, null s2
        // null | y    || valid, y s2
        // x    | null || valid, x s1
        // x    | x    || valid, x s1
        // x    | y    || invalid
        s1 = emptyAsNull(s1);
        s2 = emptyAsNull(s2);
        if (s1 == null) {
            return s2; // s2 can be either null or y
        }

        // x / null or x / x
        if (s2 == null || s1.equals(s2)) {
            return s1;
        }

        throw new IllegalArgumentException(String.format("%s [%s] must match the %s [%s] if both are provided.", label1, s1, label2, s2));
    }

    interface Check {
        String check();
    }

    public static String _validate(String s, boolean required, String label, Check check) {
        if (emptyAsNull(s) == null) {
            if (required) {
                throw new IllegalArgumentException(label + " cannot be null or empty.");
            }
            return null;
        }
        return check.check();
    }

    public static String validateMaxLength(String s, int maxLength, boolean required, String label) {
        return _validate(s, required, label, () -> {
            int len = s.getBytes(StandardCharsets.UTF_8).length;
            if (len > maxLength) {
                throw new IllegalArgumentException(label + " cannot be longer than " + maxLength + " bytes but was " + len + " bytes");
            }
            return s;
        });
    }

    public static String validatePrintable(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notPrintable(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range [" + s + "]");
            }
            return s;
        });
    }

    public static String validatePrintableExceptWildDotGt(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notPrintableOrHasWildGtDot(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include `*`, `.` or `>` [" + s + "]");
            }
            return s;
        });
    }

    public static String validateKvBucketName(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notRestrictedTerm(s)) {
                throw new IllegalArgumentException(label + " must only contain A-Z, a-z, 0-9, `-` or `_` [" + s + "]");
            }
            return s;
        });
    }

    public static String validateKvKey(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notKvKey(s)) {
                throw new IllegalArgumentException(label + " must only contain A-Z, a-z, 0-9, `-`, `_`, `/`, `=` or `.` and cannot start with `.` [" + s + "]");
            }
            return s;
        });
    }

    public static String validatePrintableExceptWildGt(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notPrintableOrHasWildGt(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include `*`, `>` or `$` [" + s + "]");
            }
            return s;
        });
    }

    public static String validatePrintableExceptWildGtDollar(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notPrintableOrHasWildGtDollar(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include `*`, `>` or `$` [" + s + "]");
            }
            return s;
        });
    }

    public static int validatePullBatchSize(int pullBatchSize) {
        if (pullBatchSize < 1 || pullBatchSize > MAX_PULL_SIZE) {
            throw new IllegalArgumentException("Pull Batch Size must be between 1 and " + MAX_PULL_SIZE + " inclusive [" + pullBatchSize + "]");
        }
        return pullBatchSize;
    }

    public static long validateMaxConsumers(long max) {
        return validateGtZeroOrMinus1(max, "Max Consumers");
    }

    public static long validateMaxMessages(long max) {
        return validateGtZeroOrMinus1(max, "Max Messages");
    }

    public static long validateMaxBucketValues(long max) {
        return validateGtZeroOrMinus1(max, "Max Bucket Values");
    }

    public static long validateMaxMessagesPerSubject(long max) {
        if (max < -1) {
            throw new IllegalArgumentException("Max Messages per Subject must be greater than or equal to zero");
        }
        return max;
        // TODO Waiting on a server change take that /\ out, put this \/ back in
//        return validateGtZeroOrMinus1(max, "Max Messages per Subject");
    }

    public static long validateMaxValuesPerKey(long max) {
        if (max < -1) {
            throw new IllegalArgumentException("Max Values per Key must be greater than or equal to zero");
        }
        return max;
        // TODO Waiting on a server change take that /\ out, put this \/ back in
//        return validateGtZeroOrMinus1(max, "Max Values per Key");
    }

    public static long validateMaxBytes(long max) {
        return validateGtZeroOrMinus1(max, "Max Bytes");
    }

    public static long validateMaxBucketBytes(long max) {
        return validateGtZeroOrMinus1(max, "Max Bucket Bytes");
    }

    public static long validateMaxMessageSize(long max) {
        return validateGtZeroOrMinus1(max, "Max Message size");
    }

    public static long validateMaxValueSize(long max) {
        return validateGtZeroOrMinus1(max, "Max Value Bytes");
    }

    public static int validateNumberOfReplicas(int replicas) {
        if (replicas < 1 || replicas > 5) {
            throw new IllegalArgumentException("Replicas must be from 1 to 5 inclusive.");
        }
        return replicas;
    }

    public static Duration validateDurationRequired(Duration d) {
        if (d == null || d.isZero() || d.isNegative()) {
            throw new IllegalArgumentException("Duration required and must be greater than 0.");
        }
        return d;
    }

    public static Duration validateDurationNotRequiredGtOrEqZero(Duration d, Duration ifNull) {
        if (d == null) {
            return ifNull;
        }
        if (d.isNegative()) {
            throw new IllegalArgumentException("Duration must be greater than or equal to 0.");
        }
        return d;
    }

    public static Duration validateDurationNotRequiredGtOrEqZero(long millis) {
        if (millis < 0) {
            throw new IllegalArgumentException("Duration must be greater than or equal to 0.");
        }
        return Duration.ofMillis(millis);
    }

    public static void validateNotNull(Object o, String fieldName) {
        if (o == null) {
            throw new IllegalArgumentException(fieldName + " cannot be null");
        }
    }

    public static long validateGtZeroOrMinus1(long l, String label) {
        if (zeroOrLtMinus1(l)) {
            throw new IllegalArgumentException(label + " must be greater than zero or -1 for unlimited");
        }
        return l;
    }

    public static long validateGtEqMinus1(long l, String label) {
        if (l < -1) {
            throw new IllegalArgumentException(label + " must be greater than zero or -1 for unlimited");
        }
        return l;
    }

    public static long validateNotNegative(long l, String label) {
        if (l < 0) {
            throw new IllegalArgumentException(label + " cannot be negative");
        }
        return l;
    }

    public static long validateGtEqZero(long l, String label) {
        if (l < 0) {
            throw new IllegalArgumentException(label + " must be greater than or equal to zero");
        }
        return l;
    }

    // ----------------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------------
    public static boolean nullOrEmpty(String s) {
        return s == null || s.trim().length() == 0;
    }

    public static boolean notPrintable(String s) {
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < 33 || c > 126) {
                return true;
            }
        }
        return false;
    }

    public static boolean notPrintableOrHasChars(String s, char[] charsToNotHave) {
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < 33 || c > 126) {
                return true;
            }
            for (char cx : charsToNotHave) {
                if (c == cx) {
                    return true;
                }
            }
        }
        return false;
    }

    // restricted-term  = (A-Z, a-z, 0-9, dash 45, underscore 95)+
    public static boolean notRestrictedTerm(String s) {
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < '0') { // before 0
                if (c == '-') { // only dash is accepted
                    continue;
                }
                return true; // "not"
            }
            if (c < ':') {
                continue; // means it's 0 - 9
            }
            if (c < 'A') {
                return true; // between 9 and A is "not restricted"
            }
            if (c < '[') {
                continue; // means it's A - Z
            }
            if (c < 'a') { // before a
                if (c == '_') { // only underscore is accepted
                    continue;
                }
                return true; // "not"
            }
            if (c > 'z') { // 122 is z, characters after of them are "not restricted"
                return true;
            }
        }
        return false;
    }

    // limited-term = (A-Z, a-z, 0-9, dash 45, underscore 95, fwd-slash 47, equals 61)+
    // kv-key-name = limited-term (dot limited-term)*
    public static boolean notKvKey(String s) {
        if (s.charAt(0) == '.') {
            return true; // can't start with dot
        }
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < '0') { // before 0
                if (c == '-' || c == '.' || c == '/') { // only dash dot and and fwd slash are accepted
                    continue;
                }
                return true; // "not"
            }
            if (c < ':') {
                continue; // means it's 0 - 9
            }
            if (c < 'A') {
                if (c == '=') { // equals is accepted
                    continue;
                }
                return true; // between 9 and A is "not limited"
            }
            if (c < '[') {
                continue; // means it's A - Z
            }
            if (c < 'a') { // before a
                if (c == '_') { // only underscore is accepted
                    continue;
                }
                return true; // "not"
            }
            if (c > 'z') { // 122 is z, characters after of them are "not limited"
                return true;
            }
        }
        return false;
    }

    static final char[] WILD_GT = {'*', '>'};
    static final char[] WILD_GT_DOT = {'*', '>', '.'};
    static final char[] WILD_GT_DOLLAR = {'*', '>', '$'};

    private static boolean notPrintableOrHasWildGt(String s) {
        return notPrintableOrHasChars(s, WILD_GT);
    }

    private static boolean notPrintableOrHasWildGtDot(String s) {
        return notPrintableOrHasChars(s, WILD_GT_DOT);
    }

    private static boolean notPrintableOrHasWildGtDollar(String s) {
        return notPrintableOrHasChars(s, WILD_GT_DOLLAR);
    }

    public static String emptyAsNull(String s) {
        return nullOrEmpty(s) ? null : s;
    }

    public static String emptyOrNullAs(String s, String ifEmpty) {
        return nullOrEmpty(s) ? ifEmpty : s;
    }

    public static boolean zeroOrLtMinus1(long l) {
        return l == 0 || l < -1;
    }

    public static Duration ensureNotNullAndNotLessThanMin(Duration provided, Duration minimum, Duration dflt)
    {
        return provided == null || provided.toNanos() < minimum.toNanos() ? dflt : provided;
    }

    public static Duration ensureDurationNotLessThanMin(long providedMillis, Duration minimum, Duration dflt)
    {
        return ensureNotNullAndNotLessThanMin(Duration.ofMillis(providedMillis), minimum, dflt);
    }
}
