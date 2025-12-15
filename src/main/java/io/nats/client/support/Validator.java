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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsJetStreamConstants.MAX_HISTORY_PER_KEY;
import static io.nats.client.support.NatsJetStreamConstants.NATS_META_KEY_PREFIX;

@SuppressWarnings("UnusedReturnValue")
public abstract class Validator {

    private Validator() {} /* ensures cannot be constructed */

    /*
        cannot contain spaces \r \n \t
    */
    public static String validateSubjectTerm(String subject, String label, boolean required) {
        if (subject == null || subject.length() == 0) {
            if (required) {
                throw new IllegalArgumentException(label + " cannot be null or empty.");
            }
            return null;
        }
        for (int i = 0; i < subject.length(); i++) {
            char c = subject.charAt(i);
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                throw new IllegalArgumentException(label + " cannot contain space, tab, carriage return or linefeed character");
            }
        }
        return subject;
    }

    /*
        cannot contain spaces \r \n \t
        cannot start or end with subject token delimiter .
        some things don't allow it to end greater
    */
    public static String validateSubjectTermStrict(String subject, String label, boolean required) {
        subject = emptyAsNull(subject);
        if (subject == null) {
            if (required) {
                throw new IllegalArgumentException(label + " cannot be null or empty.");
            }
            return null;
        }
        if (subject.endsWith(".")) {
            throw new IllegalArgumentException(label + " cannot end with '.'");
        }

        String[] segments = subject.split("\\.");
        for (int seg = 0; seg < segments.length; seg++) {
            String segment = segments[seg];
            int sl = segment.length();
            if (sl == 0) {
                if (seg == 0) {
                    throw new IllegalArgumentException(label + " cannot start with '.'");
                }
                throw new IllegalArgumentException(label + " segment cannot be empty");
            }
            else {
                for (int m = 0; m < sl; m++) {
                    char c = segment.charAt(m);
                    if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                        throw new IllegalArgumentException(label + " cannot contain space, tab, carriage return or linefeed character");
                    }
                    if (c == '*') {
                        if (sl != 1) {
                            throw new IllegalArgumentException(label + " wildcard improperly placed.");
                        }
                    }
                    if (c == '>') {
                        if (sl != 1 || (seg + 1 != segments.length)) {
                            throw new IllegalArgumentException(label + " wildcard improperly placed.");
                        }
                    }
                }
            }
        }
        return subject;
    }

    public static String validateSubject(String s, boolean required) {
        return validateSubjectTerm(s, "Subject", required);
    }

    public static String validateSubject(String subject, String label, boolean required, boolean cantEndWithGt) {
        subject = validateSubjectTermStrict(subject, label, required);
        if (subject != null && cantEndWithGt && subject.endsWith(".>")) {
            throw new IllegalArgumentException(label + " last segment cannot be '>'");
        }
        return subject;
    }

    public static String validateReplyTo(String s, boolean required) {
        return validatePrintableExceptWildGt(s, "Reply To", required);
    }

    public static String validateQueueName(String s, boolean required) {
        return validateSubjectTermStrict(s, "QueueName", required);
    }

    public static String validateStreamName(String s, boolean required) {
        return validatePrintableExceptWildDotGtSlashes(s, "Stream", required);
    }

    public static String validateDurable(String s, boolean required) {
        return validatePrintableExceptWildDotGtSlashes(s, "Durable", required);
    }

    public static String validateConsumerName(String s, boolean required) {
        return validatePrintableExceptWildDotGtSlashes(s, "Name", required);
    }

    public static String validatePrefixOrDomain(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (s.startsWith(DOT)) {
                throw new IllegalArgumentException(label + " cannot start with '.' [" + s + "]");
            }
            if (notPrintableOrHasWildGt(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include '*', '>' [" + s + "]");
            }
            return s;
        });
    }

    public static List<String> validateKvKeysWildcardAllowedRequired(List<String> keys) {
        required(keys, "Key");
        for (String key : keys) {
            validateWildcardKvKey(key, "Key", true);
        }
        return keys;
    }

    public static String validateKvKeyWildcardAllowedRequired(String s) {
        return validateWildcardKvKey(s, "Key", true);
    }

    public static String validateNonWildcardKvKeyRequired(String s) {
        return validateNonWildcardKvKey(s, "Key", true);
    }

    public static void validateNotSupplied(String s, NatsJetStreamClientError err) {
        if (!nullOrEmpty(s)) {
            throw err.instance();
        }
    }

    public static String validateMustMatchIfBothSupplied(String s1, String s2, NatsJetStreamClientError err) {
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

        throw err.instance();
    }

    public static String required(String s, String label) {
        if (emptyAsNull(s) == null) {
            throw new IllegalArgumentException(label + " cannot be null or empty.");
        }
        return s;
    }

    @Deprecated
    public static String required(String s1, String s2, String label) {
        if (emptyAsNull(s1) == null || emptyAsNull(s2) == null) {
            throw new IllegalArgumentException(label + " cannot be null or empty.");
        }
        return s1;
    }

    public static <T> T required(T o, String label) {
        if (o == null) {
            throw new IllegalArgumentException(label + " cannot be null.");
        }
        return o;
    }

    public static void required(List<?> l, String label) {
        if (l == null || l.isEmpty()) {
            throw new IllegalArgumentException(label + " cannot be null or empty.");
        }
    }

    public static void required(Map<?, ?> m, String label) {
        if (m == null || m.isEmpty()) {
            throw new IllegalArgumentException(label + " cannot be null or empty.");
        }
    }

    public static String _validate(String s, boolean required, String label, Supplier<String> customValidate) {
        if (emptyAsNull(s) == null) {
            if (required) {
                throw new IllegalArgumentException(label + " cannot be null or empty.");
            }
            return null;
        }
        return customValidate.get();
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
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include '*', '.' or '>' [" + s + "]");
            }
            return s;
        });
    }

    public static String validatePrintableExceptWildDotGtSlashes(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notPrintableOrHasWildGtDotSlashes(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include '*', '.', '>', '\\' or  '/' [" + s + "]");
            }
            return s;
        });
    }

    public static String validatePrintableExceptWildGt(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notPrintableOrHasWildGt(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include '*' or '>' [" + s + "]");
            }
            return s;
        });
    }

    public static String validateIsRestrictedTerm(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notRestrictedTerm(s)) {
                throw new IllegalArgumentException(label + " must only contain A-Z, a-z, 0-9, '-' or '_' [" + s + "]");
            }
            return s;
        });
    }

    public static String validateBucketName(String s, boolean required) {
        return validateIsRestrictedTerm(s, "Bucket Name", required);
    }

    public static String validateWildcardKvKey(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notWildcardKvKey(s)) {
                throw new IllegalArgumentException(label + " must only contain A-Z, a-z, 0-9, '*', '-', '_', '/', '=', '>' or '.' and cannot start with '.' [" + s + "]");
            }
            return s;
        });
    }

    public static String validateNonWildcardKvKey(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (notNonWildcardKvKey(s)) {
                throw new IllegalArgumentException(label + " must only contain A-Z, a-z, 0-9, '-', '_', '/', '=' or '.' and cannot start with '.' [" + s + "]");
            }
            return s;
        });
    }

    public static long validateMaxConsumers(long max) {
        return validateGtZeroOrMinus1(max, "Max Consumers");
    }

    public static long validateMaxMessages(long max) {
        return validateGtZeroOrMinus1(max, "Max Messages");
    }

    public static long validateMaxMessagesPerSubject(long max) {
        return validateGtZeroOrMinus1(max, "Max Messages Per Subject");
    }

    public static int validateMaxHistory(int max) {
        if (max < 1 || max > MAX_HISTORY_PER_KEY) {
            throw new IllegalArgumentException("Max History must be from 1 to " + MAX_HISTORY_PER_KEY + " inclusive.");
        }
        return max;
    }

    public static long validateMaxBytes(long max) {
        return validateGtZeroOrMinus1(max, "Max Bytes");
    }

    public static long validateMaxBucketBytes(long max) {
        return validateGtZeroOrMinus1(max, "Max Bucket Bytes"); // max bucket bytes is a kv alias to max bytes
    }

    private static long validateMaxMessageSize(long max, String label) {
        long l = validateGtZeroOrMinus1(max, label);
        if (l > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(label + " cannot be larger than " + Integer.MAX_VALUE);
        }
        return l;
    }

    public static long validateMaxMessageSize(long max) {
        return validateMaxMessageSize(max, "Max Message Size");
    }

    public static long validateMaxValueSize(long max) {
        return validateMaxMessageSize(max, "Max Value Size"); // max value size is a kv alias to max message size
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

    public static Duration validateDurationNotRequiredGtOrEqSeconds(long minSeconds, Duration d, Duration ifNull, String label) {
        return d == null ? ifNull : validateDurationGtOrEqSeconds(minSeconds, d.toMillis(), label);
    }

    public static Duration validateDurationGtOrEqSeconds(long minSeconds, long millis, String label) {
        if (millis < (minSeconds * 1000)) {
            throw new IllegalArgumentException(label + " must be greater than or equal to " + minSeconds + " second(s).");
        }
        return Duration.ofMillis(millis);
    }

    public static String validateNotNull(String s, String fieldName) {
        if (s == null) {
            throw new IllegalArgumentException(fieldName + " cannot be null");
        }
        return s;
    }

    public static Object validateNotNull(Object o, String fieldName) {
        if (o == null) {
            throw new IllegalArgumentException(fieldName + " cannot be null");
        }
        return o;
    }

    public static int validateGtZero(int i, String label) {
        if (i < 1) {
            throw new IllegalArgumentException(label + " must be greater than zero");
        }
        return i;
    }

    public static long validateGtZero(long l, String label) {
        if (l < 1) {
            throw new IllegalArgumentException(label + " must be greater than zero");
        }
        return l;
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

    public static boolean isGtEqZero(long l) {
        return l >= 0;
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
        return s == null || s.trim().isEmpty();
    }

    public static <T> boolean nullOrEmpty(T[] a) {
        return a == null || a.length == 0;
    }

    public static boolean nullOrEmpty(Collection<?> c) {
        return c == null || c.isEmpty();
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

    // limited-term = (A-Z, a-z, 0-9, dash 45, dot 46, fwd-slash 47, equals 61, underscore 95)+
    // kv-key-name = limited-term (dot limited-term)*
    public static boolean notNonWildcardKvKey(String s) {
        if (s.charAt(0) == '.') {
            return true; // can't start with dot
        }
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < '0') { // before 0
                if (c == '-' || c == '.' || c == '/') { // only dash dot and fwd slash are accepted
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

    // (A-Z, a-z, 0-9, star 42, dash 45, dot 46, fwd-slash 47, equals 61, gt 62, underscore 95)+
    public static boolean notWildcardKvKey(String s) {
        if (s.charAt(0) == '.') {
            return true; // can't start with dot
        }
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < '0') { // before 0
                if (c == '*' || c == '-' || c == '.' || c == '/') { // only star dash dot and fwd slash are accepted
                    continue;
                }
                return true; // "not"
            }
            if (c < ':') {
                continue; // means it's 0 - 9
            }
            if (c < 'A') {
                if (c == '=' || c == '>') { // equals, gt is accepted
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
    static final char[] WILD_GT_DOT_SLASHES = {'*', '>', '.', '\\', '/'};

    public static boolean notPrintableOrHasWildGt(String s) {
        return notPrintableOrHasChars(s, WILD_GT);
    }

    public static boolean notPrintableOrHasWildGtDot(String s) {
        return notPrintableOrHasChars(s, WILD_GT_DOT);
    }

    public static boolean notPrintableOrHasWildGtDotSlashes(String s) {
        return notPrintableOrHasChars(s, WILD_GT_DOT_SLASHES);
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

    public static String ensureEndsWithDot(String s) {
        return s == null || s.endsWith(DOT) ? s : s + DOT;
    }

    static final Pattern SEMVER_PATTERN = Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$");

    public static String validateSemVer(String s, String label, boolean required) {
        return _validate(s, required, label, () -> {
            if (!isSemVer(s)) {
                throw new IllegalArgumentException(label + " must be a valid SemVer");
            }
            return s;
        });
    }

    public static boolean isSemVer(String s) {
        return SEMVER_PATTERN.matcher(s).find();
    }

    // This function tests filter subject equivalency
    // It does not care what order and also assumes that there are no duplicates.
    // From the server: consumer subject filters cannot overlap [10138]
    public static <T> boolean listsAreEquivalent(List<T> l1, List<T> l2)
    {
        if (l1 == null || l1.isEmpty()) {
            return l2 == null || l2.isEmpty();
        }

        if (l2 == null || l1.size() != l2.size()) {
            return false;
        }

        for (T t : l1) {
            if (!l2.contains(t)) {
                return false;
            }
        }
        return true;
    }

    public static boolean mapsAreEquivalent(Map<String, String> m1, Map<String, String> m2)
    {
        int s1 = m1 == null ? 0 : m1.size();
        int s2 = m2 == null ? 0 : m2.size();

        if (s1 != s2) {
            return false;
        }

        if (s1 > 0) {
            for (Map.Entry<String, String> entry : m1.entrySet())
            {
                if (!entry.getValue().equals(m2.get(entry.getKey()))) {
                    return false;
                }
            }
        }

        return true;
    }

    // this is a special case map where the meta has both user and nats headers like
    // _nats.req.level=0, _nats.ver=2.12.0-preview.2, _nats.level=2
    // in this case we only want to compare the user keys
    public static boolean metaIsEquivalent(Map<String, String> m1, Map<String, String> m2) {
        if (m1 == null || m1.isEmpty()) {
            return m2 == null || m2.isEmpty();
        }

        // m1 isn't null or empty
        if (m2 == null || m2.isEmpty()) {
            return false;
        }

        // 1. make sure all user keys from m1 are in m2
        int user1 = 0;
        for (Map.Entry<String, String> entry : m1.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(NATS_META_KEY_PREFIX)) {
                if (!m2.containsKey(key)) {
                    return false;
                }
                user1++;
            }
        }

        // 2. all m1 keys were found in m2, so count m2 user keys
        int user2 = 0;
        for (String key : m2.keySet()) {
            if (!key.startsWith(NATS_META_KEY_PREFIX)) {
                user2++;
            }
        }

        // 3. just make sure m2 didn't have more keys
        return user1 == user2;
    }
}
