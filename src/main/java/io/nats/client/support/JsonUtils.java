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

import io.nats.client.api.SequenceInfo;
import io.nats.client.api.SequencePair;
import io.nats.client.impl.Headers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.nats.client.support.Encoding.jsonDecode;
import static io.nats.client.support.Encoding.jsonEncode;
import static io.nats.client.support.NatsConstants.COLON;

/**
 * Internal json parsing helpers.
 */
public abstract class JsonUtils {
    public static final String EMPTY_JSON = "{}";

    private static final String STRING_RE  = "\"(.+?)\"";
    private static final String BOOLEAN_RE =  "(true|false)";
    private static final String INTEGER_RE =  "(-?\\d+)";
    private static final String STRING_ARRAY_RE = "\\[\\s*(\".+?\")\\s*\\]";
    private static final String NUMBER_ARRAY_RE = "\\[\\s*(.+?)\\s*\\]";
    private static final String BEFORE_FIELD_RE = "\"";
    private static final String AFTER_FIELD_RE = "\"\\s*:\\s*";

    private static final String Q = "\"";
    private static final String QCOLONQ = "\":\"";
    private static final String QCOLON = "\":";
    private static final String QCOMMA = "\",";
    private static final String COMMA = ",";
    public static final String OPENQ = "{\"";
    public static final String CLOSE = "}";

    private JsonUtils() {} /* ensures cannot be constructed */

    public enum FieldType {
        jsonString(STRING_RE),
        jsonBoolean(BOOLEAN_RE),
        jsonInteger(INTEGER_RE),
        jsonNumber(INTEGER_RE),
        jsonStringArray(STRING_ARRAY_RE);

        final String re;
        FieldType(String re) {
            this.re = re;
        }
    }

    public static Pattern string_pattern(String field) {
        return buildPattern(field, STRING_RE);
    }

    @Deprecated
    public static Pattern number_pattern(String field) {
        return integer_pattern(field);
    }

    public static Pattern integer_pattern(String field) {
        return buildPattern(field, INTEGER_RE);
    }

    public static Pattern boolean_pattern(String field) {
        return buildPattern(field, BOOLEAN_RE);
    }

    public static Pattern string_array_pattern(String field) {
        return buildPattern(field, STRING_ARRAY_RE);
    }

    public static Pattern number_array_pattern(String field) {
        return buildPattern(field, NUMBER_ARRAY_RE);
    }

    /**
     * Builds a json parsing pattern
     * @param fieldName name of the field
     * @param type type of the field.
     * @return pattern.
     */
    public static Pattern buildPattern(String fieldName, FieldType type) {
        return buildPattern(fieldName, type.re);
    }

    public static Pattern buildPattern(String fieldName, String typeRE) {
        return Pattern.compile(BEFORE_FIELD_RE + fieldName + AFTER_FIELD_RE + typeRE, Pattern.CASE_INSENSITIVE);
    }

    /**
     * Extract a JSON object string by object name. Returns empty object '{}' if not found.
     * @param objectName object name
     * @param json source json
     * @return object json string
     */
    public static String getJsonObject(String objectName, String json) {
        return getJsonObject(objectName, json, EMPTY_JSON);
    }

    public static String getJsonObject(String objectName, String json, String dflt) {
        int[] indexes = getBracketIndexes(objectName, json, '{', '}', 0);
        return indexes == null ? dflt : json.substring(indexes[0], indexes[1] + 1);
    }

    public static String removeObject(String json, String objectName) {
        int[] indexes = getBracketIndexes(objectName, json, '{', '}', 0);
        if (indexes != null) {
            // remove the entire object replacing it with a dummy field b/c getBracketIndexes doesn't consider
            // if there is or isn't another object after it, so I don't have to worry about it being/not being the last object
            json = json.substring(0, indexes[0]) + "\"rmvd" + objectName.hashCode() + "\":\"\"" + json.substring(indexes[1] + 1);
        }
        return json;
    }

    /**
     * Extract a list JSON object strings for list object name. Returns empty list '{}' if not found.
     * Assumes that there are no brackets '{' or '}' in the actual data.
     * @param objectName list object name
     * @param json source json
     * @return object json string
     */
    public static List<String> getObjectList(String objectName, String json) {
        List<String> items = new ArrayList<>();
        int[] indexes = getBracketIndexes(objectName, json, '[', ']', -1);
        if (indexes != null) {
            StringBuilder item = new StringBuilder();
            int depth = 0;
            for (int x = indexes[0] + 1; x < indexes[1]; x++) {
                char c = json.charAt(x);
                if (c == '{') {
                    item.append(c);
                    depth++;
                } else if (c == '}') {
                    item.append(c);
                    if (--depth == 0) {
                        items.add(item.toString());
                        item.setLength(0);
                    }
                } else if (depth > 0) {
                    item.append(c);
                }
            }
        }
        return items;
    }

    private static int[] getBracketIndexes(String objectName, String json, char start, char end, int fromIndex) {
        int[] result = new int[] {-1, -1};
        int objStart = json.indexOf(Q + objectName + Q, fromIndex);
        if (objStart != -1) {
            int startIx;
            if (fromIndex != -1) {
                int colonMark = json.indexOf(COLON, objStart) + 1;
                startIx = json.indexOf(start, colonMark);
                for (int x = colonMark; x < startIx; x++) {
                    char c = json.charAt(x);
                    if (!Character.isWhitespace(c)) {
                        return getBracketIndexes(objectName, json, start, end, colonMark);
                    }
                }
            }
            else {
                startIx = json.indexOf(start, objStart);
            }
            int depth = 1;
            for (int x = startIx + 1; x < json.length(); x++) {
                char c = json.charAt(x);
                if (c == start) {
                    depth++;
                }
                else if (c == end) {
                    if (--depth == 0) {
                        result[0] = startIx;
                        result[1] = x;
                        return result;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Get a map of objects
     * @param json the json
     * @return the map of json object strings by key
     */
    public static Map<String, String> getMapOfObjects(String json) {
        Map<String, String> map = new HashMap<>();
        int s1 = json.indexOf('"');
        while (s1 != -1) {
            int s2 = json.indexOf('"', s1 + 1);
            String key = json.substring(s1 + 1, s2).trim();
            int[] indexes = getBracketIndexes(key, json, '{', '}', s1);
            if (indexes != null) {
                map.put(key, json.substring(indexes[0], indexes[1] + 1));
                s1 = json.indexOf('"', indexes[1]);
            }
            else {
                s1 = -1;
            }
        }

        return map;
    }

    /**
     * Get a map of objects
     * @param json the json
     * @return the map of json object strings by key
     */
    public static Map<String, List<String>> getMapOfLists(String json) {
        Map<String, List<String>> map = new HashMap<>();
        int s1 = json.indexOf('"');
        while (s1 != -1) {
            int s2 = json.indexOf('"', s1 + 1);
            String key = json.substring(s1 + 1, s2).trim();
            int[] indexes = getBracketIndexes(key, json, '[', ']', s1);
            if (indexes != null) {
                map.put(key, toList(json.substring(indexes[0] + 1, indexes[1])));
                s1 = json.indexOf('"', indexes[1]);
            }
            else {
                s1 = -1;
            }
        }

        return map;
    }

    /**
     * Get a map of longs
     * @param json the json
     * @return the map of longs by key
     */
    public static Map<String, Long> getMapOfLongs(String json) {
        Map<String, Long> map = new HashMap<>();
        int s1 = json.indexOf('"');
        while (s1 != -1) {
            int s2 = json.indexOf('"', s1 + 1);
            int c1 = json.indexOf(':', s2);
            int c2 = json.indexOf(',', s2);
            if (c2 == -1) {
                c2 = json.indexOf('}', s2);
            }
            String key = json.substring(s1 + 1, s2).trim();
            long count = JsonUtils.safeParseLong(json.substring(c1 + 1, c2).trim(), 0);
            map.put(key, count);
            s1 = json.indexOf('"', c2);
        }
        return map;
    }

    /**
     * Extract a list strings for list object name. Returns empty array if not found.
     * Assumes that there are no brackets '{' or '}' in the actual data.
     * @param objectName object name
     * @param json source json
     * @return a string list, empty if no values are found.
     */
    public static List<String> getStringList(String objectName, String json) {
        String flat = json.replaceAll("\r", "").replaceAll("\n", "");
        Matcher m = string_array_pattern(objectName).matcher(flat);
        if (m.find()) {
            String arrayString = m.group(1);
            return toList(arrayString);
        }
        return new ArrayList<>();
    }

    private static List<String> toList(String arrayString) {
        List<String> list = new ArrayList<>();
        String[] raw = arrayString.split(",");
        for (String s : raw) {
            String cleaned = s.trim().replace("\"", "");
            if (cleaned.length() > 0) {
                list.add(jsonDecode(cleaned));
            }
        }
        return list;
    }

    /**
     * Extract a list longs for list object name. Returns empty array if not found.
     * @param objectName object name
     * @param json source json
     * @return a long list, empty if no values are found.
     */
    public static List<Long> getLongList(String objectName, String json) {
        String flat = json.replaceAll("\r", "").replaceAll("\n", "");
        List<Long> list = new ArrayList<>();
        Matcher m = number_array_pattern(objectName).matcher(flat);
        if (m.find()) {
            String arrayString = m.group(1);
            String[] raw = arrayString.split(",");

            for (String s : raw) {
                list.add(safeParseLong(s.trim()));
            }
        }
        return list;
    }

    /**
     * Extract a list durations for list object name. Returns empty array if not found.
     * @param objectName object name
     * @param json source json
     * @return a duration list, empty if no values are found.
     */
    public static List<Duration> getDurationList(String objectName, String json) {
        List<Long> longs = getLongList(objectName, json);
        List<Duration> list = new ArrayList<>(longs.size());
        for (Long l : longs) {
            list.add(Duration.ofNanos(l));
        }
        return list;
    }

    public static byte[] simpleMessageBody(String name, Number value) {
        return (OPENQ + name + QCOLON + value + CLOSE).getBytes();
    }

    public static byte[] simpleMessageBody(String name, String value) {
        return (OPENQ + name + QCOLONQ + value + Q + CLOSE).getBytes();
    }

    public static StringBuilder beginJson() {
        return new StringBuilder("{");
    }

    public static StringBuilder beginJsonPrefixed(String prefix) {
        return prefix == null ? beginJson()
                : new StringBuilder(prefix).append(NatsConstants.SPACE).append('{');
    }

    public static StringBuilder endJson(StringBuilder sb) {
        if (sb.charAt(sb.length() - 1) == ',') {
            // remove the trailing ','
            sb.setLength(sb.length() - 1);
        }
        sb.append("}");
        return sb;
    }

    public static StringBuilder beginFormattedJson() {
        return new StringBuilder("{\n    ");
    }

    public static String endFormattedJson(StringBuilder sb) {
        sb.setLength(sb.length()-1);
        sb.append("\n}");
        return sb.toString().replaceAll(",", ",\n    ");
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addField(StringBuilder sb, String fname, String value) {
        if (value != null && value.length() > 0) {
            sb.append(Q);
            jsonEncode(sb, fname);
            sb.append(QCOLONQ);
            jsonEncode(sb, value);
            sb.append(QCOMMA);
        }
    }

    /**
     * Appends a json field to a string builder. Empty and null string are added as value of empty string
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addFieldEvenEmpty(StringBuilder sb, String fname, String value) {
        if (value == null) {
            value = "";
        }
        sb.append(Q);
        jsonEncode(sb, fname);
        sb.append(QCOLONQ);
        jsonEncode(sb, value);
        sb.append(QCOMMA);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addField(StringBuilder sb, String fname, Boolean value) {
        if (value != null) {
            sb.append(Q);
            jsonEncode(sb, fname);
            sb.append(QCOLON).append(value ? "true" : "false").append(COMMA);
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addFldWhenTrue(StringBuilder sb, String fname, Boolean value) {
        if (value != null && value) {
            addField(sb, fname, true);
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addField(StringBuilder sb, String fname, Integer value) {
        if (value != null && value >= 0) {
            sb.append(Q);
            jsonEncode(sb, fname);
            sb.append(QCOLON).append(value).append(COMMA);
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addFieldWhenGtZero(StringBuilder sb, String fname, Integer value) {
        if (value != null && value > 0) {
            sb.append(Q);
            jsonEncode(sb, fname);
            sb.append(QCOLON).append(value).append(COMMA);
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addField(StringBuilder sb, String fname, Long value) {
        if (value != null && value >= 0) {
            sb.append(Q);
            jsonEncode(sb, fname);
            sb.append(QCOLON).append(value).append(COMMA);
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addFieldWhenGtZero(StringBuilder sb, String fname, Long value) {
        if (value != null && value > 0) {
            sb.append(Q);
            jsonEncode(sb, fname);
            sb.append(QCOLON).append(value).append(COMMA);
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value duration value
     */
    public static void addFieldAsNanos(StringBuilder sb, String fname, Duration value) {
        if (value != null && value != Duration.ZERO) {
            sb.append(Q);
            jsonEncode(sb, fname);
            sb.append(QCOLON).append(value.toNanos()).append(COMMA);
        }
    }

    /**
     * Appends a json object to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value JsonSerializable value
     */
    public static void addField(StringBuilder sb, String fname, JsonSerializable value) {
        if (value != null) {
            sb.append(Q);
            jsonEncode(sb, fname);
            sb.append(QCOLON).append(value.toJson()).append(COMMA);
        }
    }

    interface ListAdder<T> {
        void append(StringBuilder sb, T t);
    }

    /**
     * Appends a json field to a string builder.
     * @param <T> the list type
     * @param sb string builder
     * @param fname fieldname
     * @param list value list
     * @param adder implementation to add value, including it's quotes if required
     */
    public static <T> void _addList(StringBuilder sb, String fname, List<T> list, ListAdder<T> adder) {
        sb.append(Q);
        jsonEncode(sb, fname);
        sb.append("\":[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
                sb.append(COMMA);
            }
            adder.append(sb, list.get(i));
        }
        sb.append("],");
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param strArray field value
     */
    public static void addStrings(StringBuilder sb, String fname, String[] strArray) {
        if (strArray != null && strArray.length > 0) {
            addStrings(sb, fname, Arrays.asList(strArray));
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param strings field value
     */
    public static void addStrings(StringBuilder sb, String fname, List<String> strings) {
        if (strings != null && strings.size() > 0) {
            _addList(sb, fname, strings, (sbs, s) -> {
                sb.append(Q);
                jsonEncode(sb, s);
                sb.append(Q);
            });
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param jsons field value
     */
    public static void addJsons(StringBuilder sb, String fname, List<? extends JsonSerializable> jsons) {
        if (jsons != null && jsons.size() > 0) {
            _addList(sb, fname, jsons, (sbs, s) -> sbs.append(s.toJson()));
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param durations list of durations
     */
    public static void addDurations(StringBuilder sb, String fname, List<Duration> durations) {
        if (durations != null && durations.size() > 0) {
            _addList(sb, fname, durations, (sbs, dur) -> sbs.append(dur.toNanos()));
        }
    }

    /**
     * Appends a date/time to a string builder as a rfc 3339 formatted field.
     * @param sb string builder
     * @param fname fieldname
     * @param zonedDateTime field value
     */
    public static void addField(StringBuilder sb, String fname, ZonedDateTime zonedDateTime) {
        if (zonedDateTime != null) {
            sb.append(Q);
            jsonEncode(sb, fname);
            sb.append(QCOLONQ)
                    .append(DateTimeUtils.toRfc3339(zonedDateTime)).append(QCOMMA);
        }
    }

    public static void addField(StringBuilder sb, String fname, Headers headers) {
        if (headers != null && headers.size() > 0) {
            sb.append(Q);
            jsonEncode(sb, fname);
            sb.append("\":{");
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                addStrings(sb, entry.getKey(), entry.getValue());
            }
            endJson(sb);
            sb.append(",");
        }
    }

    public static String readString(String json, Pattern pattern) {
        return readString(json, pattern, null);
    }

    public static String readString(String json, Pattern pattern, String dflt) {
        Matcher m = pattern.matcher(json);
        return m.find() ? jsonDecode(m.group(1)) : dflt;
    }

    public static byte[] readBytes(String json, Pattern pattern) {
        String s = readString(json, pattern, null);
        return s == null ? null : s.getBytes(StandardCharsets.US_ASCII);
    }

    public static byte[] readBase64(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        String b64 = m.find() ? m.group(1) : null;
        return b64 == null ? null : Base64.getDecoder().decode(b64);
    }

    public static boolean readBoolean(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() && Boolean.parseBoolean(m.group(1));
    }

    public static Boolean readBoolean(String json, Pattern pattern, Boolean dflt) {
        Matcher m = pattern.matcher(json);
        if (m.find()) {
            return Boolean.parseBoolean(m.group(1));
        }
        return dflt;
    }

    public static Integer readInteger(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : null;
    }

    public static int readInt(String json, Pattern pattern, int dflt) {
        Matcher m = pattern.matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : dflt;
    }

    public static void readInt(String json, Pattern pattern, IntConsumer c) {
        Matcher m = pattern.matcher(json);
        if (m.find()) {
            c.accept(Integer.parseInt(m.group(1)));
        }
    }

    public static Long readLong(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() ? safeParseLong(m.group(1)) : null;
    }

    public static long readLong(String json, Pattern pattern, long dflt) {
        Matcher m = pattern.matcher(json);
        return m.find() ? safeParseLong(m.group(1), dflt) : dflt;
    }

    public static void readLong(String json, Pattern pattern, LongConsumer c) {
        Matcher m = pattern.matcher(json);
        if (m.find()) {
            Long l = safeParseLong(m.group(1));
            if (l != null) {
                c.accept(l);
            }
        }
    }

    public static Long safeParseLong(String s) {
        try {
            return Long.parseLong(s);
        }
        catch (Exception e1) {
            try {
                return Long.parseUnsignedLong(s);
            }
            catch (Exception e2) {
                return null;
            }
        }
    }

    public static long safeParseLong(String s, long dflt) {
        Long l = safeParseLong(s);
        return l == null ? dflt : l;
    }

    public static ZonedDateTime readDate(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() ? DateTimeUtils.parseDateTime(m.group(1)) : null;
    }

    public static Duration readNanos(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() ? Duration.ofNanos(Long.parseLong(m.group(1))) : null;
    }

    public static Duration readNanos(String json, Pattern pattern, Duration dflt) {
        Matcher m = pattern.matcher(json);
        return m.find() ? Duration.ofNanos(Long.parseLong(m.group(1))) : dflt;
    }

    public static void readNanos(String json, Pattern pattern, Consumer<Duration> c) {
        Matcher m = pattern.matcher(json);
        if (m.find()) {
            c.accept(Duration.ofNanos(Long.parseLong(m.group(1))));
        }
    }

    public static String normalize(String s) {
        return Character.toString(s.charAt(0)).toUpperCase() + s.substring(1).toLowerCase();
    }

    public static String objectString(String name, Object o) {
        if (o == null) {
            return name + "=null";
        }
        if (o instanceof SequenceInfo) {
            return o.toString().replace("SequenceInfo", name);
        }
        if (o instanceof SequencePair) {
            return o.toString().replace("SequencePair", name);
        }
        return o.toString();
    }

    // ----------------------------------------------------------------------------------------------------
    // PRINT UTILS
    // ----------------------------------------------------------------------------------------------------

    private static final String INDENT = "                                ";
    private static String indent(int level) {
        return level == 0 ? "" : INDENT.substring(0, level * 4);
    }

    public static String getFormatted(Object o) {
        StringBuilder sb = new StringBuilder();
        int level = 0;
        boolean indentNext = true;
        String s = o.toString();
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c == '{') {
                sb.append(c).append('\n');
                ++level;
                indentNext = true;
            }
            else if (c == '}') {
                sb.append('\n').append(indent(--level)).append(c);
            }
            else if (c == ',') {
                sb.append('\n');
                indentNext = true;
            }
            else {
                if (indentNext) {
                    if (c != ' ') {
                        sb.append(indent(level)).append(c);
                        indentNext = false;
                    }
                }
                else {
                    sb.append(c);
                }
            }
        }
        return sb.toString();
    }

    public static void printFormatted(Object o) {
        System.out.println(getFormatted(o));
    }
}
