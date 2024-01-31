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

import static io.nats.client.support.JsonWriteUtils.*;
import static io.nats.client.support.NatsConstants.COLON;

/**
 * Internal json reading and writing helpers.
 * @deprecated This class has been extracted to the io.nats.client.support.JsonWriteUtils class in the <a href="https://github.com/nats-io/json.java">json.java</a> library.
 */
@Deprecated
public abstract class JsonUtils {
    public static final String EMPTY_JSON = "{}";

    private static final String STRING_RE  = "\"(.+?)\"";
    private static final String BOOLEAN_RE =  "(true|false)";
    private static final String INTEGER_RE =  "(-?\\d+)";
    private static final String STRING_ARRAY_RE = "\\[\\s*(\".+?\")\\s*\\]";
    private static final String NUMBER_ARRAY_RE = "\\[\\s*(.+?)\\s*\\]";
    private static final String BEFORE_FIELD_RE = "\"";
    private static final String AFTER_FIELD_RE = "\"\\s*:\\s*";

    public static final String OPENQ = "{\"";
    public static final String CLOSE = "}";

    private JsonUtils() {} /* ensures cannot be constructed */

    // ----------------------------------------------------------------------------------------------------
    // BUILD A STRING OF JSON
    // ----------------------------------------------------------------------------------------------------
    @Deprecated
    public static StringBuilder beginJson() {
        return JsonWriteUtils.beginJson();
    }

    @Deprecated
    public static StringBuilder beginArray() {
        return JsonWriteUtils.beginArray();
    }

    @Deprecated
    public static StringBuilder beginJsonPrefixed(String prefix) {
        return JsonWriteUtils.beginJsonPrefixed(prefix);
    }

    @Deprecated
    public static StringBuilder endJson(StringBuilder sb) {
        return JsonWriteUtils.endJson(sb);
    }

    @Deprecated
    public static StringBuilder endArray(StringBuilder sb) {
        return JsonWriteUtils.endArray(sb);
    }

    @Deprecated
    public static StringBuilder beginFormattedJson() {
        return JsonWriteUtils.beginFormattedJson();
    }

    @Deprecated
    public static String endFormattedJson(StringBuilder sb) {
        return JsonWriteUtils.endFormattedJson(sb);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param json raw json
     */
    @Deprecated
    public static void addRawJson(StringBuilder sb, String fname, String json) {
        JsonWriteUtils.addRawJson(sb, fname, json);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    @Deprecated
    public static void addField(StringBuilder sb, String fname, String value) {
        JsonWriteUtils.addField(sb, fname, value);
    }

    /**
     * Appends a json field to a string builder. Empty and null string are added as value of empty string
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    @Deprecated
    public static void addFieldEvenEmpty(StringBuilder sb, String fname, String value) {
        JsonWriteUtils.addFieldEvenEmpty(sb, fname, value);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    @Deprecated
    public static void addField(StringBuilder sb, String fname, Boolean value) {
        JsonWriteUtils.addField(sb, fname, value);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    @Deprecated
    public static void addFldWhenTrue(StringBuilder sb, String fname, Boolean value) {
        JsonWriteUtils.addFldWhenTrue(sb, fname, value);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    @Deprecated
    public static void addField(StringBuilder sb, String fname, Integer value) {
        JsonWriteUtils.addField(sb, fname, value);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    @Deprecated
    public static void addFieldWhenGtZero(StringBuilder sb, String fname, Integer value) {
        JsonWriteUtils.addFieldWhenGtZero(sb, fname, value);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    @Deprecated
    public static void addField(StringBuilder sb, String fname, Long value) {
        JsonWriteUtils.addField(sb, fname, value);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    @Deprecated
    public static void addFieldWhenGtZero(StringBuilder sb, String fname, Long value) {
        JsonWriteUtils.addFieldWhenGtZero(sb, fname, value);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    @Deprecated
    public static void addFieldWhenGteMinusOne(StringBuilder sb, String fname, Long value) {
        JsonWriteUtils.addFieldWhenGteMinusOne(sb, fname, value);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     * @param gt the number the value must be greater than
     */
    @Deprecated
    public static void addFieldWhenGreaterThan(StringBuilder sb, String fname, Long value, long gt) {
        JsonWriteUtils.addFieldWhenGreaterThan(sb, fname, value, gt);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value duration value
     */
    @Deprecated
    public static void addFieldAsNanos(StringBuilder sb, String fname, Duration value) {
        JsonWriteUtils.addFieldAsNanos(sb, fname, value);
    }

    /**
     * Appends a json object to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value JsonSerializable value
     */
    @Deprecated
    public static void addField(StringBuilder sb, String fname, JsonSerializable value) {
        JsonWriteUtils.addField(sb, fname, value);
    }

    @Deprecated
    public static void addField(StringBuilder sb, String fname, Map<String, String> map) {
        JsonWriteUtils.addField(sb, fname, map);
    }

    @SuppressWarnings("rawtypes")
    @Deprecated
    public static void addEnumWhenNot(StringBuilder sb, String fname, Enum e, Enum dontAddIfThis) {
        JsonWriteUtils.addEnumWhenNot(sb, fname, e, dontAddIfThis);
    }

    public interface ListAdder<T> {
        void append(StringBuilder sb, T t);
    }

    /**
     * Appends a json field to a string builder.
     * @param <T> the list type
     * @param sb string builder
     * @param fname fieldname
     * @param list value list
     * @param adder implementation to add value, including its quotes if required
     */
    @Deprecated
    public static <T> void _addList(StringBuilder sb, String fname, List<T> list, ListAdder<T> adder) {
        sb.append(Q);
        Encoding.jsonEncode(sb, fname);
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
     * @param strings field value
     */
    @Deprecated
    public static void addStrings(StringBuilder sb, String fname, String[] strings) {
        JsonWriteUtils.addStrings(sb, fname, strings);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param strings field value
     */
    @Deprecated
    public static void addStrings(StringBuilder sb, String fname, List<String> strings) {
        JsonWriteUtils.addStrings(sb, fname, strings);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param jsons field value
     */
    @Deprecated
    public static void addJsons(StringBuilder sb, String fname, List<? extends JsonSerializable> jsons) {
        JsonWriteUtils.addJsons(sb, fname, jsons);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param durations list of durations
     */
    @Deprecated
    public static void addDurations(StringBuilder sb, String fname, List<Duration> durations) {
        JsonWriteUtils.addDurations(sb, fname, durations);
    }

    /**
     * Appends a date/time to a string builder as a rfc 3339 formatted field.
     * @param sb string builder
     * @param fname fieldname
     * @param zonedDateTime field value
     */
    @Deprecated
    public static void addField(StringBuilder sb, String fname, ZonedDateTime zonedDateTime) {
        JsonWriteUtils.addField(sb, fname, zonedDateTime);
    }

    public static void addField(StringBuilder sb, String fname, Headers headers) {
        if (headers != null && !headers.isEmpty()) {
            sb.append(Q);
            Encoding.jsonEncode(sb, fname);
            sb.append("\":{");
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                addStrings(sb, entry.getKey(), entry.getValue());
            }
            endJson(sb);
            sb.append(",");
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // PRINT UTILS
    // ----------------------------------------------------------------------------------------------------
    @Deprecated
    public static String normalize(String s) {
        return Character.toString(s.charAt(0)).toUpperCase() + s.substring(1).toLowerCase();
    }

    @Deprecated
    public static String toKey(Class<?> c) {
        return JsonWriteUtils.toKey(c);
    }

    @Deprecated
    public static String objectString(String name, Object o) {
        if (o == null) {
            return name + "=null";
        }
        return o.toString();
    }

    private static final String INDENT = "                                        ";
    private static String indent(int level) {
        return level == 0 ? "" : INDENT.substring(0, level * 4);
    }

    /**
     * This isn't perfect but good enough for debugging
     * @param o the object
     * @return the formatted string
     */
    public static String getFormatted(Object o) {
        StringBuilder sb = new StringBuilder();
        int level = 0;
        int arrayLevel = 0;
        boolean lastWasClose = false;
        boolean indentNext = true;
        String indent = "";
        String s = o.toString();
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c == '{') {
                if (arrayLevel > 0 && lastWasClose) {
                    sb.append(indent);
                }
                sb.append(c).append('\n');
                indent = indent(++level);
                indentNext = true;
                lastWasClose = false;
            }
            else if (c == '}') {
                indent = indent(--level);
                sb.append('\n').append(indent).append(c);
                lastWasClose = true;
            }
            else if (c == ',') {
                sb.append(",\n");
                indentNext = true;
            }
            else {
                if (c == '[') {
                    arrayLevel++;
                }
                else if (c == ']') {
                    arrayLevel--;
                }
                if (indentNext) {
                    if (c != ' ') {
                        sb.append(indent).append(c);
                        indentNext = false;
                    }
                }
                else {
                    sb.append(c);
                }
                lastWasClose = lastWasClose && Character.isWhitespace(c);
            }
        }
        return sb.toString();
    }

    public static void printFormatted(Object o) {
        System.out.println(getFormatted(o));
    }

    // ----------------------------------------------------------------------------------------------------
    // SAFE NUMBER PARSING HELPERS
    // ----------------------------------------------------------------------------------------------------
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

    // ----------------------------------------------------------------------------------------------------
    // REGEX READING OF JSON. DEPRECATED PREFER USING THE JsonParser
    // ----------------------------------------------------------------------------------------------------
    @Deprecated
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

    @Deprecated
    public static Pattern string_pattern(String field) {
        return buildPattern(field, STRING_RE);
    }

    @Deprecated
    public static Pattern number_pattern(String field) {
        return integer_pattern(field);
    }

    @Deprecated
    public static Pattern integer_pattern(String field) {
        return buildPattern(field, INTEGER_RE);
    }

    @Deprecated
    public static Pattern boolean_pattern(String field) {
        return buildPattern(field, BOOLEAN_RE);
    }

    @Deprecated
    public static Pattern string_array_pattern(String field) {
        return buildPattern(field, STRING_ARRAY_RE);
    }

    @Deprecated
    public static Pattern number_array_pattern(String field) {
        return buildPattern(field, NUMBER_ARRAY_RE);
    }

    /**
     * Builds a json parsing pattern
     * @param fieldName name of the field
     * @param type type of the field.
     * @return pattern.
     */
    @Deprecated
    public static Pattern buildPattern(String fieldName, FieldType type) {
        return buildPattern(fieldName, type.re);
    }

    @Deprecated
    public static Pattern buildPattern(String fieldName, String typeRE) {
        return Pattern.compile(BEFORE_FIELD_RE + fieldName + AFTER_FIELD_RE + typeRE, Pattern.CASE_INSENSITIVE);
    }

    /**
     * Extract a JSON object string by object name. Returns empty object '{}' if not found.
     * @param objectName object name
     * @param json source json
     * @return object json string
     */
    @Deprecated
    public static String getJsonObject(String objectName, String json) {
        return getJsonObject(objectName, json, EMPTY_JSON);
    }

    @Deprecated
    public static String getJsonObject(String objectName, String json, String dflt) {
        int[] indexes = getBracketIndexes(objectName, json, '{', '}', 0);
        return indexes == null ? dflt : json.substring(indexes[0], indexes[1] + 1);
    }

    @Deprecated
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
    @Deprecated
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
    @Deprecated
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
    @Deprecated
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
    @Deprecated
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
            long count = safeParseLong(json.substring(c1 + 1, c2).trim(), 0);
            map.put(key, count);
            s1 = json.indexOf('"', c2);
        }
        return map;
    }

    /**
     * Extract a list strings for list object name. Returns empty array if not found.
     * Assumes that there are no brackets '{' or '}' in the actual data.
     * @deprecated Prefer using the {@link JsonParser}
     * @param objectName object name
     * @param json source json
     * @return a string list, empty if no values are found.
     */
    @Deprecated
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
            if (!cleaned.isEmpty()) {
                list.add(Encoding.jsonDecode(cleaned));
            }
        }
        return list;
    }

    /**
     * Extract a list longs for list object name. Returns empty array if not found.
     * @deprecated Prefer using the {@link JsonParser}
     * @param objectName object name
     * @param json source json
     * @return a long list, empty if no values are found.
     */
    @Deprecated
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
    @Deprecated
    public static List<Duration> getDurationList(String objectName, String json) {
        List<Long> longs = getLongList(objectName, json);
        List<Duration> list = new ArrayList<>(longs.size());
        for (Long l : longs) {
            list.add(Duration.ofNanos(l));
        }
        return list;
    }

    @Deprecated
    public static byte[] simpleMessageBody(String name, Number value) {
        return (OPENQ + name + QCOLON + value + CLOSE).getBytes();
    }

    @Deprecated
    public static byte[] simpleMessageBody(String name, String value) {
        return (OPENQ + name + QCOLONQ + value + Q + CLOSE).getBytes();
    }

    @Deprecated
    public static String readString(String json, Pattern pattern) {
        return readString(json, pattern, null);
    }

    @Deprecated
    public static String readString(String json, Pattern pattern, String dflt) {
        Matcher m = pattern.matcher(json);
        return m.find() ? Encoding.jsonDecode(m.group(1)) : dflt;
    }

    @Deprecated
    public static String readStringMayHaveQuotes(String json, String field, String dflt) {
        String jfield = "\"" + field + "\"";
        int at = json.indexOf(jfield);
        if (at != -1) {
            at = json.indexOf('"', at + jfield.length());
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = json.charAt(++at);
                if (c == '\\') {
                    char c2 = json.charAt(++at);
                    if (c2 == '"') {
                        sb.append('"');
                    }
                    else {
                        sb.append(c);
                        sb.append(c2);
                    }
                }
                else if (c == '"') {
                    break;
                }
                else {
                    sb.append(c);
                }
            }
            return Encoding.jsonDecode(sb.toString());
        }
        return dflt;
    }

    @Deprecated
    public static byte[] readBytes(String json, Pattern pattern) {
        String s = readString(json, pattern, null);
        return s == null ? null : s.getBytes(StandardCharsets.US_ASCII);
    }

    @Deprecated
    public static byte[] readBase64(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        String b64 = m.find() ? m.group(1) : null;
        return b64 == null ? null : Base64.getDecoder().decode(b64);
    }

    @Deprecated
    public static boolean readBoolean(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() && Boolean.parseBoolean(m.group(1));
    }

    @Deprecated
    public static Boolean readBoolean(String json, Pattern pattern, Boolean dflt) {
        Matcher m = pattern.matcher(json);
        if (m.find()) {
            return Boolean.parseBoolean(m.group(1));
        }
        return dflt;
    }

    @Deprecated
    public static Integer readInteger(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : null;
    }

    @Deprecated
    public static int readInt(String json, Pattern pattern, int dflt) {
        Matcher m = pattern.matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : dflt;
    }

    @Deprecated
    public static void readInt(String json, Pattern pattern, IntConsumer c) {
        Matcher m = pattern.matcher(json);
        if (m.find()) {
            c.accept(Integer.parseInt(m.group(1)));
        }
    }

    @Deprecated
    public static Long readLong(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() ? safeParseLong(m.group(1)) : null;
    }

    @Deprecated
    public static long readLong(String json, Pattern pattern, long dflt) {
        Matcher m = pattern.matcher(json);
        return m.find() ? safeParseLong(m.group(1), dflt) : dflt;
    }

    @Deprecated
    public static void readLong(String json, Pattern pattern, LongConsumer c) {
        Matcher m = pattern.matcher(json);
        if (m.find()) {
            Long l = safeParseLong(m.group(1));
            if (l != null) {
                c.accept(l);
            }
        }
    }

    @Deprecated
    public static ZonedDateTime readDate(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() ? DateTimeUtils.parseDateTime(m.group(1)) : null;
    }

    @Deprecated
    public static Duration readNanos(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() ? Duration.ofNanos(Long.parseLong(m.group(1))) : null;
    }

    @Deprecated
    public static Duration readNanos(String json, Pattern pattern, Duration dflt) {
        Matcher m = pattern.matcher(json);
        return m.find() ? Duration.ofNanos(Long.parseLong(m.group(1))) : dflt;
    }

    @Deprecated
    public static void readNanos(String json, Pattern pattern, Consumer<Duration> c) {
        Matcher m = pattern.matcher(json);
        if (m.find()) {
            c.accept(Duration.ofNanos(Long.parseLong(m.group(1))));
        }
    }

    @Deprecated
    public static <T> boolean listEquals(List<T> l1, List<T> l2)
    {
        return Validator.listEquals(l1, l2);
    }

    @Deprecated
    public static boolean mapEquals(Map<String, String> map1, Map<String, String> map2) {
        return Validator.mapEquals(map1, map2);
    }
}
