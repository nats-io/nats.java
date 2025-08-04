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

import static io.nats.client.support.DateTimeUtils.DEFAULT_TIME;
import static io.nats.client.support.Encoding.jsonDecode;
import static io.nats.client.support.Encoding.jsonEncode;
import static io.nats.client.support.JsonValueUtils.instance;
import static io.nats.client.support.NatsConstants.COLON;

/**
 * Internal json parsing helpers.
 * Read helpers deprecated Prefer using the {@link JsonParser}
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

    // ----------------------------------------------------------------------------------------------------
    // BUILD A STRING OF JSON
    // ----------------------------------------------------------------------------------------------------
    public static StringBuilder beginJson() {
        return new StringBuilder("{");
    }

    public static StringBuilder beginArray() {
        return new StringBuilder("[");
    }

    public static StringBuilder beginJsonPrefixed(String prefix) {
        return prefix == null ? beginJson()
            : new StringBuilder(prefix).append('{');
    }

    public static StringBuilder endJson(StringBuilder sb) {
        int lastIndex = sb.length() - 1;
        if (sb.charAt(lastIndex) == ',') {
            sb.setCharAt(lastIndex, '}');
            return sb;
        }
        sb.append("}");
        return sb;
    }

    public static StringBuilder endArray(StringBuilder sb) {
        int lastIndex = sb.length() - 1;
        if (sb.charAt(lastIndex) == ',') {
            sb.setCharAt(lastIndex, ']');
            return sb;
        }
        sb.append("]");
        return sb;
    }

    public static StringBuilder beginFormattedJson() {
        return new StringBuilder("{\n    ");
    }

    public static String endFormattedJson(StringBuilder sb) {
        sb.setLength(sb.length()-1);
        sb.append("\n}");
        return sb.toString().replace(",", ",\n    ");
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param json raw json
     */
    public static void addRawJson(StringBuilder sb, String fname, String json) {
        if (json != null && json.length() > 0) {
            sb.append(Q);
            jsonEncode(sb, fname);
            sb.append(QCOLON);
            sb.append(json);
            sb.append(COMMA);
        }
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
     * @param value field value
     */
    public static void addFieldWhenGteMinusOne(StringBuilder sb, String fname, Long value) {
        if (value != null && value >= -1) {
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
     * @param gt the number the value must be greater than
     */
    public static void addFieldWhenGreaterThan(StringBuilder sb, String fname, Long value, long gt) {
        if (value != null && value > gt) {
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
        if (value != null && !value.isZero() && !value.isNegative()) {
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

    public static void addField(StringBuilder sb, String fname, Map<String, String> map) {
        if (map != null && map.size() > 0) {
            addField(sb, fname, instance(map));
        }
    }

    @SuppressWarnings("rawtypes")
    public static void addEnumWhenNot(StringBuilder sb, String fname, Enum e, Enum dontAddIfThis) {
        if (e != null && e != dontAddIfThis) {
            addField(sb, fname, e.toString());
        }
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
     * Appends an empty JSON array to a string builder with the specified field name.
     * @param sb the string builder to append to
     * @param fname the name of the JSON field
     */
    private static void _addEmptyList(StringBuilder sb, String fname) {
        sb.append(Q);
        jsonEncode(sb, fname);
        sb.append("\":[],");
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param strings field value
     */
    public static void addStrings(StringBuilder sb, String fname, String[] strings) {
        if (strings != null && strings.length > 0) {
            _addStrings(sb, fname, Arrays.asList(strings));
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
            _addStrings(sb, fname, strings);
        }
    }

    private static void _addStrings(StringBuilder sb, String fname, List<String> strings) {
        _addList(sb, fname, strings, (sbs, s) -> {
            sb.append(Q);
            jsonEncode(sb, s);
            sb.append(Q);
        });
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param jsons field value
     */
    public static void addJsons(StringBuilder sb, String fname, List<? extends JsonSerializable> jsons) {
        addJsons(sb, fname, jsons, false);
    }

    /**
     * Appends a json field to a string builder and the additional flag to indicate if an empty list to be added.
     * @param sb string builder
     * @param fname fieldname
     * @param jsons field value
     * @param addEmptyList flag to indicate if an empty list to be added
     */
    public static void addJsons(StringBuilder sb, String fname, List<? extends JsonSerializable> jsons, boolean addEmptyList) {
        if (jsons != null && !jsons.isEmpty()) {
            _addList(sb, fname, jsons, (sbs, s) -> sbs.append(s.toJson()));
        }
        else if (addEmptyList) {
            _addEmptyList(sb, fname);
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
        if (zonedDateTime != null && !DEFAULT_TIME.equals(zonedDateTime)) {
            sb.append(Q);
            jsonEncode(sb, fname);
            sb.append(QCOLONQ)
                .append(DateTimeUtils.toRfc3339(zonedDateTime))
                .append(QCOMMA);
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

    // ----------------------------------------------------------------------------------------------------
    // PRINT UTILS
    // ----------------------------------------------------------------------------------------------------
    @Deprecated
    public static String normalize(String s) {
        return Character.toString(s.charAt(0)).toUpperCase() + s.substring(1).toLowerCase();
    }

    public static String toKey(Class<?> c) {
        return "\"" + c.getSimpleName() + "\":";
    }

    @Deprecated
    public static String objectString(String name, Object o) {
        if (o == null) {
            return name + "=null";
        }
        return o.toString();
    }

    private static final int INDENT_WIDTH = 4;
    private static final String INDENT = "                                        ";
    private static String indent(int level) {
        return level <= 0 ? "" : INDENT.substring(0, level * INDENT_WIDTH);
    }

    public static String getFormatted(Object o) {
        String s = o.toString();
        String newline = System.lineSeparator();

        StringBuilder sb = new StringBuilder();
        boolean begin_quotes = false;

        boolean opened = false;
        int indentLevel = 0;
        String indent = "";
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);

            if (c == '\"') {
                if (opened) {
                    sb.append(newline).append(indent);
                    opened = false;
                }
                sb.append(c);
                begin_quotes = !begin_quotes;
                continue;
            }

            if (!begin_quotes) {
                switch (c) {
                    case '{':
                    case '[':
                        sb.append(c);
                        opened = true;
                        indent = indent(++indentLevel);
                        continue;
                    case '}':
                    case ']':
                        indent = indent(--indentLevel);
                        sb.append(newline).append(indent);
                        sb.append(c);
                        opened = false;
                        continue;
                    case ':':
                        sb.append(c).append(" ");
                        continue;
                    case ',':
                        sb.append(c).append(newline).append(indentLevel > 0 ? indent : "");
                        continue;
                    default:
                        if (Character.isWhitespace(c)) continue;
                        if (opened) {
                            sb.append(newline).append(indent);
                            opened = false;
                        }
                }
            }

            sb.append(c).append(c == '\\' ? "" + s.charAt(++x) : "");
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
        String flat = json.replace("\r", "").replace("\n", "");
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
     * @deprecated Prefer using the {@link JsonParser}
     * @param objectName object name
     * @param json source json
     * @return a long list, empty if no values are found.
     */
    @Deprecated
    public static List<Long> getLongList(String objectName, String json) {
        String flat = json.replace("\r", "").replace("\n", "");
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
        return m.find() ? jsonDecode(m.group(1)) : dflt;
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
            return jsonDecode(sb.toString());
        }
        return dflt;
    }

    @Deprecated
    public static byte[] readBytes(String json, Pattern pattern) {
        String s = readString(json, pattern, null);
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
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

    public static <T> boolean listEquals(List<T> l1, List<T> l2)
    {
        if (l1 == null)
        {
            return l2 == null;
        }

        if (l2 == null)
        {
            return false;
        }

        return l1.equals(l2);
    }

    public static boolean mapEquals(Map<String, String> map1, Map<String, String> map2) {
        if (map1 == null) {
            return map2 == null;
        }
        if (map2 == null || map1.size() != map2.size()) {
            return false;
        }
        for (String key : map1.keySet()) {
            if (!Objects.equals(map1.get(key), map2.get(key))) {
                return false;
            }
        }
        return true;
    }
}
