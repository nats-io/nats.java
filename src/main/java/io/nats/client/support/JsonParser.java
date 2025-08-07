// Copyright 2023 The NATS Authors
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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class that can parse JSON to a JsonValue
 */
public class JsonParser {

    /**
     * Option for parsing.
     */
    public enum Option {
        /**
         * Keep nulls when parsing. Usually ignored
         */
        KEEP_NULLS
    }

    public static final String INVALID_VALUE = "Invalid value.";
    private static final boolean[] IS_DELIMITER = new boolean[128];

    static {
        for (char c : ",:]}/\\\"[{;=#".toCharArray()) {
            IS_DELIMITER[c] = true;
        }
    }

    private final StringBuilder workBuffer = new StringBuilder(64);

    /**
     * Parse JSON from a char array
     * @param json the JSON
     * @return the JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parse(char @Nullable [] json) throws JsonParseException {
        return new JsonParser(json, 0).parse();
    }

    /**
     * Parse JSON from a char array, starting at the index
     * @param json the JSON
     * @param startIndex the starting index in the array
     * @return the JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parse(char @Nullable [] json, int startIndex) throws JsonParseException {
        return new JsonParser(json, startIndex).parse();
    }

    /**
     * Parse JSON from a char array
     * @param json the JSON
     * @param options options for how to parse
     * @return the JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parse(char @Nullable [] json, @Nullable Option... options) throws JsonParseException {
        return new JsonParser(json, 0, options).parse();
    }

    /**
     * Parse JSON from a char array
     * @param json the JSON
     * @param startIndex the starting index in the array
     * @param options options for how to parse
     * @return the JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parse(char @Nullable [] json, int startIndex, @Nullable Option... options) throws JsonParseException {
        return new JsonParser(json, startIndex, options).parse();
    }


    /**
     * Parse JSON from a String
     * @param json the JSON
     * @return the JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parse(String json) throws JsonParseException {
        return new JsonParser(json.toCharArray(), 0).parse();
    }


    /**
     * Parse JSON from a String
     * @param json the JSON
     * @param startIndex the starting index in the string
     * @return the JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parse(String json, int startIndex) throws JsonParseException {
        return new JsonParser(json.toCharArray(), startIndex).parse();
    }

    /**
     * Parse JSON from a String
     * @param json the JSON
     * @param options options for how to parse
     * @return the JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parse(String json, @Nullable Option... options) throws JsonParseException {
        return new JsonParser(json.toCharArray(), 0, options).parse();
    }

    /**
     * Parse JSON from a String
     * @param json the JSON
     * @param startIndex the starting index in the array
     * @param options options for how to parse
     * @return the JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parse(String json, int startIndex, @Nullable Option... options) throws JsonParseException {
        return new JsonParser(json.toCharArray(), startIndex, options).parse();
    }

    /**
     * Parse JSON from a byte array
     * @param json the JSON
     * @return the JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parse(byte[] json) throws JsonParseException {
        return new JsonParser(bytesToChars(json), 0).parse();
    }

    /**
     * Parse JSON from a byte array
     * @param json the JSON
     * @param startIndex the starting index in the array
     * @return the JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parse(byte[] json, int startIndex) throws JsonParseException {
        return new JsonParser(bytesToChars(json), startIndex).parse();
    }

    /**
     * Parse JSON from a byte array
     * @param json the JSON
     * @param options options for how to parse
     * @return the JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parse(byte[] json, @Nullable Option... options) throws JsonParseException {
        return new JsonParser(bytesToChars(json), 0, options).parse();
    }

    /**
     * Parse JSON from a byte array
     * @param json the JSON
     * @param startIndex the starting index in the array
     * @param options options for how to parse
     * @return the JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parse(byte[] json, int startIndex, @Nullable Option... options) throws JsonParseException {
        return new JsonParser(bytesToChars(json), startIndex, options).parse();
    }

    /**
     * Parse JSON from a char array
     * @param json the JSON
     * @return the JsonValue
     * @throws RuntimeException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parseUnchecked(char @Nullable [] json) {
        try { return parse(json); }
        catch (JsonParseException j) { throw new RuntimeException(j); }
    }

    /**
     * Parse JSON from a char array
     * @param json the JSON
     * @param startIndex the starting index in the array
     * @return the JsonValue
     * @throws RuntimeException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parseUnchecked(char @Nullable [] json, int startIndex) {
        try { return parse(json, startIndex); }
        catch (JsonParseException j) { throw new RuntimeException(j); }
    }

    /**
     * Parse JSON from a char array
     * @param json the JSON
     * @param options options for how to parse
     * @return the JsonValue
     * @throws RuntimeException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parseUnchecked(char @Nullable [] json, @Nullable Option... options) {
        try { return parse(json, options); }
        catch (JsonParseException j) { throw new RuntimeException(j); }
    }

    /**
     * Parse JSON from a char array
     * @param json the JSON
     * @param startIndex the starting index in the array
     * @param options options for how to parse
     * @return the JsonValue
     * @throws RuntimeException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parseUnchecked(char @Nullable [] json, int startIndex, @Nullable Option... options) {
        try { return parse(json, startIndex, options); }
        catch (JsonParseException j) { throw new RuntimeException(j); }
    }

    /**
     * Parse JSON from a String
     * @param json the JSON
     * @return the JsonValue
     * @throws RuntimeException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parseUnchecked(String json) {
        try { return parse(json); }
        catch (JsonParseException j) { throw new RuntimeException(j); }
    }

    /**
     * Parse JSON from a String
     * @param json the JSON
     * @param startIndex the starting index in the string
     * @return the JsonValue
     * @throws RuntimeException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parseUnchecked(String json, int startIndex) {
        try { return parse(json, startIndex); }
        catch (JsonParseException j) { throw new RuntimeException(j); }
    }

    /**
     * Parse JSON from a String
     * @param json the JSON
     * @param options options for how to parse
     * @return the JsonValue
     * @throws RuntimeException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parseUnchecked(String json, @Nullable Option... options) {
        try { return parse(json, options); }
        catch (JsonParseException j) { throw new RuntimeException(j); }
    }

    /**
     * Parse JSON from a String
     * @param json the JSON
     * @param startIndex the starting index in the string
     * @param options options for how to parse
     * @return the JsonValue
     * @throws RuntimeException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parseUnchecked(String json, int startIndex, @Nullable Option... options) {
        try { return parse(json, startIndex, options); }
        catch (JsonParseException j) { throw new RuntimeException(j); }
    }

    /**
     * Parse JSON from a byte array
     * @param json the JSON
     * @return the JsonValue
     * @throws RuntimeException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parseUnchecked(byte[] json) {
        try { return parse(json); }
        catch (JsonParseException j) { throw new RuntimeException(j); }
    }

    /**
     * Parse JSON from a byte array
     * @param json the JSON
     * @param startIndex the starting index in the array
     * @return the JsonValue
     * @throws RuntimeException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parseUnchecked(byte[] json, int startIndex) {
        try { return parse(json, startIndex); }
        catch (JsonParseException j) { throw new RuntimeException(j); }
    }

    /**
     * Parse JSON from a byte array
     * @param json the JSON
     * @param options options for how to parse
     * @return the JsonValue
     * @throws RuntimeException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parseUnchecked(byte[] json, @Nullable Option... options) {
        try { return parse(json, options); }
        catch (JsonParseException j) { throw new RuntimeException(j); }
    }

    /**
     * Parse JSON from a byte array
     * @param json the JSON
     * @param startIndex the starting index in the array
     * @param options options for how to parse
     * @return the JsonValue
     * @throws RuntimeException if there is a problem parsing
     */
    @NonNull
    public static JsonValue parseUnchecked(byte[] json, int startIndex, @Nullable Option... options) {
        try { return parse(json, startIndex, options); }
        catch (JsonParseException j) { throw new RuntimeException(j); }
    }

    private final char @NonNull [] json;
    private final boolean keepNulls;
    private final int len;
    private int idx;
    private int nextIdx;
    private char previous;
    private char current;
    private char next;

    /**
     * Create a new JsonParse object from a char array
     * @param json the JSON
     */
    public JsonParser(char @Nullable [] json) {
        this(json, 0);
    }

    /**
     * Create a new JsonParse object from a char array
     * @param json the JSON
     * @param options options for how to parse
     */
    public JsonParser(char @Nullable [] json, @Nullable Option... options) {
        this(json, 0, options);
    }

    /**
     * Create a new JsonParse object from a char array
     * @param json the JSON
     * @param startIndex the starting index in the array
     * @param options options for how to parse
     */
    public JsonParser(char @Nullable [] json, int startIndex, @Nullable Option... options) {
        keepNulls = options != null && options.length > 0; // KEEP_NULLS is currently the only option

        if (json == null) {
            this.json = new char[0];
            len = 0;
        }
        else {
            this.json = json;
            len = json.length;
        }

        idx = startIndex;
        if (startIndex < 0) {
            throw new IllegalArgumentException("Invalid start index.");
        }
        nextIdx = -1;
        previous = 0;
        current = 0;
        next = 0;
    }

    /**
     * Parse the JSON
     * @return a JsonValue
     * @throws JsonParseException if there is a problem parsing
     */
    @NonNull
    public JsonValue parse() throws JsonParseException {
        char c = peekToken();
        if (c == 0) {
            return JsonValue.NULL;
        }
        return nextValue();
    }

    private JsonValue nextValue() throws JsonParseException {
        char c = peekToken();
        if (c == 0) {
            throw new JsonParseException("Unexpected end of data.");
        }
        if (c == '"') {
            nextToken();
            return new JsonValue(nextString());
        }
        if (c == '{') {
            nextToken();
            return new JsonValue(nextObject());
        }
        if (c == '[') {
            nextToken();
            return new JsonValue(nextArray());
        }
        return nextPrimitiveValue();
    }

    private List<JsonValue> nextArray() throws JsonParseException {
        List<JsonValue> list = new ArrayList<>(8);
        char p = peekToken();
        while (p != ']') {
            if (p == ',') {
                nextToken(); // advance past the peek
            }
            else {
                list.add(nextValue());
            }
            p = peekToken();
        }
        nextToken(); // advance past the peek
        return list;
    }

    private JsonValue nextPrimitiveValue() throws JsonParseException {
        workBuffer.setLength(0);
        char c = peekToken();
        while (c >= ' ' && isNotDelimiter(c)) {
            workBuffer.append(nextToken());
            c = peekToken();
        }
        String string = workBuffer.toString();
        if (string.length() == 4) {
            if ("true".equals(string)) {
                return JsonValue.TRUE;
            }
            if ("null".equals(string)) {
                return JsonValue.NULL;
            }
        }
        else if (string.length() == 5) {
            if ("false".equals(string)) {
                return JsonValue.FALSE;
            }
        }
        return asNumber(string);
    }

    // next object assumes you have already seen the starting {
    private Map<String, JsonValue> nextObject() throws JsonParseException {
        Map<String, JsonValue> map = new HashMap<>(8);
        String key;
        while (true) {
            char c = nextToken();
            switch (c) {
                case 0:
                    throw new JsonParseException("Text must end with '}'");
                case '}':
                    return map;
                case '{':
                case '[':
                    if (previous == '{') {
                        throw new JsonParseException("Cannot directly nest another Object or Array.");
                    }
                    // fall through
                default:
                    key = nextString();
            }

            c = nextToken();
            if (c != ':') {
                throw new JsonParseException("Expected a ':' after a key.");
            }

            JsonValue value = nextValue();
            if (value != JsonValue.NULL || keepNulls) {
                map.put(key, value);
            }

            switch (nextToken()) {
                case ',':
                    if (peekToken() == '}') {
                        return map; // dangling comma
                    }
                    break;
                case '}':
                    return map;
                default:
                    throw new JsonParseException("Expected a ',' or '}'.");
            }
        }
    }

    private char nextToken() {
        peekToken();
        idx = nextIdx;
        nextIdx = -1;
        previous = current;
        current = next;
        next = 0;
        return current;
    }

    private char nextChar() {
        previous = current;
        if (idx == len) {
            current = 0;
        }
        else {
            current = json[idx++];
        }
        next = 0;
        nextIdx = -1;
        return current;
    }

    private char peekToken() {
        if (nextIdx == -1) {
            nextIdx = idx;
            next = 0;
            while (nextIdx < len) {
                char c = json[nextIdx++];
                switch (c) {
                    case ' ':
                    case '\r':
                    case '\n':
                    case '\t':
                        continue;
                }
                return next = c;
            }
        }
        return next;
    }

    // nextString() assumes you have already seen the starting quote
    private String nextString() throws JsonParseException {
        workBuffer.setLength(0);
        while (true) {
            char c = nextChar();
            switch (c) {
                case 0:
                case '\n':
                case '\r':
                    throw new JsonParseException("Unterminated string.");
                case '\\':
                    c = nextChar();
                    switch (c) {
                        case 'b':
                            workBuffer.append('\b');
                            break;
                        case 't':
                            workBuffer.append('\t');
                            break;
                        case 'n':
                            workBuffer.append('\n');
                            break;
                        case 'f':
                            workBuffer.append('\f');
                            break;
                        case 'r':
                            workBuffer.append('\r');
                            break;
                        case 'u':
                            workBuffer.append(parseU());
                            break;
                        case '"':
                        case '\'':
                        case '\\':
                        case '/':
                            workBuffer.append(c);
                            break;
                        default:
                            throw new JsonParseException("Illegal escape.");
                    }
                    break;
                default:
                    if (c == '"') {
                        return workBuffer.toString();
                    }
                    workBuffer.append(c);
            }
        }
    }

    private char[] parseU() throws JsonParseException {
        int code = 0;
        for (int i = 0; i < 4; i++) {
            char c = nextToken();
            if (c == 0) throw new JsonParseException("Illegal escape.");

            int digit;
            if (c >= '0' && c <= '9') digit = c - '0';
            else if (c >= 'A' && c <= 'F') digit = c - 'A' + 10;
            else if (c >= 'a' && c <= 'f') digit = c - 'a' + 10;
            else throw new JsonParseException("Illegal escape.");

            code = (code << 4) | digit;
        }
        return Character.toChars(code);
    }

    private JsonValue asNumber(String val) throws JsonParseException {
        char initial = val.charAt(0);
        if ((initial >= '0' && initial <= '9') || initial == '-') {

            if (isDecimalNotation(val)) {
                // Use a BigDecimal all the time to keep the original
                // representation. BigDecimal doesn't support -0.0, ensure we
                // keep that by forcing a decimal.
                try {
                    BigDecimal bd = new BigDecimal(val);
                    if(initial == '-' && BigDecimal.ZERO.compareTo(bd)==0) {
                        return new JsonValue(-0.0);
                    }
                    return new JsonValue(bd);
                } catch (NumberFormatException retryAsDouble) {
                    // this is to support "Hex Floats" like this: 0x1.0P-1074
                    try {
                        double d = Double.parseDouble(val);
                        if(Double.isNaN(d) || Double.isInfinite(d)) {
                            throw new JsonParseException(INVALID_VALUE);
                        }
                        return new JsonValue(d);
                    } catch (NumberFormatException ignore) {
                        throw new JsonParseException(INVALID_VALUE);
                    }
                }
            }

            // block items like 00 01 etc. Java number parsers treat these as Octal.
            if (initial == '0' && val.length() > 1) {
                char at1 = val.charAt(1);
                if(at1 >= '0' && at1 <= '9') {
                    throw new JsonParseException(INVALID_VALUE);
                }
            } else if (initial == '-' && val.length() > 2) {
                char at1 = val.charAt(1);
                char at2 = val.charAt(2);
                if (at1 == '0' && at2 >= '0' && at2 <= '9') {
                    throw new JsonParseException(INVALID_VALUE);
                }
            }

            // Try parsing as long first
            try {
                long longVal = Long.parseLong(val);
                if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
                    return new JsonValue((int) longVal);
                }
                return new JsonValue(longVal);
            } catch (NumberFormatException e) {
                // Fall back to BigInteger for very large integers
                try {
                    BigInteger bi = new BigInteger(val);
                    return new JsonValue(bi);
                } catch (NumberFormatException ex) {
                    throw new JsonParseException(INVALID_VALUE);
                }
            }
        }
        throw new JsonParseException(INVALID_VALUE);
    }

    static boolean isDecimalNotation(final String val) {
        return val.indexOf('.') > -1 || val.indexOf('e') > -1
            || val.indexOf('E') > -1 || "-0".equals(val);
    }

    private boolean isNotDelimiter(char c) {
        return c < 128 && !IS_DELIMITER[c];
    }

    private static char[] bytesToChars(byte[] json) throws JsonParseException {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(json)).array();
        }
        catch (CharacterCodingException e) {
            throw new JsonParseException(e);
        }
    }
}
