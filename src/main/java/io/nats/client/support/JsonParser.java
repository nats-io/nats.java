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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.nats.client.support.JsonValue.NULL;

public class JsonParser {

    enum Option {KEEP_NULLS}

    public static JsonValue parse(char[] json) throws JsonParseException {
        return new JsonParser(json, 0).parse();
    }

    public static JsonValue parse(char[] json, int startIndex) throws JsonParseException {
        return new JsonParser(json, startIndex).parse();
    }

    public static JsonValue parse(char[] json, Option... options) throws JsonParseException {
        return new JsonParser(json, 0, options).parse();
    }

    public static JsonValue parse(char[] json, int startIndex, Option... options) throws JsonParseException {
        return new JsonParser(json, startIndex, options).parse();
    }

    public static JsonValue parse(String json) throws JsonParseException {
        return new JsonParser(json.toCharArray(), 0).parse();
    }

    public static JsonValue parse(String json, int startIndex) throws JsonParseException {
        return new JsonParser(json.toCharArray(), startIndex).parse();
    }

    public static JsonValue parse(String json, Option... options) throws JsonParseException {
        return new JsonParser(json.toCharArray(), 0, options).parse();
    }

    public static JsonValue parse(String json, int startIndex, Option... options) throws JsonParseException {
        return new JsonParser(json.toCharArray(), startIndex, options).parse();
    }

    public static JsonValue parse(byte[] json) throws JsonParseException {
        return new JsonParser(new String(json, StandardCharsets.UTF_8).toCharArray(), 0).parse();
    }

    public static JsonValue parse(byte[] json, Option... options) throws JsonParseException {
        return new JsonParser(new String(json, StandardCharsets.UTF_8).toCharArray(), 0, options).parse();
    }

    private final char[] json;
    private final boolean keepNulls;
    private final int len;
    private int idx;
    private int nextIdx;
    private char previous;
    private char current;
    private char next;

    public JsonParser(char[] json) {
        this(json, 0);
    }

    public JsonParser(char[] json, Option... options) {
        this(json, 0, options);
    }

    public JsonParser(char[] json, int startIndex, Option... options) {
        this.json = json;

        boolean kn = false;
        for (Option o : options) {
            if (o == Option.KEEP_NULLS) {
                kn = true;
                break; // b/c only option currently
            }
        }
        keepNulls = kn;

        len = json == null ? 0 : json.length;
        idx = startIndex;
        if (startIndex < 0) {
            throw new IllegalArgumentException("Invalid start index.");
        }
        nextIdx = -1;
        previous = 0;
        current = 0;
        next = 0;
    }

    public JsonValue parse() throws JsonParseException {
        char c = peekToken();
        if (c == 0) {
            return NULL;
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
        List<JsonValue> list = new ArrayList<>();
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
        StringBuilder sb = new StringBuilder();
        char c = peekToken();
        while (c >= ' ' && ",:]}/\\\"[{;=#".indexOf(c) < 0) {
            sb.append(nextToken());
            c = peekToken();
        }
        String string = sb.toString();
        if ("true".equalsIgnoreCase(string)) {
            return new JsonValue(Boolean.TRUE);
        }
        if ("false".equalsIgnoreCase(string)) {
            return new JsonValue(Boolean.FALSE);
        }
        if ("null".equalsIgnoreCase(string)) {
            return NULL;
        }
        try {
            return asNumber(string);
        }
        catch (Exception e) {
            throw new JsonParseException("Invalid value.");
        }
    }

    // next object assumes you have already seen the starting {
    private Map<String, JsonValue> nextObject() throws JsonParseException {
        Map<String, JsonValue> map = new HashMap<>();
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
            if (value != NULL || keepNulls) {
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

    // next string assumes you have already seen the starting quote
    private String nextString() throws JsonParseException {
        StringBuilder sb = new StringBuilder();
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
                            sb.append('\b');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 'u':
                            sb.append(parseU());
                            break;
                        case '"':
                        case '\'':
                        case '\\':
                        case '/':
                            sb.append(c);
                            break;
                        default:
                            throw new JsonParseException("Illegal escape.");
                    }
                    break;
                default:
                    if (c == '"') {
                        return sb.toString();
                    }
                    sb.append(c);
            }
        }
    }

    private char[] parseU() throws JsonParseException {
        char[] a = new char[4];
        for (int x = 0; x < 4; x++) {
            a[x] = nextToken();
            if (a[x] == 0) {
                throw new JsonParseException("Illegal escape.");
            }
        }
        try {
            int code = Integer.parseInt("" + a[0] + a[1] + a[2] + a[3], 16);
            return Character.toChars(code);
        }
        catch (RuntimeException e) {
            throw new JsonParseException("Illegal escape.", e);
        }
    }

    private JsonValue asNumber(String val) throws JsonParseException {
        char initial = val.charAt(0);
        if ((initial >= '0' && initial <= '9') || initial == '-') {
            // decimal representation
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
                            throw new JsonParseException("val ["+val+"] is not a valid number.");
                        }
                        return new JsonValue(d);
                    } catch (NumberFormatException ignore) {
                        throw new JsonParseException("val ["+val+"] is not a valid number.");
                    }
                }
            }
            // block items like 00 01 etc. Java number parsers treat these as Octal.
            if(initial == '0' && val.length() > 1) {
                char at1 = val.charAt(1);
                if(at1 >= '0' && at1 <= '9') {
                    throw new JsonParseException("val ["+val+"] is not a valid number.");
                }
            } else if (initial == '-' && val.length() > 2) {
                char at1 = val.charAt(1);
                char at2 = val.charAt(2);
                if(at1 == '0' && at2 >= '0' && at2 <= '9') {
                    throw new JsonParseException("val ["+val+"] is not a valid number.");
                }
            }
            BigInteger bi = new BigInteger(val);
            if(bi.bitLength() <= 31){
                return new JsonValue(bi.intValue());
            }
            if(bi.bitLength() <= 63){
                return new JsonValue(bi.longValue());
            }
            return new JsonValue(bi);
        }
        throw new JsonParseException("val ["+val+"] is not a valid number.");
    }

    private boolean isDecimalNotation(final String val) {
        return val.indexOf('.') > -1 || val.indexOf('e') > -1
            || val.indexOf('E') > -1 || "-0".equals(val);
    }
}
