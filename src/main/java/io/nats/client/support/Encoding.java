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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public abstract class Encoding {
    private Encoding() {}  /* ensures cannot be constructed */

    /**
     * base64 encode a byte array to a byte array
     * @param input the input byte array to encode
     * @return the encoded byte array
     */
    public static byte[] base64BasicEncode(byte[] input) {
        return Base64.getEncoder().encode(input);
    }

    /**
     * base64 encode a byte array to a byte array
     * @param input the input byte array to encode
     * @return the encoded byte array
     */
    public static String base64BasicEncodeToString(byte[] input) {
        return Base64.getEncoder().encodeToString(input);
    }

    /**
     * base64 url encode a byte array to a byte array
     * @param input the input byte array to encode
     * @return the encoded byte array
     */
    public static String base64BasicEncodeToString(String input) {
        return Base64.getEncoder()
            .encodeToString(input.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * base64 url encode a byte array to a byte array
     * @param input the input byte array to encode
     * @return the encoded byte array
     */
    public static byte[] base64UrlEncode(byte[] input) {
        return Base64.getUrlEncoder().withoutPadding().encode(input);
    }

    /**
     * base64 url encode a byte array to a byte array
     * @param input the input byte array to encode
     * @return the encoded byte array
     */
    public static String base64UrlEncodeToString(byte[] input) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(input);
    }

    /**
     * base64 url encode a byte array to a byte array
     * @param input the input byte array to encode
     * @return the encoded byte array
     */
    public static String base64UrlEncodeToString(String input) {
        return Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(input.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * base64 decode a byte array
     * @param input the input byte array to decode
     * @return the decoded byte array
     */
    public static byte[] base64BasicDecode(byte[] input) {
        return Base64.getDecoder().decode(input);
    }

    /**
     * base64 decode a base64 encoded string
     * @param input the input string to decode
     * @return the decoded byte array
     */
    public static byte[] base64BasicDecode(String input) {
        return Base64.getDecoder().decode(input);
    }

    /**
     * base64 decode a base64 encoded string
     * @param input the input string to decode
     * @return the decoded string
     */
    public static String base64BasicDecodeToString(String input) {
        return new String(Base64.getDecoder().decode(input));
    }

    /**
     * base64 url decode a byte array
     * @param input the input byte array to decode
     * @return the decoded byte array
     */
    public static byte[] base64UrlDecode(byte[] input) {
        return Base64.getUrlDecoder().decode(input);
    }

    /**
     * base64 url decode a base64 url encoded string
     * @param input the input string to decode
     * @return the decoded byte array
     */
    public static byte[] base64UrlDecode(String input) {
        return Base64.getUrlDecoder().decode(input);
    }

    /**
     * base64 url decode a base64 url encoded string
     * @param input the input string to decode
     * @return the decoded string
     */
    public static String base64UrlDecodeToString(String input) {
        return new String(Base64.getUrlDecoder().decode(input));
    }

    // http://en.wikipedia.org/wiki/Base_32
    private static final String BASE32_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    private static final int[] BASE32_LOOKUP;
    private static final int MASK = 31;
    private static final int SHIFT = 5;
    public static char[] base32Encode(final byte[] input) {
        int last = input.length;
        char[] charBuff = new char[(last + 7) * 8 / SHIFT];
        int offset = 0;
        int buffer = input[offset++];
        int bitsLeft = 8;
        int i = 0;

        while (bitsLeft > 0 || offset < last) {
            if (bitsLeft < SHIFT) {
                if (offset < last) {
                    buffer <<= 8;
                    buffer |= (input[offset++] & 0xff);
                    bitsLeft += 8;
                } else {
                    int pad = SHIFT - bitsLeft;
                    buffer <<= pad;
                    bitsLeft += pad;
                }
            }
            int index = MASK & (buffer >> (bitsLeft - SHIFT));
            bitsLeft -= SHIFT;
            charBuff[i] = BASE32_CHARS.charAt(index);
            i++;
        }

        int nonBlank;

        for (nonBlank=charBuff.length-1;nonBlank>=0;nonBlank--) {
            if (charBuff[nonBlank] != 0) {
                break;
            }
        }

        char[] retVal = new char[nonBlank+1];

        System.arraycopy(charBuff, 0, retVal, 0, retVal.length);

        Arrays.fill(charBuff, '\0');

        return retVal;
    }
    static {
        BASE32_LOOKUP = new int[256];

        Arrays.fill(BASE32_LOOKUP, 0xFF);

        for (int i = 0; i < BASE32_CHARS.length(); i++) {
            int index = BASE32_CHARS.charAt(i) - '0';
            BASE32_LOOKUP[index] = i;
        }
    }

    public static byte[] base32Decode(final char[] input) {
        byte[] bytes = new byte[input.length * SHIFT / 8];
        int buffer = 0;
        int next = 0;
        int bitsLeft = 0;

        for (char value : input) {
            int lookup = value - '0';

            if (lookup < 0 || lookup >= BASE32_LOOKUP.length) {
                continue;
            }

            int c = BASE32_LOOKUP[lookup];
            buffer <<= SHIFT;
            buffer |= c & MASK;
            bitsLeft += SHIFT;
            if (bitsLeft >= 8) {
                bytes[next++] = (byte) (buffer >> (bitsLeft - 8));
                bitsLeft -= 8;
            }
        }
        return bytes;
    }

    public static String jsonDecode(String s) {
        int len = s.length();
        StringBuilder sb = new StringBuilder(len);
        for (int x = 0; x < len; x++) {
            char ch = s.charAt(x);
            if (ch == '\\') {
                char nextChar = (x == len - 1) ? '\\' : s.charAt(x + 1);
                switch (nextChar) {
                    case '\\':
                        break;
                    case 'b':
                        ch = '\b';
                        break;
                    case 'f':
                        ch = '\f';
                        break;
                    case 'n':
                        ch = '\n';
                        break;
                    case 'r':
                        ch = '\r';
                        break;
                    case 't':
                        ch = '\t';
                        break;
                    // Hex Unicode: u????
                    case 'u':
                        if (x >= len - 5) {
                            ch = 'u';
                            break;
                        }
                        int code = Integer.parseInt(
                            "" + s.charAt(x + 2) + s.charAt(x + 3) + s.charAt(x + 4) + s.charAt(x + 5), 16);
                        sb.append(Character.toChars(code));
                        x += 5;
                        continue;
                    default:
                        ch = nextChar;
                        break;
                }
                x++;
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    public static String jsonEncode(String s) {
        return jsonEncode(new StringBuilder(), s).toString();
    }

    public static StringBuilder jsonEncode(StringBuilder sb, String s) {
        int len = s.length();
        for (int x = 0; x < len; x++) {
            appendChar(sb, s.charAt(x));
        }
        return sb;
    }

    public static String jsonEncode(char[] chars) {
        return jsonEncode(new StringBuilder(), chars).toString();
    }

    public static StringBuilder jsonEncode(StringBuilder sb, char[] chars) {
        for (char aChar : chars) {
            appendChar(sb, aChar);
        }
        return sb;
    }

    private static void appendChar(StringBuilder sb, char ch) {
        switch (ch) {
            case '"':
                sb.append("\\\"");
                break;
            case '\\':
                sb.append("\\\\");
                break;
            case '\b':
                sb.append("\\b");
                break;
            case '\f':
                sb.append("\\f");
                break;
            case '\n':
                sb.append("\\n");
                break;
            case '\r':
                sb.append("\\r");
                break;
            case '\t':
                sb.append("\\t");
                break;
            case '/':
                sb.append("\\/");
                break;
            default:
                if (ch < ' ') {
                    sb.append(String.format("\\u%04x", (int) ch));
                }
                else {
                    sb.append(ch);
                }
                break;
        }
    }

    public static String uriDecode(String source) {
        try {
            return URLDecoder.decode(source.replace("+", "%2B"), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return source;
        }
    }

    /**
     * @deprecated Use {@link #base64UrlEncode(byte[])} instead.
     * base64 url encode a byte array to a byte array
     * @param input the input byte array to encode
     * @return the encoded byte array
     */
    @Deprecated
    public static byte[] base64Encode(byte[] input) {
        return base64UrlEncode(input);
    }

    /**
     * @deprecated Use {@link #base64UrlEncodeToString(byte[])} instead.
     * base64 url encode a byte array to a string
     * @param input the input byte array to encode
     * @return the encoded string
     */
    @Deprecated
    public static String toBase64Url(byte[] input) {
        return base64UrlEncodeToString(input);
    }

    /**
     * @deprecated Use {@link #base64UrlEncodeToString(String)} instead.
     * base64 url encode a string to a string
     * @param input the input string to encode
     * @return the encoded string
     */
    @Deprecated
    public static String toBase64Url(String input) {
        return base64UrlEncodeToString(input);
    }

    /**
     * @deprecated Use {@link #base64UrlDecodeToString(String)} instead.
     * get a string from a base64 url encoded byte array
     * @param input the input string to decode
     * @return the decoded string
     */
    @Deprecated
    public static String fromBase64Url(String input) {
        return base64UrlDecodeToString(input);
    }
}
