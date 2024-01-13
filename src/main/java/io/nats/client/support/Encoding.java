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
     * base64 url encode a byte array to a byte array
     * @param input the input byte array to encode
     * @return the encoded byte array
     * @deprecated prefer base64UrlEncode
     */
    @Deprecated
    public static byte[] base64Encode(byte[] input) {
        return Base64.getUrlEncoder().withoutPadding().encode(input);
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
     * base64 url encode a byte array to a string
     * @param input the input byte array to encode
     * @return the encoded string
     */
    public static String toBase64Url(byte[] input) {
        return new String(base64UrlEncode(input));
    }

    /**
     * base64 url encode a string to a string
     * @param input the input string to encode
     * @return the encoded string
     */
    public static String toBase64Url(String input) {
        return new String(base64UrlEncode(input.getBytes(StandardCharsets.US_ASCII)));
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
     * get a string from a base64 url encoded byte array
     * @param input the input string to decode
     * @return the decoded string
     */
    public static String fromBase64Url(String input) {
        return new String(base64UrlDecode(input.getBytes(StandardCharsets.US_ASCII)));
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

        for (char ic : input) {
            int lookup = ic - '0';
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
        return JsonEncoding.jsonDecode(s);
    }

    public static String jsonEncode(String s) {
        return JsonEncoding.jsonEncode(s);
    }

    public static StringBuilder jsonEncode(StringBuilder sb, String s) {
        return JsonEncoding.jsonEncode(sb, s);
    }

    public static String uriDecode(String source) {
        try {
            return URLDecoder.decode(source.replace("+", "%2B"), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return source;
        }
    }
}
