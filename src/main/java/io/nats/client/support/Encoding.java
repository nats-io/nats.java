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

// Refactored into modular utility classes:
// - Base64Utils.java
// - Base32Utils.java
// - JsonUtilsLite.java
// - UriUtilsLite.java
// Deprecated Encoding.java acts as a thin wrapper (or can be removed)

package io.nats.client.support;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public abstract class Encoding {
    private Encoding() {}  // prevents instantiation

    // ------------------------ BASE64 ------------------------

    public static byte[] base64BasicEncode(byte[] input) {
        return Base64Utils.base64BasicEncode(input);
    }

    public static String base64BasicEncodeToString(byte[] input) {
        return Base64Utils.base64BasicEncodeToString(input);
    }

    public static String base64BasicEncodeToString(String input) {
        return Base64Utils.base64BasicEncodeToString(input);
    }

    public static byte[] base64UrlEncode(byte[] input) {
        return Base64Utils.base64UrlEncode(input);
    }

    public static String base64UrlEncodeToString(byte[] input) {
        return Base64Utils.base64UrlEncodeToString(input);
    }

    public static String base64UrlEncodeToString(String input) {
        return Base64Utils.base64UrlEncodeToString(input);
    }

    public static byte[] base64BasicDecode(byte[] input) {
        return Base64Utils.base64BasicDecode(input);
    }

    public static byte[] base64BasicDecode(String input) {
        return Base64Utils.base64BasicDecode(input);
    }

    public static String base64BasicDecodeToString(String input) {
        return Base64Utils.base64BasicDecodeToString(input);
    }

    public static byte[] base64UrlDecode(byte[] input) {
        return Base64Utils.base64UrlDecode(input);
    }

    public static byte[] base64UrlDecode(String input) {
        return Base64Utils.base64UrlDecode(input);
    }

    public static String base64UrlDecodeToString(String input) {
        return Base64Utils.base64UrlDecodeToString(input);
    }

    // ------------------------ BASE32 ------------------------

    public static char[] base32Encode(final byte[] input) {
        return Base32Utils.base32Encode(input);
    }

    public static byte[] base32Decode(final char[] input) {
        return Base32Utils.base32Decode(input);
    }

    // ------------------------ JSON ------------------------

    public static String jsonEncode(String s) {
        return JsonUtilsLite.jsonEncode(s);
    }

    public static String jsonDecode(String s) {
        return JsonUtilsLite.jsonDecode(s);
    }

    public static StringBuilder jsonEncode(StringBuilder sb, String s) {
        return JsonUtilsLite.jsonEncode(sb, s);
    }

    public static String jsonEncode(char[] chars) {
        return JsonUtilsLite.jsonEncode(chars);
    }

    public static StringBuilder jsonEncode(StringBuilder sb, char[] chars) {
        return JsonUtilsLite.jsonEncode(sb, chars);
    }

    // ------------------------ URI ------------------------

    public static String uriDecode(String source) {
        return UriUtilsLite.uriDecode(source);
    }

    // ------------------------ Deprecated ------------------------

    @Deprecated
    public static byte[] base64Encode(byte[] input) {
        return Base64Utils.base64UrlEncode(input);
    }

    @Deprecated
    public static String toBase64Url(byte[] input) {
        return Base64Utils.base64UrlEncodeToString(input);
    }

    @Deprecated
    public static String toBase64Url(String input) {
        return Base64Utils.base64UrlEncodeToString(input);
    }

    @Deprecated
    public static String fromBase64Url(String input) {
        return Base64Utils.base64UrlDecodeToString(input);
    }
}
