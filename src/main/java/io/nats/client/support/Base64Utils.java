package io.nats.client.support;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class Base64Utils {

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
