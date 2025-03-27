package io.nats.client.support;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class UriUtilsLite {
    public static String uriDecode(String source) {
        try {
            return URLDecoder.decode(source.replace("+", "%2B"), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return source;
        }
    }
}
