package io.nats.client.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static io.nats.client.impl.NatsJetStreamConstants.JSAPI_PREFIX;
import static io.nats.client.impl.NatsJetStreamConstants.JS_PREFIX;
import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.Validator.nullOrEmpty;
import static io.nats.client.support.Validator.validateJetStreamPrefix;

public class JsPrefixManager {

    private static final Set<String> JS_PREFIXES = Collections.synchronizedSet(new HashSet<>());

    public static String addPrefix(String prefix) {
        if (nullOrEmpty(prefix)) {
            return JSAPI_PREFIX;
        }

        prefix = validateJetStreamPrefix(prefix);
        if (!prefix.endsWith(DOT)) {
            prefix += DOT;
        }

        JS_PREFIXES.add(prefix);

        return prefix;
    }

    public static boolean isJsMessage(final String replyTo) {
        if (replyTo != null) {
            if (replyTo.startsWith(JS_PREFIX)) {
                return true;
            }

            synchronized (JS_PREFIXES) {
                for (String prefix : JS_PREFIXES) {
                    if (replyTo.startsWith(prefix)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
