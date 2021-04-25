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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.NatsJetStreamConstants.JSAPI_PREFIX;
import static io.nats.client.support.NatsJetStreamConstants.JS_PREFIX;
import static io.nats.client.support.Validator.nullOrEmpty;
import static io.nats.client.support.Validator.validateJetStreamPrefix;

public abstract class JsPrefixManager {
    private JsPrefixManager() {}  /* ensures cannot be constructed */

    private static final Set<String> JS_PREFIXES = Collections.synchronizedSet(new HashSet<>());

    public static String addPrefix(String prefix) {
        if (nullOrEmpty(prefix) || prefix.equals(JSAPI_PREFIX)) {
            return JSAPI_PREFIX;
        }

        prefix = validateJetStreamPrefix(prefix);
        if (!prefix.endsWith(DOT)) {
            prefix += DOT;
        }

        JS_PREFIXES.add(prefix);

        return prefix;
    }

    public static boolean hasPrefix(final String replyTo) {
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
