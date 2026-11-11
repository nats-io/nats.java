// Copyright 2026 The NATS Authors
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

import static io.nats.client.support.NatsConstants.CR;
import static io.nats.client.support.NatsConstants.LF;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Quoting of text the client received from a server and did not accept, for a log, a terminal or an error message:
 * the first line a server sent where the client expected something else, or a {@code connect_urls} entry the client
 * skipped. The quote is cut at 256 bytes, quotes and backslashes are escaped as in a string literal, a tab as
 * {@code 	} and every other control character as a Java unicode escape, so the quote can carry neither a terminal
 * escape sequence nor a fake line break.
 */
public final class UntrustedText {
    private static final int LIMIT = 256;

    private UntrustedText() {}

    /**
     * Quote the first line of bytes received from a server: the bytes up to the first CR or LF.
     * @param bytes the bytes as received
     * @return the quoted first line
     */
    @NonNull
    public static String quoteFirstLine(byte[] bytes) {
        return quote(bytes, true);
    }

    /**
     * Quote one token the client did not accept, such as a {@code connect_urls} entry. A line break inside the token
     * is escaped, not taken as its end.
     * @param token the token as received
     * @return the quoted token
     */
    @NonNull
    public static String quote(@NonNull String token) {
        return quote(token.getBytes(UTF_8), false);
    }

    private static String quote(byte[] bytes, boolean firstLineOnly) {
        int len = bytes.length;
        if (firstLineOnly) {
            for (int i = 0; i < len; i++) {
                if (bytes[i] == CR || bytes[i] == LF) {
                    len = i;
                    break;
                }
            }
        }
        boolean cut = len > LIMIT;
        if (cut) {
            len = LIMIT;
        }
        String text = new String(bytes, 0, len, UTF_8);
        StringBuilder sb = new StringBuilder(len + 32).append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"' || c == '\\') {
                sb.append('\\').append(c);
            }
            else if (c == '\t') {
                sb.append("\\t");
            }
            else if (c < 0x20 || (c >= 0x7f && c <= 0x9f)) {
                sb.append(String.format("\\u%04x", (int) c));
            }
            else {
                sb.append(c);
            }
        }
        sb.append('"');
        if (cut) {
            sb.append(" (cut at " + LIMIT + " bytes)");
        }
        return sb.toString();
    }
}
