// Copyright 2021 The NATS Authors
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

package io.nats.client.http;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.nats.client.impl.Headers;

import static io.nats.client.support.BufferUtils.readLine;

public class HttpResponse {
    private String version = "1.0";
    private int statusCode = 200;
    private String reasonPhrase = "OK";
    private Headers headers = new Headers();
    private HttpBody body = writer -> {};

    public static class Builder {
        private HttpResponse proto = new HttpResponse();

        /**
         * @param statusCode is the HTTP status code to use
         * @return this for method chaining.
         */
        public Builder statusCode(int statusCode) {
            if (statusCode < 100 || statusCode > 599) {
                throw new IllegalArgumentException("HTTP does not define statusCode=" + statusCode);
            }
            proto.statusCode = statusCode;
            return this;
        }

        /**
         * @param reasonPhrase is the HTTP reason phrase to use
         * @return the reason phrase
         */
        public Builder reasonPhrase(String reasonPhrase) {
            if (null == reasonPhrase || reasonPhrase.contains("\r") || reasonPhrase.contains("\n")) {
                throw new IllegalArgumentException("HTTP reason phrase must be non-null and is not allowed to contain newlines");
            }
            proto.reasonPhrase = reasonPhrase;
            return this;
        }

        /**
         * @param headers are the attached http headers
         * @return this for method chaining.
         */
        public Builder headers(Headers headers) {
            if (null == headers) {
                throw new IllegalArgumentException("headers must be non-null");
            }
            proto.headers = new Headers(headers);
            return this;
        }

        /**
         * @param body function used to write the response body given
         *     a GatheringByeChannel
         * @return this for method chaining.
         */
        public Builder body(HttpBody body) {
            if (null == body) {
                throw new IllegalArgumentException("body must be non-null");
            }
            proto.body = body;
            return this;
        }

        /**
         * Note that no validation is performed on the version. Probably only makes
         * sense to use "0.9", "1.0", "1.1", or "2".
         * 
         * @param version is the new HTTP version to use.
         * @return this for method chaining.
         */
        public Builder version(String version) {
            if (null == version) {
                throw new IllegalArgumentException("version must be non-null");
            }
            proto.version = version;
            return this;
        }

        /**
         * Build an instance of HttpResponse, and reset the builder.
         * @return the newly built http response
         */
        public HttpResponse build() {
            HttpResponse built = proto;
            proto = new HttpResponse();
            return built;
        }
    }

    private HttpResponse() {}

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        Builder builder = new Builder();
        builder.proto.version = version;
        builder.proto.statusCode = statusCode;
        builder.proto.reasonPhrase = reasonPhrase;
        builder.proto.headers = new Headers(headers);
        builder.proto.body = body;    
        return builder;
    }

    /**
     * @return the attached http headers, defaults to GET
     */
    public Headers getHeaders() {
        return headers;
    }

    /**
     * @return the HTTP version to use for this request. Defaults to "1.0"
     */
    public String getVersion() {
        return version;
    }

    /**
     * @return the HTTP status code. Defaults to 200.
     */
    public int getStatusCode() {
        return statusCode;
    }

    /**
     * @return the HTTP reason phrase. Defaults to "OK"
     */
    public String getReasonPhrase() {
        return reasonPhrase;
    }

    /**
     * @return the function used to write the request body given
     *     a GatheringByeChannel
     */
    public HttpBody getBody() {
        return body;
    }

    /**
     * @return a debug representation of the response
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(version);
        sb.append(" ");
        sb.append(statusCode);
        sb.append(" ");
        sb.append(reasonPhrase);
        sb.append("\r\n");
        headers.forEach((key, values) -> {
            values.stream().forEach(value -> {
                sb.append(key);
                sb.append(": ");
                sb.append(value);
                sb.append("\r\n");
            });
        });
        sb.append("\r\n");
        sb.append(body.toString());
        return sb.toString();
    }

    private static final Pattern RESPONSE_LINE_PATTERN = Pattern.compile("^HTTP/([^ ]+) (\\d{3}) (.*)");
    private static final Pattern HEADER_PATTERN = Pattern.compile("^([^:]+): (.*)");

    /**
     * Reads an HttpResponse from a channel.
     * 
     * @param buffer is in "get" mode and has sufficient capacity for the max line length
     * @param reader is a reader used to populate the buffer if insufficient remaining
     *    bytes exist in buffer. May be null if buffer should not be populated.
     * @return an http response.
     * 
     * @throws BufferUnderflowException if the HTTP request contained a single
     *     line longer than 10kb
     * @throws HttpProtocolException if illegal HTTP protocol is used
     * 
     * NOTE: This does not yet support reading response bodies!
     * NOTE: Resulting response will have an HttpBody that references
     *    this reader, thus any reading of the body MUST occur before
     *    future interactions with the underlying conneciton.
     */
    public static HttpResponse read(ReadableByteChannel reader, ByteBuffer buffer) throws IOException {
        HttpResponse response = new HttpResponse();

        String line = readLine(buffer, reader);
        if (null == line) {
            throw new HttpProtocolException("Premature end of channel.");
        }
        Matcher matcher = RESPONSE_LINE_PATTERN.matcher(line);
        if (!matcher.matches()) {
            throw new HttpProtocolException("Invalid HTTP response-line: " + line);
        }
        response.version = matcher.group(1);
        response.statusCode = Integer.parseInt(matcher.group(2));
        response.reasonPhrase = matcher.group(3);

        while (true) {
            line = readLine(buffer, reader);
            if ("".equals(line)) {
                break;
            } else if (null == line) {
                throw new HttpProtocolException("Premature end of channel.");
            }
            matcher = HEADER_PATTERN.matcher(line);
            if (!matcher.matches()) {
                throw new HttpProtocolException("Invalid HTTP header-line: " + line);
            }
            response.headers.add(matcher.group(1), matcher.group(2));
        }

        return response;
    }
}