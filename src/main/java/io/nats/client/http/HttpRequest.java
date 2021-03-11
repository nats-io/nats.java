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

import io.nats.client.impl.Headers;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.regex.Pattern;

/**
 * Encapsulate an HttpRequest, in Java 11 we could use this class:
 * https://docs.oracle.com/en/java/javase/11/docs/api/java.net.http/java/net/http/HttpRequest.html
 * 
 * ...but we want to support older JVMs.
 */
public class HttpRequest {
    private static Pattern URI_PATTERN = Pattern.compile("[^ \t\r\n]+");
    private String method = "GET";
    private String uri = "/";
    private String version = "1.0";
    private Headers headers = new Headers();
    private HttpBody body = writer -> {};

    public static class Builder {
        private HttpRequest proto = new HttpRequest();

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
         * @param body the http body to use.
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
         * Note that no validation is performed, but the method is trimmed of whitespace
         * and converted to all upper case.
         * 
         * @param method is the new request method to use.
         * @return this for method chaining.
         */
        public Builder method(String method) {
            if (null == method) {
                throw new IllegalArgumentException("method must be non-null");
            }
            proto.method = method.trim().toUpperCase();
            return this;
        }

        /**
         * This sets the RAW URI, you may need to perform URL encoding before passing
         * into this method.
         * 
         * @param uri is the new "path" of the URI to use.
         * @return this for method chaining.
         */
        public Builder uri(String uri) {
            if (null == uri || 0 == uri.length() || !URI_PATTERN.matcher(uri).matches()) {
                throw new IllegalArgumentException("uri must be non-null, non-empty, and may not contain spaces");
            }
            proto.uri = uri;
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
         * Build an instance of HttpRequest, and reset the builder.
         * @return a new http request
         */
        public HttpRequest build() {
            HttpRequest built = proto;
            proto = new HttpRequest();
            return built;
        }
    }

    private HttpRequest() {}

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        Builder builder = new Builder();
        builder.proto.method = method;
        builder.proto.uri = uri;
        builder.proto.version = version;
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
     * @return the request method (GET, POST, etc)
     */
    public String getMethod() {
        return method;
    }

    /**
     * This is the RAW URI, you may need to perform URL decoding the result.
     * 
     * @return the "path" of the URI (in the RFC this is the request-URI)
     */
    public String getURI() {
        return uri;
    }

    /**
     * @return the HTTP version to use for this request. Defaults to "1.0"
     */
    public String getVersion() {
        return version;
    }

    /**
     * @return the function used to write the request body given
     *     a ReadableByeChannel
     */
    public HttpBody getBody() {
        return body;
    }

    public void write(WritableByteChannel writer) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(method);
        sb.append(" ");
        sb.append(uri);
        sb.append(" HTTP/");
        sb.append(version);
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
        writer.write(UTF_8.encode(sb.toString()));
        body.write(writer);
    }

    /**
     * @return a debug representation
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(method);
        sb.append(" ");
        sb.append(uri);
        sb.append(" HTTP/");
        sb.append(version);
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
        return sb.toString();
    }
}