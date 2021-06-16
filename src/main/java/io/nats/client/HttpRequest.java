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

package io.nats.client;

import io.nats.client.impl.Headers;

/**
 * Encapsulate an HttpRequest, in Java 11 we could use this class:
 * https://docs.oracle.com/en/java/javase/11/docs/api/java.net.http/java/net/http/HttpRequest.html
 * 
 * ...but we want to support older JVMs.
 */
public class HttpRequest {
    private String method = "GET";
    private String uri = "/";
    private String version = "1.0";
    private Headers headers = new Headers();

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
     * Note that no validation is performed, but the method is trimmed of whitespace
     * and converted to all upper case.
     * 
     * @param method is the new request method to use.
     * @return this for method chaining.
     */
    public HttpRequest method(String method) {
        if (null == method) {
            throw new IllegalArgumentException("HttpRequest method must be non-null");
        }
        this.method = method.trim().toUpperCase();
        return this;
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
     * This sets the RAW URI, you may need to perform URL encoding before passing
     * into this method.
     * 
     * @param uri is the new "path" of the URI to use.
     * @return this for method chaining.
     */
    public HttpRequest uri(String uri) {
        if (null == uri) {
            throw new IllegalArgumentException("HttpRequest uri must be non-null");
        }
        this.uri = uri;
        return this;
    }

    /**
     * @return the HTTP version to use for this request. Defaults to "1.0"
     */
    public String getVersion() {
        return version;
    }

    /**
     * Note that no validation is performed on the version. Probably only makes
     * sense to use "0.9", "1.0", "1.1", or "2".
     * 
     * @param version is the new HTTP version to use.
     * @return this for method chaining.
     */
    public HttpRequest version(String version) {
        if (null == version) {
            throw new IllegalArgumentException("HttpRequest version must be non-null");
        }
        this.version = version;
        return this;
    }

    /**
     * @return the textual representation of the HTTP request line + headers (no
     *         body)
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