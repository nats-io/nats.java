// Copyright 2015-2018 The NATS Authors
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HttpHeaders implements Iterable<HttpHeaders.Entry> {
    /**
     * An individual HttpHeader entry.
     */
    interface Entry {
        /**
         * @return the case-insensitive (but not normalized) name of this header entry.
         */
        String getName();

        /**
         * @return the value of this header entry, may be empty string.
         */
        String getValue();
    }

    /**
     * Create an HttpHeaders entry from a name and value.
     * 
     * @param name  the header name, must be non-empty and non-null and should not
     *              contain ":"
     * @param value the header value, must be non-null and should not contain
     *              "\r\n".
     * 
     *              Resulting entry implements equals()/hashCode() such that the
     *              name is case-insensitive. The result also overrides toString()
     *              to return the name followed by ":" followed by value.
     * @return the new entry
     */
    public static Entry entry(String name, String value) {
        return new HttpHeader(name, value);
    }

    private static class HttpHeader implements Entry {
        private String name;
        private String value;
        private HttpHeader next;
        private HttpHeader prev;

        private HttpHeader(String name, String value) {
            if (null == name || "".equals(name)) {
                throw new IllegalArgumentException("HttpHeader must have non-null and non-empty name");
            }
            if (null == value) {
                throw new IllegalArgumentException("HttpHeader must have non-null value");
            }
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof HttpHeader)) {
                return false;
            }
            HttpHeader other = (HttpHeader) obj;
            return name.equalsIgnoreCase(other.name) && value.equals(other.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name.toLowerCase(), value);
        }

        @Override
        public String toString() {
            return name + ": " + value;
        }
    }

    private HttpHeader first = null;
    private HttpHeader last = null;
    private Map<String, List<HttpHeader>> map = new HashMap<>();

    /**
     * Shortcut for <code>add(entry(name, value))</code>
     * 
     * @param name  the header name, see {@link #entry(String, String)}.
     * @param value the header value, see {@link #entry(String, String)}.
     * @return this for method chaining.
     */
    public HttpHeaders add(String name, String value) {
        return add(entry(name, value));
    }

    /**
     * Append this header entry to the http headers.
     * 
     * @param input the new header to add.
     * @return this for method chaining.
     */
    public HttpHeaders add(Entry input) {
        HttpHeader header = new HttpHeader(input.getName(), input.getValue());
        if (null == last) {
            first = last = header;
        } else {
            last.next = header;
            header.prev = last;
            last = header;
        }
        map.computeIfAbsent(header.getName().toLowerCase(), key -> new ArrayList<HttpHeader>()).add(header);
        return this;
    }

    /**
     * Remove ALL headers with the specified name.
     * 
     * @param name the name of the header to remove (case-insensitive)
     * @return this for method chaining.
     */
    public HttpHeaders remove(String name) {
        List<HttpHeader> list = map.remove(name.toLowerCase());
        if (null != list) {
            for (HttpHeader header : list) {
                if (null == header.prev) {
                    first = header.next;
                } else {
                    header.prev.next = header.next;
                }
                if (null == header.next) {
                    last = header.prev;
                } else {
                    header.next.prev = header.prev;
                }
            }
        }
        return this;
    }

    /**
     * @param name the case-insensitive header name.
     * @return the value of the first header which has the specified case-insensitive name.
     */
    public Entry getFirst(String name) {
        List<HttpHeader> result = map.get(name.toLowerCase());
        if (null == result || result.isEmpty()) {
            return null;
        }
        return result.get(0);
    }

    /**
     * @param name the case-insensitive header name.
     * @return empty list if no header exists with name, otherwise return a list of all
     */
    public List<Entry> getAll(String name) {
        return new ArrayList<>(map.computeIfAbsent(name.toLowerCase(), key -> Collections.emptyList()));
    }

    @Override
    public Iterator<HttpHeaders.Entry> iterator() {
        return new Iterator<HttpHeaders.Entry>() {
            HttpHeader cur = first;

            @Override
            public boolean hasNext() {
                return cur != null;
            }

            @Override
            public HttpHeader next() {
                HttpHeader result = cur;
                if (null != cur) {
                    cur = cur.next;
                }
                return result;
            }

        };
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HttpHeaders)) {
            return false;
        }
        HttpHeaders other = (HttpHeaders) obj;
        // order only matters for duplicated header keys:
        for (Map.Entry<String, List<HttpHeader>> entry : map.entrySet()) {
            List<HttpHeader> list = entry.getValue();
            List<HttpHeader> otherList = other.map.get(entry.getKey());
            if (null == otherList && !list.isEmpty()) {
                return false;
            }
            if (!list.equals(otherList)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int code = 1;
        List<String> keys = new ArrayList<>(map.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            code = code * 31 + key.hashCode();
            for (HttpHeader header : map.get(key)) {
                code = code * 31 + header.getValue().hashCode();
            }
        }
        return code;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Entry header : this) {
            sb.append(header.toString());
            sb.append("\r\n");
        }
        sb.append("\r\n");
        return sb.toString();
    }
}
