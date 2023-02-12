// Copyright 2023 The NATS Authors
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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.regex.Pattern;

import static io.nats.client.support.NatsConstants.*;

public class NatsUri {
    private static final int NO_PORT = -1;
    private static final String UNABLE_TO_PARSE = "Unable to parse URI string.";
    private static final String UNSUPPORTED_SCHEME = "Unsupported NATS URI scheme.";
    private static final String URI_EX_CAN_TRY_AGAIN = "Illegal character in scheme name at index";
    public static final String LOOPBACK = "127.0.0.1";
    public static final String LOCALHOST = "localhost";

    private final URI uri;

    public URI getUri() {
        return uri;
    }

    public String getScheme() {
        return uri.getScheme();
    }

    public String getHost() {
        return uri.getHost();
    }

    public int getPort() {
        return uri.getPort();
    }

    public String getUserInfo() {
        return uri.getUserInfo();
    }

    public boolean isSecure() {
        return SECURE_PROTOCOLS.contains(uri.getScheme().toLowerCase());
    }

    public boolean isWebsocket() {
        return WSS_PROTOCOLS.contains(uri.getScheme().toLowerCase());
    }

    public NatsUri reHost(String newHost) throws URISyntaxException {
        return new NatsUri(uri.toString().replace(uri.getHost(), newHost));
    }

    static Pattern IPV4_RE = Pattern.compile("(([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])");
    static Pattern IPV6_RE = Pattern.compile("((([0-9a-fA-F]){1,4})\\:){7}([0-9a-fA-F]){1,4}");

    public boolean hostIsIpAddress() {
        return IPV4_RE.matcher(uri.getHost()).matches()
            || IPV6_RE.matcher(uri.getHost()).matches();
    }

    @Override
    public String toString() {
        return uri.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NatsUri natsUri = (NatsUri) o;
        return uri.equals(natsUri.uri);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    public NatsUri(URI uri) throws URISyntaxException {
        this(uri.toString());
    }

    public NatsUri(String url) throws URISyntaxException {
    /*
        test string --> result of new URI(String)

        [1] provide protocol and try again
        1.2.3.4:4222 --> Illegal character in scheme name at index 0: 1.2.3.4:4222

        [2] throw exception
        proto:// --> Expected authority at index

        [3] null scheme, non-empty path? provide protocol and try again
        host    --> scheme:'null', host:'null', up:'null', port:-1, path:'host'
        1.2.3.4 --> scheme:'null', host:'null', up:'null', port:-1, path:'1.2.3.4'

        [4] has scheme but null host/path, provide protocol and try again
        x:p@host         --> scheme:'x', host:'null', up:'null', port:-1, path:'null'
        x:p@1.2.3.4      --> scheme:'x', host:'null', up:'null', port:-1, path:'null'
        x:4222           --> scheme:'x', host:'null', up:'null', port:-1, path:'null'
        x:p@host:4222    --> scheme:'x', host:'null', up:'null', port:-1, path:'null'
        x:p@1.2.3.4:4222 --> scheme:'x', host:'null', up:'null', port:-1, path:'null'

        [5] has scheme, null host, non-null path, make/throw
        proto://u:p@      --> scheme:'proto', host:'null', up:'null', port:-1, path:''
        proto://:4222     --> scheme:'proto', host:'null', up:'null', port:-1, path:''
        proto://u:p@:4222 --> scheme:'proto', host:'null', up:'null', port:-1, path:''

        [6] has scheme and host just needs port
        proto://host        --> scheme:'proto', host:'host', up:'null', port:-1, path:''
        proto://u:p@host    --> scheme:'proto', host:'host', up:'u:p', port:-1, path:''
        proto://1.2.3.4     --> scheme:'proto', host:'1.2.3.4', up:'null', port:-1, path:''
        proto://u:p@1.2.3.4 --> scheme:'proto', host:'1.2.3.4', up:'u:p', port:-1, path:''

        [7] has scheme, host and port
        proto://host:4222        --> scheme:'proto', host:'host', up:'null', port:4222, path:''
        proto://u:p@host:4222    --> scheme:'proto', host:'host', up:'u:p', port:4222, path:''
        proto://1.2.3.4:4222     --> scheme:'proto', host:'1.2.3.4', up:'null', port:4222, path:''
        proto://u:p@1.2.3.4:4222 --> scheme:'proto', host:'1.2.3.4', up:'u:p', port:4222, path:''
     */

        Worker worker = new Worker(url);
        String scheme = worker.uri.getScheme();
        String path = worker.uri.getPath();
        if (scheme == null) {
            if (path != null) {
                // [3]
                worker.tryPrefixedWithDefaultProto();
                scheme = worker.uri.getScheme();
                path = worker.uri.getPath();
            }
            else {
                // [X] not in the examples so don't know what to do, we are done
                throw new URISyntaxException(url, UNABLE_TO_PARSE);
            }
        }

        String host = worker.uri.getHost();
        if (host == null) {
            if (path == null) {
                // [4]
                worker.tryPrefixedWithDefaultProto();
                scheme = worker.uri.getScheme();
                host = worker.uri.getHost();
            }
            else {
                // [5]
                throw new URISyntaxException(url, UNABLE_TO_PARSE);
            }
        }

        if (host == null) {
            // if these aren't here by now, nothing we can do
            throw new URISyntaxException(url, UNABLE_TO_PARSE);
        }

        if (!KNOWN_PROTOCOLS.contains(scheme)) {
            throw new URISyntaxException(url, UNSUPPORTED_SCHEME);
        }

        if (worker.uri.getPort() == NO_PORT) {
            // [6]
            worker.url += ":" + DEFAULT_PORT;
        }

        uri = worker.done();
    }

    private static class Worker {
        String url;
        URI uri;

        public Worker(String url) throws URISyntaxException {
            work(url, true);
        }

        public void work(String inUrl, boolean allowTryAgain) throws URISyntaxException {
            try {
                url = inUrl.trim();
                uri = new URI(url);
            }
            catch (URISyntaxException e) {
                if (allowTryAgain && e.getMessage().contains(URI_EX_CAN_TRY_AGAIN)) {
                    // [4]
                    tryPrefixedWithDefaultProto();
                }
                else {
                    // [5]
                    throw e;
                }
            }
        }

        private void tryPrefixedWithDefaultProto() throws URISyntaxException {
            url = NATS_PROTOCOL_SLASH_SLASH + url;
            work(url, false);
        }

        public URI done() throws URISyntaxException {
            return new URI(url);
        }
    }

    public static String join(String delimiter, List<NatsUri> uris) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < uris.size(); i++) {
            if (i > 0) {
                sb.append(delimiter);
            }
            sb.append(uris.get(i).toString());
        }
        return sb.toString();
    }
}
