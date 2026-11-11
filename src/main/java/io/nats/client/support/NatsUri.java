// Copyright 2023-2026 The NATS Authors
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

import io.nats.client.Options;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.regex.Pattern;

import static io.nats.client.support.NatsConstants.*;

public class NatsUri {
    private static final int NO_PORT = -1;
    private static final String UNABLE_TO_PARSE = "Unable to parse URI string.";
    private static final String UNSUPPORTED_SCHEME = "Unsupported NATS URI scheme.";
    private static final String ENTRY_CARRIES_A_SCHEME = "the entry carries a scheme";
    private static final String ENTRY_NOT_HOST_PORT = "the entry does not parse as host:port";
    private static final String ENTRY_PORT_OUT_OF_RANGE = "the port is not between 1 and 65535";
    private static final String URI_E_ALLOW_TRY_PREFIXED = "Illegal character in scheme name at index";
    private static final Pattern IPV4_RE = Pattern.compile("(([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])");
    private static final String COLON_SLASH_SLASH = "://";

    public static NatsUri DEFAULT_NATS_URI = new NatsUri();

    private final URI uri;
    private boolean isSecure;
    private boolean isWebsocket;
    private boolean hostIsIpAddress;
    private String tlsHost;
    private String inheritedUserInfo;

    @NonNull
    public URI getUri() {
        return uri;
    }

    @NonNull
    public String getScheme() {
        return uri.getScheme();
    }

    @NonNull
    public String getHost() {
        return uri.getHost();
    }

    public int getPort() {
        return uri.getPort();
    }

    @Nullable
    public String getUserInfo() {
        return uri.getUserInfo();
    }

    public boolean isSecure() {
        return isSecure;
    }

    public boolean isWebsocket() {
        return isWebsocket;
    }

    public boolean hostIsIpAddress() {
        return hostIsIpAddress;
    }

    /**
     * The hostname to present to the server during the TLS handshake. It differs from the host
     * when this uri was rehosted to an ip address by hostname resolution, or when it was discovered
     * as a bare ip address from a server that had a hostname.
     * @return the saved hostname, or the host when none has been saved
     */
    @NonNull
    public String getTlsHost() {
        return tlsHost == null ? getHost() : tlsHost;
    }

    /**
     * Copy this uri with a hostname to present to the server during the TLS handshake.
     * This does not change the host, the port, equality, the hash code or the string form.
     * @param tlsHost the hostname
     * @return a copy with the hostname
     */
    @NonNull
    public NatsUri withTlsHost(@NonNull String tlsHost) {
        NatsUri copy = new NatsUri(this);
        copy.tlsHost = tlsHost;
        return copy;
    }

    /**
     * The raw user info this server inherited from the server it was learned through, so that CONNECT
     * to a discovered server uses the credentials used for the server that reported it. It is never part
     * of the uri itself: not of its string form, its equality or its hash code.
     * @return the inherited raw user info, as it appears in a uri, or null when there is none
     */
    @Nullable
    public String getInheritedUserInfo() {
        return inheritedUserInfo;
    }

    /**
     * Copy this uri with raw user info to use for CONNECT when the uri itself has none.
     * This does not change the host, the port, the uri's own user info, equality, the hash code or the string form.
     * @param inheritedUserInfo the raw user info, as it appears in a uri, or null for none
     * @return a copy with the inherited user info
     */
    @NonNull
    public NatsUri withInheritedUserInfo(@Nullable String inheritedUserInfo) {
        NatsUri copy = new NatsUri(this);
        copy.inheritedUserInfo = inheritedUserInfo;
        return copy;
    }

    /**
     * The raw user info CONNECT uses for this server: the uri's own user info, or else the inherited user info.
     * @return the raw user info, or null when there is none
     */
    @Nullable
    public String getConnectUserInfo() {
        String own = uri.getRawUserInfo();
        return own == null ? inheritedUserInfo : own;
    }

    @NonNull
    public NatsUri reHost(String newHost) throws URISyntaxException {
        if (newHost.contains(":") && !newHost.startsWith("[")) {
            // this fixes IPV6 where it comes in without []
            // which it needs when making a url
            newHost = "[" + newHost + "]";
        }
        String newUrl = (uri.getRawUserInfo() == null)
            ? uri.getScheme() + "://" + newHost + ":" + uri.getPort()
            : uri.getScheme() + "://" + uri.getRawUserInfo() + "@" + newHost + ":" + uri.getPort();
        NatsUri rehosted = new NatsUri(newUrl, uri.getScheme());
        // keep the hostname for the TLS handshake when rehosting to an ip address
        if (rehosted.hostIsIpAddress() && (tlsHost != null || !hostIsIpAddress)) {
            rehosted.tlsHost = getTlsHost();
        }
        rehosted.inheritedUserInfo = inheritedUserInfo;
        return rehosted;
    }

    @Override
    public String toString() {
        return uri.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (this == o) return true;
        if (o instanceof NatsUri) {
            o = ((NatsUri)o).uri;
        }
        return uri.equals(o);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    public NatsUri() {
        try {
            uri = new URI(Options.DEFAULT_URL);
        } catch (URISyntaxException e) {
            // seriously, this better not happen!
            throw new RuntimeException(e);
        }
        postConstruct();
    }

    public NatsUri(@NonNull URI uri) throws URISyntaxException {
        this(uri.toString(), null);
    }

    private NatsUri(NatsUri source) {
        uri = source.uri;
        isSecure = source.isSecure;
        isWebsocket = source.isWebsocket;
        hostIsIpAddress = source.hostIsIpAddress;
        tlsHost = source.tlsHost;
        inheritedUserInfo = source.inheritedUserInfo;
    }

    public NatsUri(@NonNull String url) throws URISyntaxException {
        this(url, null);
    }

    public NatsUri(@NonNull String url, @Nullable String defaultScheme) throws URISyntaxException {
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

        [5X] has scheme, null host, non-null path, make/throw
        proto://u:p@      --> scheme:'proto', host:'null', up:'null', port:-1, path:''
        proto://:4222     --> scheme:'proto', host:'null', up:'null', port:-1, path:''
        proto://u:p@:4222 --> scheme:'proto', host:'null', up:'null', port:-1, path:''

        [5V6] also IPV6 ends up here
        proto://0:0:0:0:0:0:0:1:4222

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

        String prefix;
        if (defaultScheme == null) {
            prefix = NATS_PROTOCOL_SLASH_SLASH;
        }
        else {
            prefix = defaultScheme.toLowerCase();
            if (!prefix.endsWith(COLON_SLASH_SLASH)) {
                prefix += COLON_SLASH_SLASH;
            }
        }

        url = url.trim();
        Helper helper = parse(url, true, prefix);
        String scheme = helper.uri.getScheme();
        String path = helper.uri.getPath();
        if (scheme == null) {
            if (path != null) {
                // [3]
                helper = tryPrefixed(helper.url, prefix);
                scheme = helper.uri.getScheme();
                path = helper.uri.getPath();
            }
            else {
                // [X] not in the examples so don't know what to do, we are done
                throw new URISyntaxException(url, UNABLE_TO_PARSE);
            }
        }

        String host = helper.uri.getHost();
        if (host == null) {
            if (path == null) {
                // [4]
                helper = tryPrefixed(helper.url, prefix);
                scheme = helper.uri.getScheme();
                host = helper.uri.getHost();
            }
            else {
                // [5X]
                throw new URISyntaxException(url, UNABLE_TO_PARSE);
            }
        }

        if (host == null) {
            // if these aren't here by now, nothing we can do
            throw new URISyntaxException(url, UNABLE_TO_PARSE);
        }

        String lower = scheme.toLowerCase();
        if (!KNOWN_PROTOCOLS.contains(lower)) {
            throw new URISyntaxException(url, UNSUPPORTED_SCHEME);
        }
        if (!lower.equals(scheme) && helper.url.regionMatches(true, 0, scheme, 0, scheme.length())) {
            // lower case the scheme only; a replace over the whole url would also rewrite user info or a host
            helper.url = lower + helper.url.substring(scheme.length());
        }

        if (helper.uri.getPort() == NO_PORT) {
            // [6]
            uri = new URI(helper.url + ":" + DEFAULT_PORT);
        }
        else {
            uri = new URI(helper.url);
        }

        postConstruct();
    }

    private void postConstruct() {
        String s = uri.getScheme().toLowerCase();
        isSecure = SECURE_PROTOCOLS.contains(s);
        isWebsocket = WEBSOCKET_PROTOCOLS.contains(s);
        s = uri.getHost();
        hostIsIpAddress = IPV4_RE.matcher(s).matches() || s.startsWith("[") && s.endsWith("]");
    }

    static class Helper {
        String url;
        URI uri;

        public Helper(String url) throws URISyntaxException {
            this.url = url;
            this.uri = new URI(url);
        }
    }

    private Helper tryPrefixed(String url, String prefix) throws URISyntaxException {
        return parse(prefix + url, false, prefix);
    }

    private Helper parse(String url, boolean allowTryPrefixed, String prefix) throws URISyntaxException {
        try {
            return new Helper(url);
        }
        catch (URISyntaxException e) {
            if (allowTryPrefixed && e.getMessage().contains(URI_E_ALLOW_TRY_PREFIXED)) {
                // [4]
                return tryPrefixed(url, prefix);
            }
            else {
                // [5]
                throw e;
            }
        }
    }

    /**
     * The host and port of a {@code connect_urls} entry, in the form nats-server writes: {@code host:port}, with an
     * IPv6 address in brackets. User info before the host ({@code user@host:port}, {@code user:pass@host:port}) is
     * dropped, since a discovered server is sent the credentials used for the server that reported it. An entry that
     * carries a scheme ({@code nats://host:port}, {@code ws://host:port}) is not used, not even reduced to its host
     * and port, since a discovered server is dialed with the transport and the TLS of the server that reported it.
     * An entry that does not parse as {@code host:port} once user info is dropped, or whose port is outside 1 to 65535,
     * is not used. The host is checked the way {@link URI} checks a host, and kept as written.
     * @param entry the entry as the server sent it
     * @return the {@code host:port} of the entry
     * @throws URISyntaxException when the entry is not used; the reason says why
     */
    @NonNull
    public static String discoveredHostPort(@NonNull String entry) throws URISyntaxException {
        if (entry.contains(COLON_SLASH_SLASH)) {
            throw new URISyntaxException(entry, ENTRY_CARRIES_A_SCHEME);
        }
        String hostPort = entry.substring(entry.lastIndexOf('@') + 1); // drops user info; drops nothing when there is no @
        int colon = hostPort.lastIndexOf(':');
        if (colon < 1 || colon == hostPort.length() - 1) {
            throw new URISyntaxException(entry, ENTRY_NOT_HOST_PORT);
        }
        String host = hostPort.substring(0, colon);
        if (host.startsWith("[") ? !host.endsWith("]") : host.indexOf(':') != -1) {
            // an IPv6 address is in brackets; a colon anywhere else in the host means this is not host:port
            throw new URISyntaxException(entry, ENTRY_NOT_HOST_PORT);
        }
        int port = 0;
        for (int i = colon + 1; i < hostPort.length(); i++) {
            char c = hostPort.charAt(i);
            if (c < '0' || c > '9') {
                throw new URISyntaxException(entry, ENTRY_NOT_HOST_PORT);
            }
            port = port * 10 + (c - '0');
            if (port > 65535) {
                throw new URISyntaxException(entry, ENTRY_PORT_OUT_OF_RANGE);
            }
        }
        if (port == 0) {
            throw new URISyntaxException(entry, ENTRY_PORT_OUT_OF_RANGE);
        }
        URI uri;
        try {
            uri = new URI(NATS_PROTOCOL_SLASH_SLASH + host + ":" + port);
        }
        catch (URISyntaxException e) {
            throw new URISyntaxException(entry, ENTRY_NOT_HOST_PORT);
        }
        // the whole host must have been taken as the host: a path, query or fragment character inside it ends the host early
        if (!host.equals(uri.getHost()) || uri.getPort() != port) {
            throw new URISyntaxException(entry, ENTRY_NOT_HOST_PORT);
        }
        return host + ":" + port;
    }

    @NonNull
    public static String join(@NonNull String delimiter, @NonNull List<NatsUri> uris) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < uris.size(); i++) {
            if (i > 0) {
                sb.append(delimiter);
            }
            sb.append(uris.get(i));
        }
        return sb.toString();
    }

    private String equivalentComparable() {
        return uri.getHost().toLowerCase() + ":" + uri.getPort();
    }

    public boolean equivalent(@NonNull NatsUri other) {
        return equivalentComparable().compareTo(other.equivalentComparable()) == 0;
    }
}
