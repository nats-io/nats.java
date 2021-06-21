package io.nats.client.support;

import java.net.URI;
import java.net.URISyntaxException;

public interface URIUtils {
    static URI withScheme(URI uri, String scheme) {
        try {
            return new URI(scheme, uri.getSchemeSpecificPart(), uri.getFragment());
        } catch (URISyntaxException ex) {
            throw new IllegalStateException(ex);
        }
    }

    static URI withDefaultPort(URI uri, int defaultPort) {
        if (uri.getPort() >= 0) {
            return uri;
        }
        return withPort(uri, defaultPort);
    }

    static URI withPort(URI uri, int port) {
        try {
            return new URI(
                uri.getScheme(),
                uri.getUserInfo(),
                uri.getHost(),
                port,
                uri.getPath(),
                uri.getQuery(),
                uri.getFragment());
        } catch (URISyntaxException ex) {
            throw new IllegalStateException(ex);
        }
    }

    static URI withHost(URI uri, String host) {
        try {
            return new URI(
                uri.getScheme(),
                uri.getUserInfo(),
                host,
                uri.getPort(),
                uri.getPath(),
                uri.getQuery(),
                uri.getFragment());
        } catch (URISyntaxException ex) {
            throw new RuntimeException("Illegal hostname=" + host, ex);
        }
    }
}
