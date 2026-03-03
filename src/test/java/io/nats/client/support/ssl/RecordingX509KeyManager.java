package io.nats.client.support.ssl;

import javax.net.ssl.X509KeyManager;
import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * An X509KeyManager wrapper that records alias selection events
 * as {@link AliasSelectionEvent} instances.
 */
class RecordingX509KeyManager implements X509KeyManager {
    private final X509KeyManager delegate;
    private final List<AliasSelectionEvent> events;

    RecordingX509KeyManager(X509KeyManager delegate, List<AliasSelectionEvent> events) {
        this.delegate = delegate;
        this.events = events;
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) {
        return delegate.getClientAliases(keyType, issuers);
    }

    @Override
    public String chooseClientAlias(String[] keyTypes, Principal[] issuers, Socket socket) {
        String alias = delegate.chooseClientAlias(keyTypes, issuers, socket);
        events.add(new AliasSelectionEvent("chooseClientAlias", keyTypes, alias));
        return alias;
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
        return delegate.getServerAliases(keyType, issuers);
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
        String alias = delegate.chooseServerAlias(keyType, issuers, socket);
        events.add(new AliasSelectionEvent("chooseServerAlias", keyType, alias));
        return alias;
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
        return delegate.getCertificateChain(alias);
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
        return delegate.getPrivateKey(alias);
    }
}
