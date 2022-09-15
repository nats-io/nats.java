package io.nats.client.impl;



import javax.net.ssl.*;
import java.io.BufferedInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.SecureRandom;

public class TlsUtils {

    private static final String DEFAULT_ALGORITHM = "SunX509";
    private static final String DEFAULT_KEYSTORE_TYPE = "JKS";
    public static final String DEFAULT_SSL_PROTOCOL = "TLSv1.2";

    private static KeyStore loadKeystore(final String path, final char[] storePassword, final String keyStoreType) throws Exception {
        final KeyStore store = getKeyStore(path, keyStoreType);
        try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(path)))) {
            store.load(in, storePassword);
        }
        return store;
    }

    private static KeyStore getKeyStore(final String path, final String keyStoreType) throws KeyStoreException {

        KeyStore store;
        // . p12 or . pfx .
        if (keyStoreType == null ) {
            if (path.endsWith(".jks")) {
                store = KeyStore.getInstance("JKS");
            } else if (path.endsWith(".pfx") || path.endsWith(".p12")) {
                store = KeyStore.getInstance("pkcs12");
            } else {
                store = KeyStore.getInstance(DEFAULT_KEYSTORE_TYPE);
            }
        } else {
            store = KeyStore.getInstance(keyStoreType);
        }
        return store;
    }

    public static KeyManager[] createKeyManagers(final String path,
                                                 final char[] storePassword,
                                                 final String keyStoreType,
                                                 final String algorithm){

        try {
            final KeyStore store = loadKeystore(path, storePassword, keyStoreType);
            final KeyManagerFactory factory = KeyManagerFactory.getInstance(algorithm != null ? algorithm : DEFAULT_ALGORITHM);
            factory.init(store, storePassword);
            return factory.getKeyManagers();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TrustManager[] createTrustManagers(final String path,
                                                     final char[] storePassword,
                                                     final String keyStoreType,
                                                     final String algorithm)  {
        try {
            KeyStore store = loadKeystore(path, storePassword, keyStoreType);
            TrustManagerFactory factory = TrustManagerFactory.getInstance(algorithm != null ? algorithm : DEFAULT_ALGORITHM);
            factory.init(store);
            return factory.getTrustManagers();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    public static SSLContext createSSLContext(final String protocol,
                                              final TrustManager[] trustManagers,
                                              final KeyManager[] keyManagers)  {
        try {
            final SSLContext ctx = SSLContext.getInstance(protocol != null ? protocol : DEFAULT_SSL_PROTOCOL);
            ctx.init(keyManagers, trustManagers, new SecureRandom());
            return ctx;
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

}
