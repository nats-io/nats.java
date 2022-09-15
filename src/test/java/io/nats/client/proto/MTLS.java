package io.nats.client.proto;


import io.nats.client.Options;

import javax.net.ssl.*;
import java.io.BufferedInputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.SecureRandom;

public class MTLS {
    public static String ROOT = new File("environments/tls/").getAbsoluteFile().toString();
    public static String KEYSTORE_PATH =  ROOT + "/keystore.jks";
    public static String TRUSTSTORE_PATH = ROOT + "/truststore.jks";
    public static String STORE_PASSWORD ="password";
    public static String KEY_PASSWORD = "password";
    public static String ALGORITHM = "SunX509";


    public static KeyStore loadKeystore(String path) throws Exception {
        KeyStore store = KeyStore.getInstance("JKS");

        try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(path)))) {
            store.load(in, STORE_PASSWORD.toCharArray());
        }

        return store;
    }

    public static KeyManager[] createTestKeyManagers() throws Exception {
        KeyStore store = loadKeystore(KEYSTORE_PATH);
        KeyManagerFactory factory = KeyManagerFactory.getInstance(ALGORITHM);
        factory.init(store, KEY_PASSWORD.toCharArray());
        return factory.getKeyManagers();
    }

    public static TrustManager[] createTestTrustManagers() throws Exception {
        KeyStore store = loadKeystore(TRUSTSTORE_PATH);
        TrustManagerFactory factory = TrustManagerFactory.getInstance(ALGORITHM);
        factory.init(store);
        return factory.getTrustManagers();
    }

    public static SSLContext createSSLContext()  {
        try {
            SSLContext ctx = SSLContext.getInstance(Options.DEFAULT_SSL_PROTOCOL);
            ctx.init(createTestKeyManagers(), createTestTrustManagers(), new SecureRandom());
            return ctx;
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

}
