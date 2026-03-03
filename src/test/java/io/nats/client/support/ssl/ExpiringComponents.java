package io.nats.client.support.ssl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;

/**
 * ExpiringComponents of creating an SSLContext with an expiring client cert.
 */
public class ExpiringComponents {
    public final DiagnosticSslContext sslContext;
    public final X509Certificate clientCert;
    private final X509Certificate clientCaCert;

    ExpiringComponents(DiagnosticSslContext sslContext, X509Certificate clientCert, X509Certificate clientCaCert) {
        this.sslContext = sslContext;
        this.clientCert = clientCert;
        this.clientCaCert = clientCaCert;
    }

    /**
     * Write a NATS server config that uses the existing server.pem and key.pem
     * for server identity, but the generated CA cert for client verification.
     *
     * @return the absolute path to the config file
     */
    public String writeNatsConfig(Path dir) throws Exception {
        ExpiringClientCertUtil.writeCertPem(dir.resolve("client-ca.pem"), clientCaCert);
        String caPath = dir.resolve("client-ca.pem").toAbsolutePath().toString().replace('\\', '/');

        String config =
            "port: 4443\n\n"
                + "net: localhost\n\n"
                + "tls {\n"
                + "  cert_file: \"src/test/resources/certs/server.pem\"\n"
                + "  key_file: \"src/test/resources/certs/key.pem\"\n"
                + "  timeout: 2\n"
                + "  ca_file: \"" + caPath + "\"\n"
                + "  verify: true\n"
                + "}\n";

        Path configPath = dir.resolve("nats-tls-test.conf");
        Files.write(configPath, config.getBytes());
        return configPath.toAbsolutePath().toString();
    }
}
