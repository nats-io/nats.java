// Copyright 2026 The NATS Authors
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

package io.nats.client.support.ssl;

import org.bouncycastle.asn1.*;
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.*;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;

/**
 * Utility for creating an SSLContext with a short-lived client certificate
 * for testing TLS reconnect behavior when client certs expire.
 * <p>
 * Uses the existing test server cert/key (server.pem, key.pem) and truststore
 * for server verification. Generates a fresh CA and short-lived client cert
 * at runtime using Bouncy Castle for client authentication.
 * <p>
 * The NATS server is configured with {@code verify: true} and the generated
 * CA cert as {@code ca_file}, so it requires and validates client certificates.
 * The client's SSLContext trusts the original CA (from truststore.jks) for
 * server verification and presents the short-lived cert for client auth.
 */
public class ExpiringClientCertUtil {

    private static final AlgorithmIdentifier SIG_ALG_ID = new AlgorithmIdentifier(
        PKCSObjectIdentifiers.sha256WithRSAEncryption, DERNull.INSTANCE);

    /**
     * Result of creating an SSLContext with an expiring client cert.
     */
    public static class Result {
        public final DiagnosticSslContext sslContext;
        public final X509Certificate clientCert;
        private final X509Certificate clientCaCert;

        Result(DiagnosticSslContext sslContext, X509Certificate clientCert, X509Certificate clientCaCert) {
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
            writeCertPem(dir.resolve("client-ca.pem"), clientCaCert);
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

    /**
     * Create an SSLContext whose client certificate expires after the given duration.
     * <p>
     * The SSLContext trusts the existing test CA (from truststore.jks) for server
     * verification and presents a dynamically generated short-lived client cert.
     * Use {@link Result#writeNatsConfig(Path)} to create a matching NATS server config.
     *
     * @param clientCertValidityMillis how long the client cert is valid (from now)
     * @return result with SSLContext and config writer
     */
    public static Result create(long clientCertValidityMillis) throws Exception {
        Date now = new Date();
        Date caExpiry = new Date(now.getTime() + 3_600_000); // CA valid for 1 hour

        // Generate a CA for signing client certs
        KeyPair caKP = generateKeyPair();
        X509Certificate caCert = generateCertificate(
            new X500Name("CN=Test Client CA, O=NATS Test"),
            new X500Name("CN=Test Client CA, O=NATS Test"),
            caKP.getPublic(), caKP.getPrivate(),
            now, caExpiry, true);

        // Generate short-lived client cert signed by this CA
        Date clientExpiry = new Date(now.getTime() + clientCertValidityMillis);
        KeyPair clientKP = generateKeyPair();
        X509Certificate clientCert = generateCertificate(
            new X500Name("CN=Test Client, O=NATS Test"),
            new X500Name("CN=Test Client CA, O=NATS Test"),
            clientKP.getPublic(), caKP.getPrivate(),
            now, clientExpiry, false);

        // Build SSLContext:
        // - TrustManagers: from existing truststore.jks (trusts original CA that signed server.pem)
        // - KeyManagers: short-lived client cert chained to generated CA
        TrustManager[] trustManagers = SslTestingHelper.createTestTrustManagers();

        char[] pw = "test".toCharArray();
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, null);
        ks.setKeyEntry("client", clientKP.getPrivate(), pw,
            new java.security.cert.Certificate[]{clientCert, caCert});
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, pw);

        DiagnosticSslContext ctx = DiagnosticSslContext.getInstance("TLSv1.2");
        ctx.init(kmf.getKeyManagers(), trustManagers, new SecureRandom());

        return new Result(ctx, clientCert, caCert);
    }

    /**
     * Create a Result where the client cert is already expired.
     */
    public static Result createExpired() throws Exception {
        Date now = new Date();
        Date caExpiry = new Date(now.getTime() + 3_600_000);

        KeyPair caKP = generateKeyPair();
        X509Certificate caCert = generateCertificate(
            new X500Name("CN=Test Client CA, O=NATS Test"),
            new X500Name("CN=Test Client CA, O=NATS Test"),
            caKP.getPublic(), caKP.getPrivate(),
            now, caExpiry, true);

        // Client cert: valid from 2 hours ago, expired 1 hour ago
        Date clientNotBefore = new Date(now.getTime() - 7_200_000);
        Date clientNotAfter = new Date(now.getTime() - 3_600_000);
        KeyPair clientKP = generateKeyPair();
        X509Certificate clientCert = generateCertificate(
            new X500Name("CN=Test Client, O=NATS Test"),
            new X500Name("CN=Test Client CA, O=NATS Test"),
            clientKP.getPublic(), caKP.getPrivate(),
            clientNotBefore, clientNotAfter, false);

        TrustManager[] trustManagers = SslTestingHelper.createTestTrustManagers();

        char[] pw = "test".toCharArray();
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, null);
        ks.setKeyEntry("client", clientKP.getPrivate(), pw,
            new java.security.cert.Certificate[]{clientCert, caCert});
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, pw);

        DiagnosticSslContext ctx = DiagnosticSslContext.getInstance("TLSv1.2");
        ctx.init(kmf.getKeyManagers(), trustManagers, new SecureRandom());

        return new Result(ctx, clientCert, caCert);
    }

    // -------------------------------------------------------------------
    // Certificate generation using Bouncy Castle ASN.1
    // -------------------------------------------------------------------

    private static KeyPair generateKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048, new SecureRandom());
        return kpg.generateKeyPair();
    }

    private static X509Certificate generateCertificate(
        X500Name subject, X500Name issuer,
        PublicKey publicKey, PrivateKey signingKey,
        Date notBefore, Date notAfter,
        boolean isCa) throws Exception {

        V3TBSCertificateGenerator tbsGen = new V3TBSCertificateGenerator();
        tbsGen.setSerialNumber(new ASN1Integer(new BigInteger(64, new SecureRandom())));
        tbsGen.setIssuer(issuer);
        tbsGen.setSubject(subject);
        tbsGen.setStartDate(new Time(notBefore));
        tbsGen.setEndDate(new Time(notAfter));
        tbsGen.setSignature(SIG_ALG_ID);
        tbsGen.setSubjectPublicKeyInfo(
            SubjectPublicKeyInfo.getInstance(publicKey.getEncoded()));

        // Extensions
        ExtensionsGenerator extGen = new ExtensionsGenerator();
        extGen.addExtension(Extension.basicConstraints, true,
            new BasicConstraints(isCa));
        tbsGen.setExtensions(extGen.generate());

        // Sign
        TBSCertificate tbs = tbsGen.generateTBSCertificate();
        Signature sig = Signature.getInstance("SHA256withRSA");
        sig.initSign(signingKey);
        sig.update(tbs.getEncoded());
        byte[] signature = sig.sign();

        // Assemble: { tbsCertificate, signatureAlgorithm, signatureValue }
        ASN1EncodableVector v = new ASN1EncodableVector();
        v.add(tbs);
        v.add(SIG_ALG_ID);
        v.add(new DERBitString(signature));

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        return (X509Certificate) cf.generateCertificate(
            new ByteArrayInputStream(new DERSequence(v).getEncoded()));
    }

    // -------------------------------------------------------------------
    // PEM / file helpers
    // -------------------------------------------------------------------

    static void writeCertPem(Path path, X509Certificate cert) throws Exception {
        byte[] der = cert.getEncoded();
        String pem = "-----BEGIN CERTIFICATE-----\n"
            + Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(der)
            + "\n-----END CERTIFICATE-----\n";
        Files.write(path, pem.getBytes());
    }

    /**
     * Recursively delete a temporary directory.
     */
    public static void deleteTempDir(Path dir) {
        if (dir == null) return;
        try {
            Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                });
        } catch (IOException ignored) {}
    }
}
