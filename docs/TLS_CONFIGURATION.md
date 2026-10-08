# TLS configuration

How the client decides whether a connection uses TLS, which `SSLContext` it uses, what `opentls` does and does not do, and the rule for server lists that mix schemes. This is the detail behind the SSLContext and Connection Security sections of the [README](../README.md).

## 1. One SSLContext per connection

A connection has one `SSLContext`. It is used for every server the connection ever talks to: the bootstrap servers given in the options and the servers discovered from the cluster through `connect_urls`. Discovered servers take the scheme of the first bootstrap server that is not plain `nats://`, so a connection bootstrapped with `tls://` reaches discovered servers with TLS and the same context.

The context is accepted or created once, when `Options.Builder.build()` runs, in this order. The first rule that applies wins and the rest are not consulted.

1. `sslContext(SSLContext ctx)` was called: that context, as given.
2. `sslContextFactory(SSLContextFactory)` was set, or the `io.nats.client.ssl.context.factory.class` property named a class: the factory builds the context from the keystore, truststore and algorithm properties.
3. `keystore` or `truststore` was set, by builder or property: the client builds a context from those JKS files with the `tls.algorithm` property, `SunX509` by default.
4. `opentls()` was called, the `io.nats.client.opentls` property is true, or any bootstrap server has the `opentls://` scheme: the trust-all context described in section 3.
5. `secure()` was called, the `io.nats.client.secure` property is true, or any bootstrap server has the `tls://` or `wss://` scheme: `SSLContext.getDefault()`, described in section 2.
6. Otherwise no context, and the connection is plain TCP. A server that requires TLS then fails the connect with "SSL required by server."

Rules 4 and 5 look at the bootstrap servers only when neither `opentls` nor `secure` was set explicitly. Asking for both is an error, section 4.

## 2. The default context: `tls://`, `wss://`, `secure()`

`SSLContext.getDefault()` is the JVM's default: it verifies the server's certificate chain against the JVM trust store, `cacerts` in the JDK's `lib/security`, or whatever the `javax.net.ssl.trustStore` system properties point at. It presents the client certificate from `javax.net.ssl.keyStore` when a server asks for one. It does not read the operating system's certificate store; a CA installed in Windows or macOS is not trusted by the JVM unless it is also in the JVM trust store or the JVM is configured to use the system store.

So `tls://` works out of the box against a server whose certificate chains to a public CA, and against a private CA once that CA is in the JVM trust store or supplied through the `truststore` properties, an `SSLContextFactory`, or an `SSLContext` you build.

## 3. The trust-all context: `opentls`

`opentls` builds a context whose trust manager accepts any server certificate chain, valid or not, from any issuer, expired or not, and presents no client certificate. The server must have client verification off for it to connect.

What that means: the connection is encrypted, and nothing checks who is on the other end. A server presenting any certificate at all is accepted, including one presented by something standing between the client and the real server. `opentls` gives confidentiality against a passive observer and no protection against an impersonated server. Every NATS client has a switch like it, under names such as `InsecureSkipVerify`, and all of them document it as unsuitable for production. Use it during development, or on a network where the server is trusted for other reasons, and nowhere else.

It is never selected unless asked for, by one of three routes: the `opentls()` builder method, the `io.nats.client.opentls=true` property, or an `opentls://` scheme on a bootstrap server. The third route means a configuration value alone can select it; review server URLs that come from configuration with that in mind.

Because the context is per connection, `opentls` applies to every server the connection reaches, including discovered ones.

## 4. Asking for both contexts is rejected

Options that ask for the default context and the trust-all context at the same time are rejected when they are built, with `IllegalStateException`:

```
Options ask for both the default SSL context (secure, or a tls or wss server url) and the trust-all SSL context (opentls, or an opentls server url). One SSL context serves every server, so a connection cannot both verify certificates and trust all of them. Choose one, or provide an SSLContext.
```

That covers `secure()` together with `opentls()`, the `secure` and `opentls` properties both true, and a bootstrap list that contains both an `opentls://` server and a `tls://` or `wss://` server. The reason is section 1: one context serves every server, so a connection cannot both verify certificates and trust all of them, and letting `opentls` win, which is what happened before this rule, silently removed the verification the other side asked for.

One explicit choice is not a conflict: `opentls()` with `tls://` servers builds the trust-all context, `secure()` with `opentls://` servers builds the default context, because an explicit flag stops the schemes from being consulted, and a context given with `sslContext(ctx)` is used whatever the flags and schemes are. Plain `nats://` servers may be mixed with either secure scheme, as before.

To fix rejected options, choose one of the two, or supply the context you want with `sslContext(ctx)`.

## 5. Client certificates

A server configured with `verify: true` requires a client certificate. Supply it through the `keystore` and `keystorePassword` properties together with `truststore` and `truststorePassword`, through an `SSLContextFactory`, through an `SSLContext` built with key managers, or through the `javax.net.ssl.keyStore` system properties when using the default context. `opentls` presents no client certificate and cannot be used against such a server.

The README's TLS Certs section shows how the test keystore and truststore are produced from the PEM files in `src/test/resources/certs`.

## 6. TLS handshake first

`tlsFirst()` performs the TLS handshake before reading the server's INFO, for servers configured with `handshake_first`. It requires a context: building options with `tlsFirst()` and no context from the rules above fails with "SSL context required for tls handshake first".

## 7. What the server and client expect of each other

Whether the client attempts the TLS upgrade is decided by whether it has a context; whether the server requires or offers TLS comes from its INFO. The README's "TLS client versus server checks" table lists the six combinations and the two that fail with an `IOException`.
