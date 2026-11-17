# ADR-40 TLS discovery and reconnect: the changes of pull request nats-io/nats.java#1668

The pull request delivers the client's conformance to ADR-40, revision 5, made in ten parts. This document carries the pull request description, with each part summarized in place of the list of parts, and a summary of every file the pull request changes, in the order of GitHub's file tree.

## Pull request description

TLS discovery and reconnect: the client's conformance to ADR-40, revision 5, made in ten parts and delivered here as one change.

## What changes

How the client treats the servers a cluster advertises, how it verifies them over TLS, how it reconnects, and what it writes in events and errors. The behavior before and after each change, with the option that restores the old behavior where there is one, is in `RELEASE_NOTES_ADR40.md`, referenced from the README under Important and under Recent Version Notes.

## The reviewed pull requests

The change was made in ten parts, in this order. Each part is summarized below with its commits, all of which are in this pull request.

### 1. Discovered servers: host:port entries, reporting server's transport, skipped entries reported

An entry of `connect_urls` is `host:port`, with an IPv6 address in brackets, the form nats-server writes and the only form the client uses. User info in an entry is dropped: the discovered server is sent the credentials the client uses for the server that reported it. An entry that carries a scheme is skipped as a whole, as is one that does not parse as `host:port` once its user info is dropped or whose port is outside 1 to 65535; the other entries of the INFO are used. The rule is `NatsUri.discoveredHostPort`, applied in `NatsConnection.handleInfo` before the pool sees the list and again in `NatsServerPool`.

A discovered server is dialed the way the server that reported it was dialed: it takes that server's scheme, so the same transport and, the context being per connection, the same TLS. Without an origin the pool's default scheme applies. A configured server named by an entry does not change.

Skipped entries are reported through `ErrorListener.discoveredServerSkipped(Connection, String entry, String reason)`, a default method called once per skipped entry each time an INFO carries it, with the entry as the server sent it; the logger and console listeners log it at WARNING with the entry quoted and control characters escaped, through `Encoding.quote`. A connection that ignores discovered servers uses no entry and is not told about skipped ones; the pool still receives the list. An INFO whose entries are all skipped drops the servers learned from earlier INFOs and keeps the configured servers and the current one; an INFO without `connect_urls`, or with an empty list, changes nothing. The rejection of a `ws://` server that a custom `ServerPool` hands out stays; a `connect_urls` entry can no longer bring one.

For a custom `ServerPool`, `acceptDiscoveredUrls` receives `host:port` strings only; a pool that gave its discovered servers a scheme from the entry takes it from the `origin` of `acceptDiscoveredUrls(List, NatsUri)` instead, as `NatsServerPool` does.

Compatibility: the detail string of `Events.DISCOVERED_SERVERS` is the normalized `host:port` list, user info dropped and leading zeros removed from the port; in a bootstrap list that mixes transports, a server discovered through `nats://a` is listed and dialed as `nats://`, the transport of the server that reported it.

Docs: README under the SSLContext rules; `docs/TLS_CONFIGURATION.md` sections 1 and 10. Tests: `OptionsTests.testNatsUriDiscoveredHostPort`; `ServerPoolTests.testDiscoveredServersTakeTheSchemeOfTheOrigin`, `testDiscoveredEntriesAreHostAndPortOnly`, `testDiscoveredServersInheritTheOriginsUserInfo`; `TLSConnectTests.testConnectUrlsEntriesWithASchemeAreSkippedAndNotDialed`; `ReconnectTests.testUserInfoInAConnectUrlsEntryIsNotUsedAsCredentials`; `ConnectionListenerTests`.

Commits:

- 87e4d0a8 Discovered servers: host:port entries only, dialed with the transport of the server that reported them, skipped entries reported
- 8addebdc Review of the discovered-entry rule: no skipped-entry reports when discovered servers are ignored, the quoting helper public as UntrustedText, the all-skipped case documented
- 778bd8b5 Review of the stack: acceptDiscoveredUrls states that the pool receives the list whether or not discovered servers are ignored, and that a custom pool honors the option itself

### 2. TLS server name option, trailing dot, name-check error

`Options.Builder.tlsServerName(String)`, property `io.nats.client.tls.server.name`, `Options.getTlsServerName()`: when set, it is the name the certificate of every server, configured or discovered, is verified against and sent as SNI, in place of the host of the url and of the hostname a discovered ip address inherits. It is for servers reached by ip address, or through a load balancer, proxy or service name, whose certificates carry one name, and it is the first remedy the javadoc, `docs/TLS_CONFIGURATION.md` and the README offer for a certificate that does not carry the name, before `tlsVerifyHostname(false)`. It asks for TLS: set with no other TLS setting it selects the default context, as `tlsFirst()` does, and the `ws://` rule applies. `build()` refuses a name that is neither a hostname nor an ip address, and a string with colons counts as an ip address only when it is an IPv6 address.

One trailing dot is stripped from the name verified against and sent as SNI, for every server, and the host is dialed as written: RFC 6066 has no trailing dot in SNI, nats-server refuses a handshake whose name ends in one, and a certificate carries the name without it.

A name that is an ip address is verified as an ip address and no SNI is sent; only a `tlsServerName` can be one, since a configured ip address has no TLS host.

The error of a failed name check is an `SSLHandshakeException` that states the name the client expected and where it came from, the host of its url, the `tlsServerName` of the options, or the hostname of the server that reported a discovered ip address, quotes the JDK's text, points at `tlsServerName` and at `client_advertise` for a server discovered from the cluster, and never at turning verification off. The JDK's `CertificateException` is its cause. Only the JDK's own name-check failure is wrapped; any other handshake failure passes through unchanged. The `IOException` of a failed initial connect, "Unable to connect to NATS servers", carries the last attempt's failure as its cause. A `contains` check on the old JDK message still matches the new one; `startsWith` or equality checks do not.

Docs: `docs/TLS_CONFIGURATION.md` sections 2 and 4; README property row, hostname verification note and TLS section. Tests: `OptionsTests.testTlsServerName`; `TlsHostnameTests.tlsServerNameIsTheNameOfEveryServer`, `tlsServerNameOverridesTheUrlHostAndTheInheritedName`, `tlsServerNameThatIsAnIpAddressIsVerifiedAsOneAndSendsNoSni`, `trailingDotIsStrippedFromTheNameVerifiedAgainstAndSentAsSni`, `nameCheckErrorStatesTheExpectedNameItsSourceAndTheRemedies`, `nameCheckFailureWrapsOnlyTheJdkNameCheck`.

Commits:

- 44686a78 TLS server name option; trailing dot stripped from the name verified and sent as SNI; name-check error states the expected name and the remedies
- 63ba880c Merge branch 'adr-conformance-discovered-entries' into adr-conformance-tls-server-name
- 1f259ac8 Review of the TLS server name: a name with a colon is not an ip address and is refused at build(), the connect failure is per attempt, the name-check wrapping is documented as the JDK's text
- 35a42d0e Merge adr-conformance-discovered-entries: the stack review fixes

### 3. TLS name of a discovered server over hops; verify discovered servers by ip

A server discovered as an ip address is verified against, and presents as SNI, the hostname it inherits from the server that reported it: the hostname that server was configured with, or the one it inherited itself, so the name travels over any number of hops. `NatsUri.tlsHostToInherit()` is the rule and `NatsServerPool.acceptDiscoveredUrls` applies it.

Under TLS with the name check on, a discovered ip address learned through a server given as an ip address has no name to verify against. While neither `tlsServerName` nor `tlsVerifyDiscoveredByIp` is set, `NatsConnection.handleInfo` skips the entry before the pool sees it and reports it through `discoveredServerSkipped` with a reason that names the two settings; a custom pool never receives such an entry.

`Options.Builder.tlsVerifyDiscoveredByIp()`, property `io.nats.client.tls.verify.discovered.by.ip`, `Options.isTlsVerifyDiscoveredByIp()`: a discovered ip address is then verified against its address with no SNI, inheriting nothing, and one with no name is connected to. It is for clusters whose node certificates carry only ip addresses, and is documented as safe only where the CA that issued them is not shared with others. It has no effect while a TLS server name is set, and neither it nor the skip applies with the name check turned off. It asks for TLS like a TLS server name.

Compatibility: a deployment that connects by ip address to a cluster that advertises ip addresses, with certificates that carry those addresses, needs `tlsVerifyDiscoveredByIp()` or `tlsServerName`; without one of them the discovered servers are skipped and reported, and the connection has no failover. A deployment that connects by hostname to a cluster that advertises ip addresses two or more hops deep verifies every hop against that hostname.

Docs: `docs/TLS_CONFIGURATION.md` section 4; README property row and TLS section; the javadocs of `tlsVerifyHostname`, `ServerPool.acceptDiscoveredUrls` and `ErrorListener.discoveredServerSkipped`. Tests: `OptionsTests.testTlsVerifyDiscoveredByIp`; `TlsHostnameTests.discoveredIpRetainsHostnameOverTwoHops`, `discoveredIpWithNoNameToInheritIsSkippedUnlessAllowed`, `discoveryInheritsNoNameWhenDiscoveredServersAreVerifiedByIp`; `TLSConnectTests.testConnectUrlsEntriesWithASchemeAreSkippedAndNotDialed`.

Commits:

- e58e056f TLS name of a discovered server: inherited over any number of hops; a discovered ip address with no name is skipped unless tlsVerifyDiscoveredByIp or tlsServerName is set
- eccf410e Merge branch 'adr-conformance-tls-server-name' into adr-conformance-discovered-tls-name
- 1e2cd2a6 Merge adr-conformance-tls-server-name: the stack review fixes

### 4. Reconnect: round wait, zero jitter, per-server counts, origin kept

The wait between rounds holds across the whole reconnect: `reconnectImplConnect` keeps the servers handed out in the current round and ends the round when any of them comes back, so the wait holds when the round's first server left the pool through its attempt limit or an INFO that pruned it. `connectImpl` ends its one pass over the pool the same way.

A `reconnectJitter` or `reconnectJitterTls` of zero means no jitter: the random part is added only when the jitter is above zero, and the builder refuses a negative one.

Reconnect counts are per server. A server that enters the list during a reconnect, discovered from the cluster or named again after it left, starts at the higher of the count it kept when it left and the highest count of any server in the list, the origin of the list excluded; a server that left keeps its count; a successful connect resets the server it connected to only, so the counts accumulate across the reconnects of a connection. At the limit a server has no attempt left: it does not enter the list and keeps that count, so a server that left at its limit, configured or discovered, stays out while the connection lasts, and the connection closes at `maxReconnects` during an outage in which servers keep naming each other.

`acceptDiscoveredUrls` keeps the origin of the list, the server whose INFO it is, which during a reconnect is being connected to and is not yet the last connected one when its own INFO arrives.

The `maxReconnects` and the jitter javadocs state the rules. Tests: `OptionsTests.testReconnectJitterZeroMeansNoJitter`; `ServerPoolTests.testServerAddedStartsAtTheRaisedCount`, `testConfiguredServerAtItsLimitStaysOut`, `testServerThatSentTheListIsKept`, `testMaxReconnects`; `ReconnectTests.testReconnectWithZeroJitter`, `testReconnectWaitHoldsWhenTheRoundsFirstServerLeavesThePool`.

Commits:

- e5520ee4 Reconnect: the wait between rounds holds when the round's first server leaves the pool; a jitter of zero means no jitter; per-server counts are kept and raised for servers that enter the list; the server that sent a list is kept
- 007918e8 Review of the reconnect counts: the origin's count does not lift the servers its list names; the javadoc says the counts accumulate across reconnects
- 7e64957e Merge adr-conformance-discovered-tls-name: the stack review fixes

### 5. Reader robustness and attempt deadlines

An INFO that does not parse on an established connection is skipped and reported through `exceptionOccurred`, quoted as untrusted text, and the connection stays up; during an attempt it fails the attempt.

A header block that does not parse drops only the message that carries it: the block is handed to `IncomingHeadersProcessor` when it is complete, and when that fails for any reason the message is marked, its data is read and discarded, and the drop is reported. nats-server relays a header block as the publisher wrote it, so such a block can reach any subscriber.

`parseLength` computes in a long and rejects a value above an int. A length above twice the server's max payload, and never below 64 MiB, and a header length above the total length are parse errors that make the client reconnect instead of allocating the memory. Any other `RuntimeException` in the reader loop is reported and makes the client reconnect: the reader does not stop silently.

A `-ERR` after which the server closes the connection fails the attempt at once with the server's text: the read failure answers the attempt's pending PING, and the `IOException` a connect throws carries the server's error ahead of the read error.

The attempt's deadline covers the data port's connect: the TCP connect, a `wss://` TLS handshake, the WebSocket upgrade and the name resolution of the non-resolving modes run on the executor under the attempt's remaining time; at the deadline the port is force-closed, which unblocks an upgrade that hangs, and `close()` force-closes the port of an attempt in progress. A port closed while its connect still runs closes the socket it gets and fails, and the write-timeout port schedules no watch task for it.

The name lookup of the resolving modes runs on the executor under the connection timeout, with one lookup in flight per server across the rounds of a reconnect; a lookup that does not finish in time, fails or cannot be started is a failed attempt of that server, reported, counted toward the server's limit and never a TLS mismatch. The `executor` and `ServerPool.resolveHostToIps` javadocs state the threading.

The addresses of one name share one budget: a name that resolves to several addresses is one attempt, the addresses tried in turn under the attempt's deadline; the `connectionTimeout` and `ResolveToAll` javadocs state it.

A TLS upgrade whose handshake-completed notification does not arrive within the connection timeout fails the attempt, and the attempt's recorded failure is checked before CONNECT is written as well as after the PING.

Compatibility: a connect against a server that refuses with `-ERR` and closes fails at once with the server's error as the cause; a WebSocket upgrade and a name lookup that hang fail the attempt at the connection timeout.

Tests: `InfoHandlerTests.testInfoThatDoesNotParseIsSkippedOnAnEstablishedConnection`, `testHeaderBlockThatDoesNotParseDropsOnlyThatMessage`, `testHeaderBlockThatDoesNotParseFromAPublisherThroughARealServerDropsOnlyThatMessage`; `ParseTests.testLengthBeyondWhatAServerCouldSendIsAParseError`, `testInfoThatDoesNotParseIsSkippedByTheReader`; `WebsocketConnectTests.testWebSocketUpgradeIsBoundedByTheConnectionTimeout`; `ConnectTests.testNameLookupIsBoundedByTheConnectionTimeout`, `testAddressesOfOneNameShareOneAttemptBudget`, `testLookupFailureIsAFailedAttemptNotATlsMismatch`, `testLookupThatCannotBeStartedIsAFailedAttempt`, `testAHungLookupIsNotRepeatedEveryRound`, `testAnAttemptAbandonedAtItsDeadlineLeavesNoOpenSocketAndNoWriteWatchTask`; `TLSConnectTests.testAnUnconfirmedHandshakeFailsTheAttemptBeforeConnect`.

Commits:

- 36071b11 Reader and attempt robustness: an INFO that does not parse is skipped and reported, a header block that does not parse drops only its message, impossible lengths and any parse failure reconnect instead of ending the reader, a server error ends the attempt at once, the data port connect and the name lookup are bounded
- 4675dbed Review of the reader robustness: any failure of the header parser drops only the message, the length bound is twice the max payload and never below 64 MiB, the data port's socket is visible to the thread that closes it
- c968b86d Merge branch 'adr-conformance-reconnect' into adr-conformance-reader
- ec3c7893 Review of the stack: a failed name lookup is a failed attempt, a port closed during its connect stays closed, an unconfirmed TLS handshake fails the attempt before CONNECT
- 36fc5f11 Review of the stack: the executor, resolveHostToIps, ResolveToAll and connectionTimeout javadocs state the connect threading and the one budget of a name's addresses; copyright years
- 4a70fd2d Merge adr-conformance-reconnect: the stack review fixes
- 9cd5b733 Review of the stack: one name lookup in flight per server, so a lookup the connection gave up on is waited on by the next round instead of being started again

### 6. Credentials redacted in events, errors and traces

`NatsUri.toRedactedString()` is the url with its credentials hidden: a password becomes `***`, a token as a whole, and the user, scheme, host, port and path stay as they are. `NatsUri.redactUrl(String)` does the same for a url string by position, parsing nothing, with the authority ending at the first of `/`, `?` and `#`; for a string that does not parse it replaces whatever stands between its `://` and its last `@`.

Redacted: the detail string of every connection event, the "Unable to connect to NATS servers" failure and the TLS-mismatch failure, the connect and reconnect trace lines, and the `serverRejected` report. `Connection.getConnectedUrlRedacted()`, a default method, gives the connected url in that form; `getConnectedUrl()` and `getServers()` are unchanged, and their javadocs, with `Options.getServers()`, say the urls carry the credentials as configured and point at the redacted forms. `Options.Builder.servers` refuses a url that does not parse with the reason and the redacted url, and a cause whose input is redacted too.

Compatibility: a connection listener's `uriDetails`, the text of a failed connect and the `serverRejected` string no longer contain the password of a url, and for a credentialed url no longer equal what `getConnectedUrl()` and `getServers()` return.

Tests: `OptionsTests.testNatsUriRedacted`; `AuthTests.testCredentialsAreRedactedInEventsAndErrors`.

Commits:

- 22cf3585 Credentials of a server url are hidden in events, errors and trace lines; Connection.getConnectedUrlRedacted()
- 29ba260a Review of the redaction: urls are redacted in place, the url parse error hides the credentials, the getServers javadocs point at the redaction
- cfc4e55d Review of the stack: redactUrl ends the authority at the first of /, ? and #, so an @ in a query or fragment is not taken for user info; Connection.java copyright year
- deb90d00 Merge adr-conformance-reader: the stack review fixes
- 3f1fe266 Merge adr-conformance-reader: the stack review fixes, one lookup in flight per server

### 7. TLS 1.3 offered, TLS 1.2 floor on every connection

`Options.DEFAULT_SSL_PROTOCOL` is `TLS`, the highest version the JVM supports, so the contexts the client builds from the keystore and truststore properties and for `opentls` offer TLS 1.3 where the JVM has it; the default context is the JVM's own.

`upgradeToSecure` keeps, of the protocols the socket enables, only those at TLS 1.2 or above, whatever the context: the client never negotiates TLS 1.0, 1.1 or SSL and never falls back to them. The filter, `tlsProtocolsAtOrAbove12`, is an allow-list on the name form `TLSv1.N` with N at least 2, so an unknown protocol name is dropped rather than kept. A context that enables nothing at or above 1.2 gets no protocol and its handshake fails.

Compatibility: a connection to a server that offers only TLS 1.0 or 1.1 fails on every context; a context the application supplies is not changed, only floored; `Options.DEFAULT_SSL_PROTOCOL` is a compile-time constant, so code compiled against an earlier jar keeps `TLSv1.2` inlined until it is rebuilt.

Docs: README "Connection Security" and `docs/TLS_CONFIGURATION.md` section 2. Tests: `OptionsTests.testBuiltContextsOfferTheHighestTlsVersion`; `TLSConnectTests.testTlsProtocolsAtOrAbove12`, `testBuiltContextNegotiatesTls13AndEnablesNothingBelow12`.

Commits:

- 6bdf2786 TLS versions: built contexts offer TLS 1.3, every connection enables TLS 1.2 and above only
- 7d4f4d71 Review of the TLS versions: the protocol filter's javadoc states the allow-list choice
- 334cbb38 Merge adr-conformance-redaction: the stack review fixes
- 5fcd2bb5 Merge adr-conformance-redaction: the stack review fixes, one lookup in flight per server

### 8. Opt out of the authorization abort

`Options.Builder.ignoreAuthErrorAbort()`, property `io.nats.client.ignore.auth.error.abort`, `Options.isIgnoreAuthErrorAbort()`. By default the second identical authorization error from one server during a reconnect, an authorization violation, an expired user authentication or an expired account, ends the reconnect and the connection closes; with the option the reconnect goes on until the connection is closed or `maxReconnects` is reached. An error line received before a TLS handshake fails the handshake without being parsed, so it never counts toward the abort. The javadoc names `NatsConnection.isAuthenticationError` as the definition of the errors that count. The default connection timeout stays 2 s.

Docs: the README property table. Tests: `OptionsTests.testIgnoreAuthErrorAbort`; `AuthTests.testAuthErrorAbortCanBeSwitchedOff`.

Commits:

- d81ef153 Authorization abort: ignoreAuthErrorAbort keeps reconnecting after repeated authorization errors
- d3334c0a Review of the authorization abort: the option's javadoc and isAuthenticationError point at each other
- 7508eff5 Merge adr-conformance-tls-versions: the stack review fixes
- 91cb268e Merge adr-conformance-tls-versions: the stack review fixes, one lookup in flight per server

### 9. Scenario tests

`TlsDiscoveryScenarioTests`, test code only. Against two real nats-servers routed to each other, each advertising `127.0.0.1` and its port, the seed killed once the client holds the discovered entry: a discovered ip address is reconnected to over TLS under the name inherited from the seed, with a certificate that names `localhost` only, and carries traffic afterwards; TLS is kept on the reconnect when both servers offer plaintext through `allow_non_tls`; a discovered server with a certificate from an untrusted CA, with a trusted certificate for another name, or without TLS is never reconnected to, the failure is reported and the connection closes once every server has run out of attempts; a server advertised under a rooted name, `localhost.`, is dialed as written and verified and sent as SNI as `localhost`.

Against `SniTestServer`: plaintext injected after INFO and before the upgrade is not acted on, the client answers nothing, closes, fails the attempt as a handshake failure quoting the first injected line, and never holds the server the injection advertised; a protocol line before INFO is not acted on; the plaintext INFO of a seed whose certificate the client does not trust never steers the client. Url credentials carried to discovered servers, and server-advertised `ws://` and `nats://` entries against a TLS client, are covered by `ReconnectTests.testUrlCredentialsReachDiscoveredServers` and `TLSConnectTests.testConnectUrlsEntriesWithASchemeAreSkippedAndNotDialed`.

Test infrastructure: `SniTestServer` can write plaintext before or after the INFO line, records what the client then sent in plaintext, and counts accepted connections; `Certificates.writePem` writes a generated certificate and key as PEM files for a nats-server `tls` block; `cluster_plain.conf` is a server with nothing but a port, the tests adding the cluster and `tls` blocks as inserts. The exceptions handed to `exceptionOccurred` for a failed reconnect attempt are the raw ones, the JDK's `SSLHandshakeException` for a trust failure and the `IOException` for the TLS mismatch, and name no server; the name-check failure does, since it is wrapped.

Commits:

- d9983622 Scenario tests: the end-to-end cases of the TLS discovery and reconnect standard, in the client's own suite
- d92bd93f Review of the scenario tests: the trust-failure needle accepts both JDK texts, the cluster wait looks for the other member, the single-write injection is stated
- 01d895c0 Merge adr-conformance-auth-abort: the stack review fixes
- c0063409 Merge adr-conformance-auth-abort: the stack review fixes, one lookup in flight per server

### 10. ADR-40 refactoring

`NatsConnection.quoteUntrusted(String)` and the no-argument `NatsConnection.readInitialInfo()`, both without callers, are removed; `readInitialInfo(boolean)` stays. `UntrustedText` is folded into `Encoding`: `Encoding.quoteFirstLine(byte[])` and `Encoding.quote(String)` are the same functions, with the cut at 256 bytes and the escaping, and the callers in `NatsConnection`, `NatsConnectionReader`, the console and logger error listeners, the `ErrorListener` javadoc, `InfoHandlerTests` and `docs/TLS_CONFIGURATION.md` point at `Encoding`. No behavior change. Also in this part: the rooted `client_advertise` scenario test, and `RELEASE_NOTES_ADR40.md` with the README's Important and Recent Version Notes references.

Commits:

- 90682986 Remove the unused quoteUntrusted(String) from NatsConnection
- 4e9a6ff6 Remove the no-argument readInitialInfo from NatsConnection
- 2745fd40 Move the quoting of untrusted text into Encoding, drop UntrustedText
- f58735d7 Merge adr-conformance-scenario-tests: the review fixes of the scenario tests
- 8b67f493 Rooted client_advertise against real servers: a server discovered as localhost. is dialed as written, and verified and sent as SNI as localhost
- 8d91743e Release notes for the TLS discovery and reconnect changes, referenced from the README's Important and Version Notes sections
- 0f160313 Merge adr-conformance-scenario-tests: the stack review fixes
- d9183ce1 Merge adr-conformance-scenario-tests: the stack review fixes, one lookup in flight per server

## Compatibility

The fifteen notes in `RELEASE_NOTES_ADR40.md`. The ones most likely to reach a user: a discovered server is a `host:port` entry dialed with the transport and TLS of the server that reported it, and a custom `ServerPool` receives `host:port` only; an ip-address seed with an ip-address cluster needs `tlsVerifyDiscoveredByIp()` or `tlsServerName`; `maxReconnects` holds during an outage in which the servers keep naming each other; the addresses of one hostname share one connection timeout per attempt; TLS 1.2 is the floor; events, errors and trace lines no longer carry url passwords; `ignoreAuthErrorAbort()` is new and off by default.

## Tests

`TlsDiscoveryScenarioTests` runs the scenarios of the standard end to end against two real nats-servers and against the fake TLS server, nine in all. The other tests sit with the code they cover: `TlsHostnameTests`, `TLSConnectTests`, `ConnectTests`, `ReconnectTests`, `ServerPoolTests`, `OptionsTests`, `InfoHandlerTests`, `AuthTests`, `WebsocketConnectTests`.

## Files

One section per file, 35 in all, in three groups: Project for the root items and the documents, Source for everything under `src/main/java`, Test Source for everything under `src/test`. Within a group the files follow the order of GitHub's file tree: directories before files at each level, each sorted by name. The first line of a section is the file's path from the project root.

## Project

### ADR40_TLS_DISCOVERY_AND_RECONNECT_CHANGELOG.md

`docs/ADR40_TLS_DISCOVERY_AND_RECONNECT_CHANGELOG.md`

This document: the pull request description, with a summary and the commits of each of the ten parts of the change, and a summary of every file the pull request changes, in the order of GitHub's file tree.

### TLS_CONFIGURATION.md

`docs/TLS_CONFIGURATION.md`

Updated for the new rules. A discovered server takes the scheme and the TLS of the server that reported it. `tlsFirst()` and `tlsServerName()` select the default context when nothing else has. The contexts the client builds are created for `TLS` and offer TLS 1.3, and every connection enables TLS 1.2 and above only. `tlsServerName` is described as the first remedy when certificates do not carry the name in the url. The name-check failure is an `SSLHandshakeException` that states the expected name and where it came from, with the JDK's `CertificateException` as its cause. A new section 10 describes the form of a `connect_urls` entry: `host:port` only, user info dropped, entries with a scheme skipped and reported, and what a custom `ServerPool` receives.

### README.md

`README.md`

The snapshot badge and the Gradle, Maven and uber-jar coordinates say 2.27.0. The Important section opens with "2.27.0 Changes to secure connection behavior" and points at the two version-note sections and the release notes. The hostname verification note is now for 2.27.0 and names `tlsServerName` as the first remedy. A new section, Version 2.27.0 TLS Discovery and Reconnect, lists the seven changes most likely to reach a user. The property table gains `ignore.auth.error.abort`, `tls.server.name` and `tls.verify.discovered.by.ip`. The TLS text says the client negotiates TLS 1.3 where available and never below TLS 1.2, the SSLContext rules say `tlsServerName()` selects the default context, the `connect_urls` paragraph says an entry with a scheme is skipped and reported, and the Connection Security section describes `tlsServerName` and `tlsVerifyDiscoveredByIp`.

### RELEASE_NOTES_ADR40.md

`RELEASE_NOTES_ADR40.md`

New. Fifteen notes for the release, each with the behavior before, the behavior now and the option that restores the old behavior where there is one: discovered entries as `host:port`, the DISCOVERED_SERVERS event detail, the scheme of a discovered server, ip-address seeds, the inherited name over hops, the name-check exception, `maxReconnects` during an outage, jitter, the shared connection timeout of a name's addresses, faster failed attempts and bounded lookups, malformed input, url password redaction, the TLS 1.2 floor, `ignoreAuthErrorAbort()`, and the unconfirmed TLS handshake.

### build.gradle

`build.gradle`

`jarVersion` is 2.27.0, a minor release.

## Source

### ErrorListenerConsoleImpl.java

`src/main/java/io/nats/client/impl/ErrorListenerConsoleImpl.java`

Implements `discoveredServerSkipped`, printing the entry quoted as untrusted text and the reason.

### ErrorListenerLoggerImpl.java

`src/main/java/io/nats/client/impl/ErrorListenerLoggerImpl.java`

Implements `discoveredServerSkipped`, logging the entry quoted as untrusted text and the reason.

### NatsConnection.java

`src/main/java/io/nats/client/impl/NatsConnection.java`

The connect pass and the reconnect round end when any server handed out comes back, not only the first one, so the wait between rounds holds when the round's first server left the pool. Each attempt records its failure in `connectFailure`, the cause of the connect's `IOException`, and its TLS mismatch; the connection closes without retrying only when every server of a pass or a round failed with the mismatch. A second identical authorization error from one server ends the reconnect unless `ignoreAuthErrorAbort` is set. The data port's connect runs as a task on the executor under the attempt's remaining time, with the port in `attemptDataPort` force-closed at the deadline or on close. The name lookup runs in `resolveHostBounded` on the executor under a timer of its own, with one lookup in flight per server across rounds; a lookup that fails, runs out of time or cannot be started is a failed attempt of that server and never a TLS mismatch. The addresses of one name share one attempt budget. `throwIfAttemptFailed` ends the attempt before CONNECT and after the PING when another thread recorded a failure. A `-ERR` after which the server closes fails the attempt at once with the server's error as the failure. With TLS after INFO, the plaintext INFO's `connect_urls` and lame-duck flag are not used, discovered servers come from the INFOs inside TLS, and against nats-server 2.12 and later the client waits for the INFO that follows the first PONG before reporting connected. An INFO that does not parse on an established connection is skipped and reported. `connect_urls` entries are normalized to `host:port` with user info dropped; an entry with a scheme, one that does not parse or one with a port out of range is skipped and reported through `discoveredServerSkipped`, as is, under TLS with the name check on, a discovered ip address with no name to verify against unless `tlsVerifyDiscoveredByIp` is set. The DISCOVERED_SERVERS event carries the normalized list. Events, errors, trace lines and the `serverRejected` string name servers with their credentials redacted, through `redacted(...)` and `getConnectedUrlRedacted()`. The quoting of untrusted text moved to `Encoding`; the no-argument `readInitialInfo()` and `quoteUntrusted(String)` are removed.

### NatsConnectionReader.java

`src/main/java/io/nats/client/impl/NatsConnectionReader.java`

A `RuntimeException` that escapes the parsing is reported as a communication issue and makes the client reconnect, instead of ending the reader's thread without a word. A header block that does not parse drops only the message that carries it, reported through `exceptionOccurred`, with `discardIncoming` skipping that message's data. `parseLength` refuses a value above `Integer.MAX_VALUE` instead of wrapping. `checkIncomingLength` treats a length above twice the server's max payload, and never below 64 MiB, as a line that does not parse, and an HMSG whose header length exceeds its total length is a parse error. An INFO that does not parse is skipped on an established connection and fails the attempt during one.

### NatsServerPool.java

`src/main/java/io/nats/client/impl/NatsServerPool.java`

Discovered entries are parsed through `NatsUri.discoveredHostPort` and given the scheme of the origin, or the pool's default scheme without one; an entry the rule refuses never becomes a server. The origin, the server whose INFO it is, is kept in the list like the configured servers and the last connected one. A discovered ip address inherits the name the origin was configured with or inherited itself, so the name travels over any number of hops, and inherits nothing under `tlsVerifyDiscoveredByIp`. Reconnect counts are per server: `keptCounts`, bounded to 1024 entries, keeps the count of a server that left the list at its limit; a server that enters the list starts at the higher of its kept count and the highest count in the list, the origin excluded, and does not enter at the limit; `connectSucceeded` resets the count of that server only; `connectFailed` removes a server at its limit and keeps its count. Entries are keyed by `hostPortKey()`.

### SocketDataPort.java

`src/main/java/io/nats/client/impl/SocketDataPort.java`

The TLS host and where it came from, `TLS_HOST_FROM_URL`, `TLS_HOST_FROM_OPTIONS` or `TLS_HOST_FROM_REPORTING_SERVER`, which the name-check error states. `tlsServerName` is the name of every server. One trailing dot is stripped from the name verified against and sent as SNI; a name that is an ip address is verified as one and no SNI is sent. `socket` is volatile and a `closed` flag set by `close()` and `forceClose()` makes a connect that completes afterwards close its socket and fail. `tlsProtocolsAtOrAbove12` keeps only `TLSv1.N` with N at least 2 and is applied to every upgrade. `awaitHandshake` throws when the handshake-completed notification does not arrive within the connection timeout, where the upgrade used to report and return with the plaintext streams in place. `nameCheckFailure` wraps the JDK's name-check failure, recognized by `isNameCheckMessage`, in an `SSLHandshakeException` that states the expected name, its source and the remedies.

### SocketDataPortWithWriteTimeout.java

`src/main/java/io/nats/client/impl/SocketDataPortWithWriteTimeout.java`

The write-watch task is volatile. It is not scheduled when the connect threw, and it is stopped when the port was closed between the connect and the scheduling. `close()` closes the socket first and stops the task after, so the two orderings cannot leave a task running. Copyright year.

### Encoding.java

`src/main/java/io/nats/client/support/Encoding.java`

`quoteFirstLine(byte[])` and `quote(String)`: the quoting of untrusted text from a server, the first line up to CR or LF or one token, for logs, terminals and error messages. The quote is cut at 256 bytes; quotes and backslashes are escaped as in a string literal, a tab as `\t` and every other control character as a unicode escape. Moved here from `NatsConnection`. Copyright year.

### NatsUri.java

`src/main/java/io/nats/client/support/NatsUri.java`

`discoveredHostPort(String)`, the rule for a `connect_urls` entry: `host:port` with an IPv6 address in brackets, user info dropped, and a `URISyntaxException` with the reason for an entry that carries a scheme, does not parse as `host:port` or has a port outside 1 to 65535. `isIpAddress(String)` recognizes IPv4 and bracketed or bare IPv6 with a zone, and is what `hostIsIpAddress` and the `tlsServerName` check use. `tlsHostToInherit()` gives the name a discovered ip address inherits. `hostPortKey()` replaces the private `equivalentComparable()`. `toRedactedString()` and `redactUrl(String)` hide the credentials of a url, a password as `***` and a token as a whole, with the authority ending at the first of `/`, `?` and `#`.

### Connection.java

`src/main/java/io/nats/client/Connection.java`

`getConnectedUrlRedacted()`, a default method that returns the connected url with its credentials hidden. The javadocs of `getServers()` and `getConnectedUrl()` say the urls carry the credentials they were configured with and point at the redacted forms. Copyright year.

### ErrorListener.java

`src/main/java/io/nats/client/ErrorListener.java`

`discoveredServerSkipped(Connection, String entry, String reason)`, a default method called once per `connect_urls` entry the connection did not use: an entry with a scheme, one that does not parse as `host:port`, one with a port out of range, or under TLS a discovered ip address with no name to verify against. The javadoc says the entry is untrusted text to escape before it goes to a log, and that the method is not called when the options ignore discovered servers.

### Options.java

`src/main/java/io/nats/client/Options.java`

`DEFAULT_SSL_PROTOCOL` is `TLS`, so the contexts the client builds offer the highest version the JVM has. Three options with properties and getters: `tlsServerName` (`io.nats.client.tls.server.name`), validated at `build()` as a hostname or an ip address; `tlsVerifyDiscoveredByIp()` (`io.nats.client.tls.verify.discovered.by.ip`); `ignoreAuthErrorAbort()` (`io.nats.client.ignore.auth.error.abort`). `tlsFirst`, `tlsServerName` and `tlsVerifyDiscoveredByIp` select the default context when nothing else has, and the `ws://` rejection message lists them. `reconnectJitter` and `reconnectJitterTls` take zero as no jitter and refuse a negative value. `servers(...)` reports a url that does not parse with its credentials redacted, in the message and in the cause. Javadocs: `executor` runs the data port's connect and the name lookup of every attempt, so a fixed pool needs room for them; `connectionTimeout` is one budget per attempt that the addresses of a name share; the `ResolveToAll` modes say the same; `getServers()` notes the credentials.

### ServerPool.java

`src/main/java/io/nats/client/ServerPool.java`

Javadocs. `acceptDiscoveredUrls` receives `host:port` entries only, after the connection has dropped user info and skipped and reported the entries it does not use, and receives the list whether or not the options ignore discovered servers, so a custom pool honors that option itself. The origin overload says a discovered server is dialed with the transport and the TLS of the origin and inherits the hostname the origin was configured with or inherited, through `tlsHostToInherit()` and `withTlsHost(String)`. `resolveHostToIps(String, boolean, boolean)` is called on a thread of the connection's executor under the connection timeout, and a call the connection gave up on may still be running when `nextServer` or `connectFailed` is called.

## Test Source

### AbandonedConnectDataPort.java

`src/test/java/io/nats/client/impl/AbandonedConnectDataPort.java`

New. A write-timeout data port whose first connect waits for the test to let it proceed, through the attempt's deadline and the interrupt of the abandoned attempt, then connects and records what came of it; later connects go straight through.

### ConnectionListenerTests.java

`src/test/java/io/nats/client/impl/ConnectionListenerTests.java`

The DISCOVERED_SERVERS test feeds a `host:port` entry, and gains two cases: an entry with a scheme is skipped and reported through `discoveredServerSkipped` with its reason, and a connection that ignores discovered servers uses no entry and reports nothing. Copyright year.

### ErrorListenerTests.java

`src/test/java/io/nats/client/impl/ErrorListenerTests.java`

The default-method coverage calls `discoveredServerSkipped`. Copyright year.

### InfoHandlerTests.java

`src/test/java/io/nats/client/impl/InfoHandlerTests.java`

A `-ERR` after which the server closes fails the attempt at once. `testInfoThatDoesNotParseIsSkippedOnAnEstablishedConnection`, including that the same INFO fails an attempt. `testHeaderBlockThatDoesNotParseFromAPublisherThroughARealServerDropsOnlyThatMessage`, measured against nats-server relaying a header block as the publisher wrote it, and `testHeaderBlockThatDoesNotParseDropsOnlyThatMessage` against the mock.

### ListenerForTesting.java

`src/test/java/io/nats/client/impl/ListenerForTesting.java`

Records `discoveredServerSkipped` calls as `DiscoveredServerSkippedEvent` entries, read through `getDiscoveredServerSkippedEvents()`.

### ParseTests.java

`src/test/java/io/nats/client/impl/ParseTests.java`

`parseLength` of ten digits beyond an int is a `NumberFormatException`. `testLengthBeyondWhatAServerCouldSendIsAParseError`. `testProtocolLineTooLong` is replaced by `testInfoThatDoesNotParseIsSkippedByTheReader`. Copyright year.

### ReconnectTests.java

`src/test/java/io/nats/client/impl/ReconnectTests.java`

`testUserInfoInAConnectUrlsEntryIsNotUsedAsCredentials`, `testReconnectWithZeroJitter` and `testReconnectWaitHoldsWhenTheRoundsFirstServerLeavesThePool`, the last with a pool that drops the round's first server.

### ServerPoolTests.java

`src/test/java/io/nats/client/impl/ServerPoolTests.java`

Discovered entries are `host:port` constants. `testServerAddedStartsAtTheRaisedCount`, `testConfiguredServerAtItsLimitStaysOut`, `testDiscoveredServersTakeTheSchemeOfTheOrigin`, `testDiscoveredEntriesAreHostAndPortOnly` and `testServerThatSentTheListIsKept`; the existing tests follow the entries without a scheme and the per-server counts.

### TLSConnectTests.java

`src/test/java/io/nats/client/impl/TLSConnectTests.java`

`testTlsProtocolsAtOrAbove12`; `ProtocolRecordingDataPort` and `testBuiltContextNegotiatesTls13AndEnablesNothingBelow12`; `UnconfirmedHandshakeDataPort` and `testAnUnconfirmedHandshakeFailsTheAttemptBeforeConnect`, which measures that nothing is written in plaintext. `testDiscoveredPlainWebsocketServerIsRejectedOnce` is replaced by `testConnectUrlsEntriesWithASchemeAreSkippedAndNotDialed`.

### TlsDiscoveryScenarioTests.java

`src/test/java/io/nats/client/impl/TlsDiscoveryScenarioTests.java`

New. The end-to-end scenarios of the TLS discovery and reconnect standard: S02, S06, S16, S17 and S18 against two real nats-servers routed to each other, built by the `Cluster` helper from configuration inserts with `client_advertise`; S21, S25 and S26 against `SniTestServer`; and a server advertised under a rooted name, `localhost.`, dialed as written and verified and sent as SNI without the dot. `RecordingDataPort` records the protocol and the server name of every TLS session.

### TlsHostnameTests.java

`src/test/java/io/nats/client/impl/TlsHostnameTests.java`

The inheritance tests assert `tlsHostToInherit` and the name travelling over any number of hops. New: `discoveryInheritsNoNameWhenDiscoveredServersAreVerifiedByIp`, `discoveredIpRetainsHostnameOverTwoHops`, `discoveredIpWithNoNameToInheritIsSkippedUnlessAllowed`, `tlsServerNameIsTheNameOfEveryServer`, `tlsServerNameOverridesTheUrlHostAndTheInheritedName`, `tlsServerNameThatIsAnIpAddressIsVerifiedAsOneAndSendsNoSni`, `trailingDotIsStrippedFromTheNameVerifiedAgainstAndSentAsSni`, `nameCheckErrorStatesTheExpectedNameItsSourceAndTheRemedies` and `nameCheckFailureWrapsOnlyTheJdkNameCheck`.

### WebsocketConnectTests.java

`src/test/java/io/nats/client/impl/WebsocketConnectTests.java`

`testWebSocketUpgradeIsBoundedByTheConnectionTimeout`. Copyright year.

### SniTestServer.java

`src/test/java/io/nats/client/support/ssl/SniTestServer.java`

`Certificates(host, withLoopbackIpSan, issuedBy)` keeps the CA key so a chain can be issued by another CA; `writePem()` writes the certificate and key as PEM files, `PemFiles`, for a nats-server `tls` block. The server counts the connections it accepted, can inject plaintext before or after the INFO line, and records in `clientPlaintext` what the client sent in plaintext after INFO.

### AuthTests.java

`src/test/java/io/nats/client/AuthTests.java`

`testCredentialsAreRedactedInEventsAndErrors` and `testAuthErrorAbortCanBeSwitchedOff`. Copyright year.

### ConnectTests.java

`src/test/java/io/nats/client/ConnectTests.java`

`testAddressesOfOneNameShareOneAttemptBudget`, `testNameLookupIsBoundedByTheConnectionTimeout`, `testAHungLookupIsNotRepeatedEveryRound`, `testLookupFailureIsAFailedAttemptNotATlsMismatch` through the connect pass and the reconnect rounds with `HangingLookupPool`, `testLookupThatCannotBeStartedIsAFailedAttempt`, and `testAnAttemptAbandonedAtItsDeadlineLeavesNoOpenSocketAndNoWriteWatchTask` under `ResolveToAll` and `HappyEyeballs`. Copyright year.

### OptionsTests.java

`src/test/java/io/nats/client/OptionsTests.java`

`testNatsUriDiscoveredHostPort`, `testTlsServerName`, `testBuiltContextsOfferTheHighestTlsVersion`, `testNatsUriRedacted`, `testNatsUriIsIpAddress`, `testIgnoreAuthErrorAbort`, `testReconnectJitterZeroMeansNoJitter` and `testTlsVerifyDiscoveredByIp`.

### cluster_plain.conf

`src/test/resources/cluster_plain.conf`

New. A server configuration with nothing but a port, for tests that add the rest, a cluster block and a tls block, as configuration inserts.
