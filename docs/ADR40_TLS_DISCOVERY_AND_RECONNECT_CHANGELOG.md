# ADR-40 TLS discovery and reconnect: the changes of pull request nats-io/nats.java#1668

The pull request delivers the client's conformance to ADR-40, revision 5, reviewed as ten pull requests in `synadia-labs/nats.java-internal`. This document carries the pull request description as submitted and a summary of every file the pull request changes, in the order of GitHub's file tree.

## Pull request description

TLS discovery and reconnect: the client's conformance to ADR-40, revision 5, reviewed PR by PR in `synadia-labs/nats.java-internal` and delivered here as one change.

## What changes

How the client treats the servers a cluster advertises, how it verifies them over TLS, how it reconnects, and what it writes in events and errors. The behavior before and after each change, with the option that restores the old behavior where there is one, is in `RELEASE_NOTES_ADR40.md`, referenced from the README under Important and under Recent Version Notes.

## The reviewed pull requests

Each was reviewed on its own layer in the internal repository; the review fixes are commits on the same branches. In stack order, bottom first:

1. [#1 Discovered servers: host:port entries, reporting server's transport, skipped entries reported](https://github.com/synadia-labs/nats.java-internal/pull/1)
2. [#2 TLS server name option, trailing dot, name-check error](https://github.com/synadia-labs/nats.java-internal/pull/2)
3. [#3 TLS name of a discovered server over hops; verify discovered servers by ip](https://github.com/synadia-labs/nats.java-internal/pull/3)
4. [#4 Reconnect: round wait, zero jitter, per-server counts, origin kept](https://github.com/synadia-labs/nats.java-internal/pull/4)
5. [#5 Reader robustness and attempt deadlines](https://github.com/synadia-labs/nats.java-internal/pull/5)
6. [#6 Credentials redacted in events, errors and traces](https://github.com/synadia-labs/nats.java-internal/pull/6)
7. [#7 TLS 1.3 offered, TLS 1.2 floor on every connection](https://github.com/synadia-labs/nats.java-internal/pull/7)
8. [#8 Opt out of the authorization abort](https://github.com/synadia-labs/nats.java-internal/pull/8)
9. [#9 Scenario tests](https://github.com/synadia-labs/nats.java-internal/pull/9)
10. [#10 ADR-40 refactoring](https://github.com/synadia-labs/nats.java-internal/pull/10)

A review of the whole stack followed; its fixes are on the branches of the pull requests that introduced the code and merged up through the stack: a failed or timed-out name lookup is a failed attempt and never a TLS mismatch, one lookup in flight per server, a data port closed during its connect stays closed, an unconfirmed TLS handshake fails the attempt before CONNECT, url redaction ends the authority at a query or fragment, and the javadocs of the connect threading and the shared connection timeout.

## Compatibility

The fifteen notes in `RELEASE_NOTES_ADR40.md`. The ones most likely to reach a user: a discovered server is a `host:port` entry dialed with the transport and TLS of the server that reported it, and a custom `ServerPool` receives `host:port` only; an ip-address seed with an ip-address cluster needs `tlsVerifyDiscoveredByIp()` or `tlsServerName`; `maxReconnects` holds during an outage in which the servers keep naming each other; the addresses of one hostname share one connection timeout per attempt; TLS 1.2 is the floor; events, errors and trace lines no longer carry url passwords; `ignoreAuthErrorAbort()` is new and off by default.

## Tests

`TlsDiscoveryScenarioTests` runs the scenarios of the standard end to end against two real nats-servers and against the fake TLS server, nine in all. The other tests sit with the code they cover: `TlsHostnameTests`, `TLSConnectTests`, `ConnectTests`, `ReconnectTests`, `ServerPoolTests`, `OptionsTests`, `InfoHandlerTests`, `AuthTests`, `WebsocketConnectTests`.

## Files

One section per file, 35 files, in the order of GitHub's file tree: directories before files at each level, each sorted by name. The first line of a section is the file's path from the project root.

## ADR40_TLS_DISCOVERY_AND_RECONNECT_CHANGELOG.md

`docs/ADR40_TLS_DISCOVERY_AND_RECONNECT_CHANGELOG.md`

This document: a copy of the pull request description and a summary of every file the pull request changes, in the order of GitHub's file tree.

## TLS_CONFIGURATION.md

`docs/TLS_CONFIGURATION.md`

Updated for the new rules. A discovered server takes the scheme and the TLS of the server that reported it. `tlsFirst()` and `tlsServerName()` select the default context when nothing else has. The contexts the client builds are created for `TLS` and offer TLS 1.3, and every connection enables TLS 1.2 and above only. `tlsServerName` is described as the first remedy when certificates do not carry the name in the url. The name-check failure is an `SSLHandshakeException` that states the expected name and where it came from, with the JDK's `CertificateException` as its cause. A new section 10 describes the form of a `connect_urls` entry: `host:port` only, user info dropped, entries with a scheme skipped and reported, and what a custom `ServerPool` receives.

## ErrorListenerConsoleImpl.java

`src/main/java/io/nats/client/impl/ErrorListenerConsoleImpl.java`

Implements `discoveredServerSkipped`, printing the entry quoted as untrusted text and the reason.

## ErrorListenerLoggerImpl.java

`src/main/java/io/nats/client/impl/ErrorListenerLoggerImpl.java`

Implements `discoveredServerSkipped`, logging the entry quoted as untrusted text and the reason.

## NatsConnection.java

`src/main/java/io/nats/client/impl/NatsConnection.java`

The connect pass and the reconnect round end when any server handed out comes back, not only the first one, so the wait between rounds holds when the round's first server left the pool. Each attempt records its failure in `connectFailure`, the cause of the connect's `IOException`, and its TLS mismatch; the connection closes without retrying only when every server of a pass or a round failed with the mismatch. A second identical authorization error from one server ends the reconnect unless `ignoreAuthErrorAbort` is set. The data port's connect runs as a task on the executor under the attempt's remaining time, with the port in `attemptDataPort` force-closed at the deadline or on close. The name lookup runs in `resolveHostBounded` on the executor under a timer of its own, with one lookup in flight per server across rounds; a lookup that fails, runs out of time or cannot be started is a failed attempt of that server and never a TLS mismatch. The addresses of one name share one attempt budget. `throwIfAttemptFailed` ends the attempt before CONNECT and after the PING when another thread recorded a failure. A `-ERR` after which the server closes fails the attempt at once with the server's error as the failure. With TLS after INFO, the plaintext INFO's `connect_urls` and lame-duck flag are not used, discovered servers come from the INFOs inside TLS, and against nats-server 2.12 and later the client waits for the INFO that follows the first PONG before reporting connected. An INFO that does not parse on an established connection is skipped and reported. `connect_urls` entries are normalized to `host:port` with user info dropped; an entry with a scheme, one that does not parse or one with a port out of range is skipped and reported through `discoveredServerSkipped`, as is, under TLS with the name check on, a discovered ip address with no name to verify against unless `tlsVerifyDiscoveredByIp` is set. The DISCOVERED_SERVERS event carries the normalized list. Events, errors, trace lines and the `serverRejected` string name servers with their credentials redacted, through `redacted(...)` and `getConnectedUrlRedacted()`. The quoting of untrusted text moved to `Encoding`; the no-argument `readInitialInfo()` and `quoteUntrusted(String)` are removed.

## NatsConnectionReader.java

`src/main/java/io/nats/client/impl/NatsConnectionReader.java`

A `RuntimeException` that escapes the parsing is reported as a communication issue and makes the client reconnect, instead of ending the reader's thread without a word. A header block that does not parse drops only the message that carries it, reported through `exceptionOccurred`, with `discardIncoming` skipping that message's data. `parseLength` refuses a value above `Integer.MAX_VALUE` instead of wrapping. `checkIncomingLength` treats a length above twice the server's max payload, and never below 64 MiB, as a line that does not parse, and an HMSG whose header length exceeds its total length is a parse error. An INFO that does not parse is skipped on an established connection and fails the attempt during one.

## NatsServerPool.java

`src/main/java/io/nats/client/impl/NatsServerPool.java`

Discovered entries are parsed through `NatsUri.discoveredHostPort` and given the scheme of the origin, or the pool's default scheme without one; an entry the rule refuses never becomes a server. The origin, the server whose INFO it is, is kept in the list like the configured servers and the last connected one. A discovered ip address inherits the name the origin was configured with or inherited itself, so the name travels over any number of hops, and inherits nothing under `tlsVerifyDiscoveredByIp`. Reconnect counts are per server: `keptCounts`, bounded to 1024 entries, keeps the count of a server that left the list at its limit; a server that enters the list starts at the higher of its kept count and the highest count in the list, the origin excluded, and does not enter at the limit; `connectSucceeded` resets the count of that server only; `connectFailed` removes a server at its limit and keeps its count. Entries are keyed by `hostPortKey()`.

## SocketDataPort.java

`src/main/java/io/nats/client/impl/SocketDataPort.java`

The TLS host and where it came from, `TLS_HOST_FROM_URL`, `TLS_HOST_FROM_OPTIONS` or `TLS_HOST_FROM_REPORTING_SERVER`, which the name-check error states. `tlsServerName` is the name of every server. One trailing dot is stripped from the name verified against and sent as SNI; a name that is an ip address is verified as one and no SNI is sent. `socket` is volatile and a `closed` flag set by `close()` and `forceClose()` makes a connect that completes afterwards close its socket and fail. `tlsProtocolsAtOrAbove12` keeps only `TLSv1.N` with N at least 2 and is applied to every upgrade. `awaitHandshake` throws when the handshake-completed notification does not arrive within the connection timeout, where the upgrade used to report and return with the plaintext streams in place. `nameCheckFailure` wraps the JDK's name-check failure, recognized by `isNameCheckMessage`, in an `SSLHandshakeException` that states the expected name, its source and the remedies.

## SocketDataPortWithWriteTimeout.java

`src/main/java/io/nats/client/impl/SocketDataPortWithWriteTimeout.java`

The write-watch task is volatile. It is not scheduled when the connect threw, and it is stopped when the port was closed between the connect and the scheduling. `close()` closes the socket first and stops the task after, so the two orderings cannot leave a task running. Copyright year.

## Encoding.java

`src/main/java/io/nats/client/support/Encoding.java`

`quoteFirstLine(byte[])` and `quote(String)`: the quoting of untrusted text from a server, the first line up to CR or LF or one token, for logs, terminals and error messages. The quote is cut at 256 bytes; quotes and backslashes are escaped as in a string literal, a tab as `\t` and every other control character as a unicode escape. Moved here from `NatsConnection`. Copyright year.

## NatsUri.java

`src/main/java/io/nats/client/support/NatsUri.java`

`discoveredHostPort(String)`, the rule for a `connect_urls` entry: `host:port` with an IPv6 address in brackets, user info dropped, and a `URISyntaxException` with the reason for an entry that carries a scheme, does not parse as `host:port` or has a port outside 1 to 65535. `isIpAddress(String)` recognizes IPv4 and bracketed or bare IPv6 with a zone, and is what `hostIsIpAddress` and the `tlsServerName` check use. `tlsHostToInherit()` gives the name a discovered ip address inherits. `hostPortKey()` replaces the private `equivalentComparable()`. `toRedactedString()` and `redactUrl(String)` hide the credentials of a url, a password as `***` and a token as a whole, with the authority ending at the first of `/`, `?` and `#`.

## Connection.java

`src/main/java/io/nats/client/Connection.java`

`getConnectedUrlRedacted()`, a default method that returns the connected url with its credentials hidden. The javadocs of `getServers()` and `getConnectedUrl()` say the urls carry the credentials they were configured with and point at the redacted forms. Copyright year.

## ErrorListener.java

`src/main/java/io/nats/client/ErrorListener.java`

`discoveredServerSkipped(Connection, String entry, String reason)`, a default method called once per `connect_urls` entry the connection did not use: an entry with a scheme, one that does not parse as `host:port`, one with a port out of range, or under TLS a discovered ip address with no name to verify against. The javadoc says the entry is untrusted text to escape before it goes to a log, and that the method is not called when the options ignore discovered servers.

## Options.java

`src/main/java/io/nats/client/Options.java`

`DEFAULT_SSL_PROTOCOL` is `TLS`, so the contexts the client builds offer the highest version the JVM has. Three options with properties and getters: `tlsServerName` (`io.nats.client.tls.server.name`), validated at `build()` as a hostname or an ip address; `tlsVerifyDiscoveredByIp()` (`io.nats.client.tls.verify.discovered.by.ip`); `ignoreAuthErrorAbort()` (`io.nats.client.ignore.auth.error.abort`). `tlsFirst`, `tlsServerName` and `tlsVerifyDiscoveredByIp` select the default context when nothing else has, and the `ws://` rejection message lists them. `reconnectJitter` and `reconnectJitterTls` take zero as no jitter and refuse a negative value. `servers(...)` reports a url that does not parse with its credentials redacted, in the message and in the cause. Javadocs: `executor` runs the data port's connect and the name lookup of every attempt, so a fixed pool needs room for them; `connectionTimeout` is one budget per attempt that the addresses of a name share; the `ResolveToAll` modes say the same; `getServers()` notes the credentials.

## ServerPool.java

`src/main/java/io/nats/client/ServerPool.java`

Javadocs. `acceptDiscoveredUrls` receives `host:port` entries only, after the connection has dropped user info and skipped and reported the entries it does not use, and receives the list whether or not the options ignore discovered servers, so a custom pool honors that option itself. The origin overload says a discovered server is dialed with the transport and the TLS of the origin and inherits the hostname the origin was configured with or inherited, through `tlsHostToInherit()` and `withTlsHost(String)`. `resolveHostToIps(String, boolean, boolean)` is called on a thread of the connection's executor under the connection timeout, and a call the connection gave up on may still be running when `nextServer` or `connectFailed` is called.

## AbandonedConnectDataPort.java

`src/test/java/io/nats/client/impl/AbandonedConnectDataPort.java`

New. A write-timeout data port whose first connect waits for the test to let it proceed, through the attempt's deadline and the interrupt of the abandoned attempt, then connects and records what came of it; later connects go straight through.

## ConnectionListenerTests.java

`src/test/java/io/nats/client/impl/ConnectionListenerTests.java`

The DISCOVERED_SERVERS test feeds a `host:port` entry, and gains two cases: an entry with a scheme is skipped and reported through `discoveredServerSkipped` with its reason, and a connection that ignores discovered servers uses no entry and reports nothing. Copyright year.

## ErrorListenerTests.java

`src/test/java/io/nats/client/impl/ErrorListenerTests.java`

The default-method coverage calls `discoveredServerSkipped`. Copyright year.

## InfoHandlerTests.java

`src/test/java/io/nats/client/impl/InfoHandlerTests.java`

A `-ERR` after which the server closes fails the attempt at once. `testInfoThatDoesNotParseIsSkippedOnAnEstablishedConnection`, including that the same INFO fails an attempt. `testHeaderBlockThatDoesNotParseFromAPublisherThroughARealServerDropsOnlyThatMessage`, measured against nats-server relaying a header block as the publisher wrote it, and `testHeaderBlockThatDoesNotParseDropsOnlyThatMessage` against the mock.

## ListenerForTesting.java

`src/test/java/io/nats/client/impl/ListenerForTesting.java`

Records `discoveredServerSkipped` calls as `DiscoveredServerSkippedEvent` entries, read through `getDiscoveredServerSkippedEvents()`.

## ParseTests.java

`src/test/java/io/nats/client/impl/ParseTests.java`

`parseLength` of ten digits beyond an int is a `NumberFormatException`. `testLengthBeyondWhatAServerCouldSendIsAParseError`. `testProtocolLineTooLong` is replaced by `testInfoThatDoesNotParseIsSkippedByTheReader`. Copyright year.

## ReconnectTests.java

`src/test/java/io/nats/client/impl/ReconnectTests.java`

`testUserInfoInAConnectUrlsEntryIsNotUsedAsCredentials`, `testReconnectWithZeroJitter` and `testReconnectWaitHoldsWhenTheRoundsFirstServerLeavesThePool`, the last with a pool that drops the round's first server.

## ServerPoolTests.java

`src/test/java/io/nats/client/impl/ServerPoolTests.java`

Discovered entries are `host:port` constants. `testServerAddedStartsAtTheRaisedCount`, `testConfiguredServerAtItsLimitStaysOut`, `testDiscoveredServersTakeTheSchemeOfTheOrigin`, `testDiscoveredEntriesAreHostAndPortOnly` and `testServerThatSentTheListIsKept`; the existing tests follow the entries without a scheme and the per-server counts.

## TLSConnectTests.java

`src/test/java/io/nats/client/impl/TLSConnectTests.java`

`testTlsProtocolsAtOrAbove12`; `ProtocolRecordingDataPort` and `testBuiltContextNegotiatesTls13AndEnablesNothingBelow12`; `UnconfirmedHandshakeDataPort` and `testAnUnconfirmedHandshakeFailsTheAttemptBeforeConnect`, which measures that nothing is written in plaintext. `testDiscoveredPlainWebsocketServerIsRejectedOnce` is replaced by `testConnectUrlsEntriesWithASchemeAreSkippedAndNotDialed`.

## TlsDiscoveryScenarioTests.java

`src/test/java/io/nats/client/impl/TlsDiscoveryScenarioTests.java`

New. The end-to-end scenarios of the TLS discovery and reconnect standard: S02, S06, S16, S17 and S18 against two real nats-servers routed to each other, built by the `Cluster` helper from configuration inserts with `client_advertise`; S21, S25 and S26 against `SniTestServer`; and a server advertised under a rooted name, `localhost.`, dialed as written and verified and sent as SNI without the dot. `RecordingDataPort` records the protocol and the server name of every TLS session.

## TlsHostnameTests.java

`src/test/java/io/nats/client/impl/TlsHostnameTests.java`

The inheritance tests assert `tlsHostToInherit` and the name travelling over any number of hops. New: `discoveryInheritsNoNameWhenDiscoveredServersAreVerifiedByIp`, `discoveredIpRetainsHostnameOverTwoHops`, `discoveredIpWithNoNameToInheritIsSkippedUnlessAllowed`, `tlsServerNameIsTheNameOfEveryServer`, `tlsServerNameOverridesTheUrlHostAndTheInheritedName`, `tlsServerNameThatIsAnIpAddressIsVerifiedAsOneAndSendsNoSni`, `trailingDotIsStrippedFromTheNameVerifiedAgainstAndSentAsSni`, `nameCheckErrorStatesTheExpectedNameItsSourceAndTheRemedies` and `nameCheckFailureWrapsOnlyTheJdkNameCheck`.

## WebsocketConnectTests.java

`src/test/java/io/nats/client/impl/WebsocketConnectTests.java`

`testWebSocketUpgradeIsBoundedByTheConnectionTimeout`. Copyright year.

## SniTestServer.java

`src/test/java/io/nats/client/support/ssl/SniTestServer.java`

`Certificates(host, withLoopbackIpSan, issuedBy)` keeps the CA key so a chain can be issued by another CA; `writePem()` writes the certificate and key as PEM files, `PemFiles`, for a nats-server `tls` block. The server counts the connections it accepted, can inject plaintext before or after the INFO line, and records in `clientPlaintext` what the client sent in plaintext after INFO.

## AuthTests.java

`src/test/java/io/nats/client/AuthTests.java`

`testCredentialsAreRedactedInEventsAndErrors` and `testAuthErrorAbortCanBeSwitchedOff`. Copyright year.

## ConnectTests.java

`src/test/java/io/nats/client/ConnectTests.java`

`testAddressesOfOneNameShareOneAttemptBudget`, `testNameLookupIsBoundedByTheConnectionTimeout`, `testAHungLookupIsNotRepeatedEveryRound`, `testLookupFailureIsAFailedAttemptNotATlsMismatch` through the connect pass and the reconnect rounds with `HangingLookupPool`, `testLookupThatCannotBeStartedIsAFailedAttempt`, and `testAnAttemptAbandonedAtItsDeadlineLeavesNoOpenSocketAndNoWriteWatchTask` under `ResolveToAll` and `HappyEyeballs`. Copyright year.

## OptionsTests.java

`src/test/java/io/nats/client/OptionsTests.java`

`testNatsUriDiscoveredHostPort`, `testTlsServerName`, `testBuiltContextsOfferTheHighestTlsVersion`, `testNatsUriRedacted`, `testNatsUriIsIpAddress`, `testIgnoreAuthErrorAbort`, `testReconnectJitterZeroMeansNoJitter` and `testTlsVerifyDiscoveredByIp`.

## cluster_plain.conf

`src/test/resources/cluster_plain.conf`

New. A server configuration with nothing but a port, for tests that add the rest, a cluster block and a tls block, as configuration inserts.

## README.md

`README.md`

The snapshot badge and the Gradle, Maven and uber-jar coordinates say 2.27.0. The Important section opens with "2.27.0 Changes to secure connection behavior" and points at the two version-note sections and the release notes. The hostname verification note is now for 2.27.0 and names `tlsServerName` as the first remedy. A new section, Version 2.27.0 TLS Discovery and Reconnect, lists the seven changes most likely to reach a user. The property table gains `ignore.auth.error.abort`, `tls.server.name` and `tls.verify.discovered.by.ip`. The TLS text says the client negotiates TLS 1.3 where available and never below TLS 1.2, the SSLContext rules say `tlsServerName()` selects the default context, the `connect_urls` paragraph says an entry with a scheme is skipped and reported, and the Connection Security section describes `tlsServerName` and `tlsVerifyDiscoveredByIp`.

## RELEASE_NOTES_ADR40.md

`RELEASE_NOTES_ADR40.md`

New. Fifteen notes for the release, each with the behavior before, the behavior now and the option that restores the old behavior where there is one: discovered entries as `host:port`, the DISCOVERED_SERVERS event detail, the scheme of a discovered server, ip-address seeds, the inherited name over hops, the name-check exception, `maxReconnects` during an outage, jitter, the shared connection timeout of a name's addresses, faster failed attempts and bounded lookups, malformed input, url password redaction, the TLS 1.2 floor, `ignoreAuthErrorAbort()`, and the unconfirmed TLS handshake.

## build.gradle

`build.gradle`

`jarVersion` is 2.27.0, a minor release.
