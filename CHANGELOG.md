
# Change Log

## Version 2.6.2 & 2.6.3

* [FIXED] - problem with jars being built with jdk 11
* [ADDED] - Automated deploy

## Version 2.6.1

* [FIXED] - #263 - Added server URLs to connect exception (not to auth exception)
* [FIXED] - #262 - Added @deprecated as needed
* [FIXED] - #261 - Added a static credentials implementation that uses char arrays
* [FIXED] - #260 - Moved to nats-server from gnatsd for testing
* [FIXED/CHANGED] - #259 - Double authentication errors from a server during reconnect attempts will result in the connection being closed.
* [FIXED] - #257 - Added connection method to messages that come from subscriptions, dispatchers and requests
* [FIXED] - #243 - Added check for whitespace in subjects and queue names
* [FIXED] - Improved a couple flaky tests

## Version 2.6.0

* [FIXED] - cleaned up use of "chain" instead of "creds"
* [FIXED] - #255 - added special ioexception when possible to indicate an authentication problem on connect
* [FIXED] - #252 - deprecated strings for username/pass/token and use char arrays instead, required changing some other code to CharBuffer
* [ADDED] - Openjdk11 to travis build and updated gradle wrapper to 5.5
* [ADDED] - an option to trace connect timing, including a test and example
* [FIXED/ADDED] - #197 - the ability to use a single dispatcher for multiple message handlers, including multiple subscriptions on a single subject

## Version 2.5.2

* [FIXED] - #244 - fixed an issue with parsing ipv6 addresses in the info JSON, added unit test for parser
* [FIXED] - #245 - fixed a timing bug in nats bench, now subscribers start timing at the first receive
* [FIXED/CHANGED] - #246 - fixed a confusing output from nats bench in CSV mode, now the test count and the total count are printed
* [ADDED] - spring cache to git ignore file
* [ADDED] - support for running nats bench with conscrypt

## Version 2.5.1

* [FIXED] - #239 - cleaned up extra code after SSL connect failure
* [FIXED] - #240 - removed stack trace that was left from debugging
* [FIXED] - #241 - changed method used to create an ssl socket to allow support for conscrypt/wildfly native libraries
* [ADDED] - conscrypt flag to natsautobench to allow testing with native library, requires Jar in class path

## Version 2.5.0

* [CHANGED] added back pressure to message queue on publish, this may effect behavior of multi-threaded publishers so moving minor version

## Version 2.4.5 & 2.4.6

* Clean up for rename to nats.java

## Version 2.4.4

* [FIXED] - #230 - removed extra executor allocation
* [FIXED] - #231 - found a problem with message ordering when filtering pings on reconnect, caused issues with reconnect in general
* [FIXED] - #226 - added more doc about ping intervals and max ping
* [FIXED] - #224 - resolved a latency problem with windows due to the cost of the message queues spinwait/lock
* [CHANGED] - started support for renaming gnatsd to nats-server, full release isn't done so using gnatsd for tests still

## Version 2.4.3

* [FIXED] - #223 - made SID public in the message
* [FIXED] - #227 - changed default thread to be non-daemon and normal priority, fixes shutdown issues
* [FIXED] - minor issue in javadoc that showed up when building on windows
* [ADDED] - test for fast pings and disconnect, duration.zero on nextMsg
* [CHANGED] - accepted pull request to replace explicit thread creation with executor

## Version 2.4.2

* [FIXED] - #217 - added check to "ignore" exceptions from reader during drain
* [CHANGED] - no longer call exception handler for "ignored" communication exceptions during close/drain after close occurs
* [ADDED] - #209 - support for comma separated urls in connect() or server()
* [FIXED] - #206 - incorrect error message when reconnect buffer is overrun
* [CHANGED] - #214 - moved to an executor instead of 1-off threads
* [FIXED] - #220 - an icky timing issue that could delay messages
* [CHANGED] - added larger TCP defaults to improve network performance
* [ADDED] - public method to create an inbox subject using the prefix from options
* [FIXED] - #203 & #204 - Fixed a thread/timing issue with writer and cleaning pong queues

## Version 2.4.1

* [FIXED] - #199 - turns out we had to hard code the manifest to remove the private package

## Version 2.4.0

* [ADDED] - support for JWT-based authentication and NGS
* [FIXED] - issue with norandomize server connect order, it was broken at some point
* [FIXED] #199 - import of a private package
* [FIXED] #195 - issue with "discovered" servers not having a protocol but we tried to parse as uri
* [FIXED] #186, #191 - sneaky issue with connection reader not reseting state on reconnect
* [ADDED] #192 - option to set inbox prefix, default remains the same
* [FIXED] #196 - updated all the versions i could find, and added note in gradle file about them

## Version 2.3.0

* [BREAKING CHANGE] Replaced use of strings for seeds and public keys in NKey code to use char[] to allow better memory security.

## Version 2.2.0

* [ADDED] Support for NKeys, and nonce based auth

## Version 2.1.2

* [FIXED] #181 - issue with default pending limits on consumers not matching doc
* [FIXED] #179 - added version variable for jars in build.gradle to make it easier to change
* [ADDED] Support for UTF8 subjects

## Version 2.1.1

* [FIXED] Issue with version in Nats.java, also updated deploying.md with checklist
* [FIXED] Fixed issue during reconnect where buffered messages blocked protocol messages

## Version 2.1.0

* [ADDED] Support for consumer or connection drain. (New API lead to version bump.)
* [FIXED] Fixed an issue with null pointer when ping/pong and reconnect interacted poorly.

## Version 2.0.2

* [FIXED] In a cluster situation the library wasn't using each server's auth info if it was in the URI.

## Version 2.0.1

* [CHANGED] Request now returns a CompletableFuture to allow more application async options
* [ADDED] Added back OSGI manifest information
* [ADDED] getLastError method to connection
* [ADDED/CHANGED] Implemented noEcho tests, and require the server to support noEcho if it is set

## Version 2.0.0

* [BREAKING CHANGE] Moved build to gradle
* [BREAKING CHANGE] Ground up rewrite to simplify and address API issues, API has changed
* [BREAKING CHANGE] Simplified connection API
* [CHANGED] Removed external dependencies

## Version 1.1-SNAPSHOT

_2017-02-09_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/1.0...HEAD)

* [FIXED] Flush wait interval (the amount of time the flusher waits before checking the flush queue) is once again set at 1ms.
* [FIXED] Do not shuffle entire pool when adding URL from INFO
* [ADDED] Connection name can now be accessed using `Connection#getName()`
* [CHANGED] CI tests now run against both Oracle JDK 8 and OpenJDK 8

## Version 1.0

_2017-02-02_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/0.7.3...1.0)

* [ADDED] `Nats.connect()` and variants have been added as a preferred method for connecting to NATS. `ConnectionFactory.createConnection()` will also continue to be supported.
* [ADDED] `Connection#getServers()` and `Connection#getDiscoveredServers()` APIs to match Go client
* [ADDED] `isTlsRequired()` and `isAuthRequired()` to match Go client capabilities.
* [CHANGED] Methods that previously threw `TimeoutException` now simply return `null` (for non-`void` methods) or throw `IOException` if their timeout elapses before they complete.
  * `ConnectionFactory#createConnection()`
  * `ConnectionImpl#flush(int timeout)`
  * `SyncSubscription#nextMessage(int timeout)` - returns `null` if the timeout elapses before a message is available.
* [CHANGED] Several constant definitions have been moved to the `Nats` class.

## Version 0.7.3

_2016-11-01_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/0.7.1...0.7.3)

* [FIXED #83](/../../issues/#83) All thread pool executors are now properly shutdown by `Connection#close()`.

## Version 0.7.1

_2016-10-30_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/0.7.0...0.7.1)

* [NEW API] `Connection#publish(String subject, String reply, byte[] data, boolean flush)` allows the caller to specify whether a flush of the connection's OutputStream should be forced. The default behavior for the other variants of publish is `false`. This was added to optimize performance for request-reply (used heavily in `java-nats-streaming`). The internal flush strategy of `ConnectionImpl` minimizes flush frequency by using a synchronized flusher thread to 'occasionally' flush. This benefits asynchronous publishing performance, but penalizes request-reply scenarios where a single message is published and then we wait for a reply.
* `NatsBench` can now be configured via properties file (see `src/test/resources/natsbench.properties`)

## Version 0.7.0

_2016-10-19_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/0.6.0...0.7.0)

* [BREAKING CHANGE] `SyncSubscription#nextMessage()` and its variants now throw `InterruptedException` if the underlying `poll`/`take` operation is interrupted.
* Fixed interrupt handling.
* Removed `Channel` implementation in favor of directly using `BlockingQueue`.

## Version 0.6.0

_2016-10-11_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/0.5.3...0.6.0)

* Implemented asynchronous handling of `INFO` messages, allowing the client to process `INFO` messages that may be received from the server after a connection is already established. These asynchronous `INFO` messages may update the client's list of servers in the connected cluster.
* Added proper JSON parsing via [google/gson](https://github.com/google/gson).
* Cleaned up some threading oddities in `ConnectionImpl`
* Moved async subscription threading mechanics into the `Connection`, similar to the Go client.
* Fixed a number of inconsistencies in how subscription pending limits were handled.
* Removed subscription pending limits from `ConnectionFactory`. These should be set on the Subscription using `Subscription#setPendingLimits()`.
* `ConnectionImpl` is now `public`, to avoid some issues with reflection in Java and reflective languages such as Clojure. Described further in [#35](/../../pull/35) (special thanks to [@mccraigmccraig](https://github.com/mccraigmccraig)).
* [#58](/../../issues/#58) Updated `NUID` implementation to match [the Go version](nats-io/nuid)
* [#48](/../../issues/#48) Fixed an NPE issue in `TCPConnectionMock` when calling `bounce()` (affects tests only).
* [#26](/../../issues/#26) Fixed a problem with `AsyncSubscription` feeder thread not exiting correctly in all cases.
* Updated integration tests to more closely reflect similar Go tests.
* Miscellaneous typo, style and other minor fixes.

## Version 0.5.3

_2016-08-29_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.5.2...jnats-0.5.3)

* Moved `nats_checkstyle.xml` out of `src` tree to avoid jar/bundle filtering

## Version 0.5.2

_2016-08-29_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.5.1...jnats-0.5.2)

* Depends on stable nats-parent-1.1.pom
* Excludes nats_checkstyle.xml from jar/bundle
* Downloads `gnatsd` binary for current arch/os to `target/` for test phase
* Housekeeping changes to travis-ci configuration

## Version 0.5.1

_2016-08-21_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.5.0...jnats-0.5.1)

* Fixed a problem with gnatsd 0.9.2 `connect_urls` breaking the client connect sequence. This field is now ignored.
* Retooled the way that releases are shipped from Travis CI, using the `deploy:` clause and new scripts

## Version 0.3.2

_2016-08-20_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.3.1...jnats-0.3.2)

* Fixed a problem parsing Long from String on Android.

## Version 0.5.0

_2016-08-10_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.4.1...jnats-0.5.0)

* Reverted to Java 1.7 compatibility to avoid Android incompatibility
* Fixed an issue that was preventing TLS connections from reconnecting automatically.
* Fixed an issue with asynchronous subscriptions not terminating their feeder thread.
* Exposed pending limits APIs
* Updated examples to match Go client and added benchmark program
* Integrated [NATS parent POM](https://github.com/nats-io/nats-parent-pom)
* Integrated check style
* Integrated maven-bundle-plugin to provide OSGI compliant java-nats bundle
* Miscellaneous minor bug fixes and javadoc updates

## Version 0.4.1

_2016-04-03  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.4.0...jnats-0.4.1)

* Removed a stray log trace statement in publish that was affecting performance.

## Version 0.4.0

_2016-03-29_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.3.1...jnats-0.4.0)

* Built on JDK 8
* Added NUID (a java implementation of [http://github.com/nats-io/nuid](http://github.com/nats-io/nuid)), an entropy-friendly UUID generator that operates ~40 ns per op
* Connection#newInbox() now uses NUID to generate the unique portion of the inbox name
* Added support for pending byte/msg limits for subscriptions:
* Subscription#setPendingLimits(int msgs, int bytes)
* Made the size of the Connection reconnect (pending) buffer configurable with ConnectionFactory setters and getters
* Optimized parser performance
* Optimized parser handling of large message payloads
* ConnectionFactory will now construct a default URL by combining supplied host, port, user, and password if no URL is directly supplied.
* Fixed a couple of issues with misnamed properties
* Miscellaneous doc corrections/updates

## Version 0.3.1

_2016-01-18_ _Initial public release of java-nats, now available on Maven Central._

* Added support for TLS v1.2
* Numerous performance improvements
* The DisconnectedEventHandler, ReconnectedEventHandler and ClosedEventHandler classes from the Alpha release have been renamed to DisconnectedCallback, ReconnectedCallback and ClosedCallback, respectively.
* Travis CI integration
* Coveralls.io support
* Increased test coverage
