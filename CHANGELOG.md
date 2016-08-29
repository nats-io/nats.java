Change Log
==========

## Version 0.5.3
_2016-08-29_  [GitHub Diff](https://github.com/nats-io/jnats/compare/jnats-0.5.2...jnats-0.5.3)
 * Moved `nats_checkstyle.xml` out of `src` tree to avoid jar/bundle filtering 

## Version 0.5.2
_2016-08-29_  [GitHub Diff](https://github.com/nats-io/jnats/compare/jnats-0.5.1...jnats-0.5.2)
 * Depends on stable nats-parent-1.1.pom
 * Excludes nats_checkstyle.xml from jar/bundle
 * Downloads `gnatsd` binary for current arch/os to `target/` for test phase
 * Housekeeping changes to travis-ci configuration

## Version 0.5.1
_2016-08-21_  [GitHub Diff](https://github.com/nats-io/jnats/compare/jnats-0.5.0...jnats-0.5.1)
 * Fixed a problem with gnatsd 0.9.2 `connect_urls` breaking the client connect sequence. This field is now ignored.
 * Retooled the way that releases are shipped from Travis CI, using the `deploy:` clause and new scripts

## Version 0.3.2
_2016-08-20_  [GitHub Diff](https://github.com/nats-io/jnats/compare/jnats-0.3.1...jnats-0.3.2)
 * Fixed a problem parsing Long from String on Android.

## Version 0.5.0
_2016-08-10_  [GitHub Diff](https://github.com/nats-io/jnats/compare/jnats-0.4.1...jnats-0.5.0)
 * Reverted to Java 1.7 compatibility to avoid Android incompatibility
 * Fixed an issue that was preventing TLS connections from reconnecting automatically.
 * Fixed an issue with asynchronous subscriptions not terminating their feeder thread.
 * Exposed pending limits APIs
 * Updated examples to match Go client and added banchmark program
 * Integrated [NATS parent POM](https://github.com/nats-io/nats-parent-pom)
 * Integrated Checkstyle
 * Integrated maven-bundle-plugin to provide OSGI compliant jnats bundle
 * Miscellaneous minor bug fixes and javadoc updates
## Version 0.4.1
_2016-04-03  [GitHub Diff](https://github.com/nats-io/jnats/compare/jnats-0.4.0...jnats-0.4.1)
 * Removed a stray log trace statement in publish that was affecting performance.

## Version 0.4.0
_2016-03-29_  [GitHub Diff](https://github.com/nats-io/jnats/compare/jnats-0.3.1...jnats-0.4.0)
 * Built on JDK 8
 * Added NUID (a java implementation of http://github.com/nats-io/nuid), an entropy-friendly UUID generator that operates ~40ns per op
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
_2016-01-18

_Initial public release of jnats, now available on Maven Central._

 * Added support for TLS v1.2
 * Numerous performance improvements
 * The DisconnectedEventHandler, ReconnectedEventHandler and ClosedEventHandler classes from the Alpha release have been renamed to DisconnectedCallback, ReconnectedCallback and ClosedCallback, respectively.
 * Travis CI integration
 * Coveralls.io support
 * Increased test coverage
