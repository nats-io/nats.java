Change Log
==========

## Version 0.5.1
_2016-08-20_  [GitHub Diff](https://github.com/nats-io/jnats/compare/jnats-0.4.1...jnats-0.5.1)
 * Reverted to Java 1.7 compatibility to avoid Android incompatibility
* Fixed a problem with gnatsd 0.9.2 `connect_urls` breaking the client connect sequence. This field is now ignored.
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

## Version 0.4.0
_2016-03-29_  [GitHub Diff](https://github.com/nats-io/jnats/compare/jnats-0.3.1...jnats-0.4.0)


## Version 0.3.1
_2016-01-18  [GitHub Diff](https://github.com/nats-io/jnats/compare/v0.2.0-alpha...jnats-0.3.1)


## Version 0.2.0-alpha
_2015-11-19_ 
