# NATS - Java Client
A [Java](http://java.com) client for the [NATS messaging system](https://nats.io).

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fnats-io%2Fnats-java.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fnats-io%2Fnats-java?ref=badge_shield)
[![Build Status](https://travis-ci.org/nats-io/nats-java.svg?branch=master)](http://travis-ci.org/nats-io/nats-java)
[![Coverage Status](https://coveralls.io/repos/nats-io/nats-java/badge.svg?branch=master&service=github)](https://coveralls.io/github/nats-io/nats-java?branch=master)
[![Javadoc](http://javadoc.io/badge/io.nats/jnats.svg)](http://javadoc.io/doc/io.nats/jnats)


## Installation

```bash
# Java client
tbd

# Server
go get github.com/nats-io/gnatsd
```

## Basic Usage

### Connecting

### Publishing

### Subscribing Synchronously

### Subscribing Asynchronously With Dispatchers

## Advanced Usage

### TLS

-Djavax.net.ssl.keyStore=src/test/resources/keystore.jks
-Djavax.net.ssl.keyStorePassword=password
-Djavax.net.ssl.trustStore=src/test/resources/cacerts
-Djavax.net.ssl.trustStorePassword=password

java -cp build/libs/nats-java-2.0.0.jar:build/libs/java-nats-examples-2.0.0.jar io.nats.examples.NatsSub opentls://localhost:4443 test 3


java -cp build/libs/nats-java-2.0.0.jar:build/libs/java-nats-examples-2.0.0.jar -Djavax.net.ssl.keyStore=src/test/resources/keystore.jks -Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStore=src/test/resources/cacerts -Djavax.net.ssl.trustStorePassword=password io.nats.examples.NatsPub tls://localhost:4443 test "hello world"

gnatsd --conf src/test/resources/tls.conf

### Connection options

### Clusters & Reconnecting


## Benchmarking


## License

Unless otherwise noted, the NATS source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fnats-io%2Fnats-java.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fnats-io%2Fnats-java?ref=badge_large)