![](https://raw.githubusercontent.com/nats-io/nats-site/master/src/img/large-logo.png)
# NATS - Java client
A [Java](http://www.java.com) client for the [NATS messaging system](https://nats.io).

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![Build Status](https://travis-ci.org/nats-io/jnats.svg?branch=master)](http://travis-ci.org/nats-io/jnats)
[![Coverage Status](https://coveralls.io/repos/nats-io/jnats/badge.svg?branch=master&service=github)](https://coveralls.io/github/nats-io/jnats?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.nats/jnats/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.nats/jnats)
[![Javadoc](http://javadoc-badge.appspot.com/io.nats/jnats.svg?label=javadoc)](http://nats-io.github.io/jnats)


## Installation

### Maven Central

#### Releases

The current stable release is 0.5.0, available on Maven Central.
Add the following dependency to your project's `pom.xml`:

```xml
  <dependencies>
    ...
    <dependency>
      <groupId>io.nats</groupId>
      <artifactId>jnats</artifactId>
      <version>0.5.0</version>
    </dependency>
  </dependencies>
```
#### Snapshots

Snapshots are regularly uploaded to the Sonatype OSSRH (OSS Repository Hosting) using
the same Maven coordinates.
Add the following dependency to your project's `pom.xml`.

```xml
  <dependencies>
    ...
    <dependency>
      <groupId>io.nats</groupId>
      <artifactId>jnats</artifactId>
      <version>0.5.1-SNAPSHOT</version>
    </dependency>
  </dependencies>
```
If you don't already have your pom.xml configured for using Maven snapshots, you'll also need to add the following repository to your pom.xml.

```xml
<repositories>
    ...
    <repository>
        <id>sonatype-snapshots</id>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```
#### Building from source code (this repository)
First, download and install the parent POM:
```
git clone git@github.com:nats-io/nats-parent-pom.git
cd nats-parent-pom
mvn install
```

Second, download the source code:
```
git clone git@github.com:nats-io/jnats.git
```

To build the library, use [maven](https://maven.apache.org/). From the root directory of the project:

```
mvn package verify
```
The jar file will be built in the `target` directory. Then copy the jar file to your classpath and you're all set.

NOTE: running the unit tests requires that `gnatsd` be installed on your system and available in your executable search path.

## Basic Usage

```java

ConnectionFactory cf = new ConnectionFactory(Constants.DEFAULT_URL);
Connection nc = cf.createConnection();

// Simple Publisher
nc.publish("foo", "Hello World".getBytes());

// Simple Async Subscriber
nc.subscribe("foo", m -> {
    System.out.println("Received a message: %s\n", new String(m.getData()));
});

// Simple Sync Subscriber
Subscription sub = nc.subscribeSync("foo");
Message m = sub.nextMsg(timeout);

// Unsubscribing
sub = nc.subscribe("foo");
sub.unsubscribe();

// Requests
msg = nc.request("help", "help me", 10000);

// Replies
nc.subscribe("help", m -> {
    nc.publish(m.getReplyTo(), "I can help!");
});

// Close connection
ConnectionFactory cf = new ConnectionFactory("nats://localhost:4222");
nc = cf.createConnection();
nc.close();
```
## TLS

TLS/SSL connections may be configured through the use of an [SSLContext](https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html).
 
```java
	// Set up and load the keystore
	final KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
	final char[] keyPassPhrase = "password".toCharArray();
	final KeyStore ks = KeyStore.getInstance("JKS");
	ks.load(classLoader.getResourceAsStream("keystore.jks"), keyPassPhrase);
	kmf.init(ks, keyPassPhrase);

	// Set up and load the trust store
	final char[] trustPassPhrase = "password".toCharArray();
	final KeyStore tks = KeyStore.getInstance("JKS");
	tks.load(classLoader.getResourceAsStream("cacerts"), trustPassPhrase);
	final TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
	tmf.init(tks);

	// Get and initialize the SSLContext
	SSLContext c = SSLContext.getInstance(Constants.DEFAULT_SSL_PROTOCOL);
	c.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

	// Create a new NATS connection factory
	ConnectionFactory cf = new ConnectionFactory("nats://localhost:1222");
	cf.setSecure(true); 	// Set the secure option, indicating that TLS is required
	cf.setTlsDebug(true);   // Set TLS debug, which will produce additional console output
	cf.setSslContext(c);	// Set the context for this factory

	// Create a new SSL connection
	try (Connection connection = cf.createConnection()) {
		connection.publish("foo", "Hello".getBytes());
		connection.close();
	} catch (Exception e) {
		e.printStackTrace();
	}
```

## Wildcard Subscriptions

```java

// "*" matches any token, at any level of the subject.
nc.subscribe("foo.*.baz", m -> {
    System.out.printf("Msg received on [%s] : %s\n", m.getSubject(), new String(m.getData()));
});

nc.subscribe("foo.bar.*", m -> {
    System.out.printf("Msg received on [%s] : %s\n", m.getSubject(), new String(m.getData()));
});

// ">" matches any length of the tail of a subject, and can only be the last token
// E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
nc.subscribe("foo.>", m -> {
    System.out.printf("Msg received on [%s] : %s\n", m.getSubject(), new String(m.getData()));
});

// Matches all of the above
nc.publish("foo.bar.baz", "Hello World");

```

## Queue Groups

```java
// All subscriptions with the same queue name will form a queue group.
// Each message will be delivered to only one subscriber per queue group,
// using queuing semantics. You can have as many queue groups as you wish.
// Normal subscribers will continue to work as expected.

nc.subscribe("foo", "job_workers", m -> {
    received += 1;
});

```

## Advanced Usage

```java

// Flush connection to server, returns when all messages have been processed.
nc.flush()
System.out.println("All clear!");

// flush can also be called with a timeout value.
try {
    flushed = nc.flush(1000);
    System.out.println("All clear!");
} catch (TimeoutException e) {
    System.out.println("Flushed timed out!");
}

// Auto-unsubscribe after MAX_WANTED messages received
final static int MAX_WANTED = 10;
...
sub = nc.subscribe("foo");
sub.autoUnsubscribe(MAX_WANTED);

// Multiple connections
nc1 = cf.createConnection("nats://host1:4222");
nc2 = cf.createConnection("nats://host2:4222");

nc1.subscribe("foo", m -> {
    System.out.printf("Received a message: %s\n", new String(m.getData()));
});

nc2.publish("foo", "Hello World!");

```

## Clustered Usage

```java

String[] servers = new String[] {
	"nats://localhost:1222",
	"nats://localhost:1223",
	"nats://localhost:1224",
};

// Setup options to include all servers in the cluster
ConnectionFactory cf = new ConnectionFactory();
cf.setServers(servers);

// Optionally set ReconnectWait and MaxReconnect attempts.
// This example means 10 seconds total per backend.
cf.setMaxReconnect(5);
cf.setReconnectWait(2000);

// Optionally disable randomization of the server pool
cf.setNoRandomize(true);

Connection nc = cf.createConnection();

// Setup callbacks to be notified on disconnects and reconnects
nc.setDisconnectedCallback(event -> {
    System.out.printf("Got disconnected from %s!\n", event.getConnection().getConnectedUrl());
});

// See who we are connected to on reconnect.
nc.setReconnectedCallback(event -> {
    System.out.printf("Got reconnected to %s!\n", event.getConnection().getConnectedUrl());
});

// Setup a callback to be notified when the Connection is closed
nc.setClosedCallback( event -> {
    System.out.printf("Connection to %s has been closed.\n", event.getConnection().getConnectedUrl());
});

```

## License

(The MIT License)

Copyright (c) 2012-2016 Apcera Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

