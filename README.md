![NATS](src/main/javadoc/images/large-logo.png)

# NATS - Java Client

### A [Java](http://java.com) client for the [NATS messaging system](https://nats.io).

**Current Release**: 2.21.5 &nbsp; **Current Snapshot**: 2.21.6-SNAPSHOT

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.nats/jnats/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.nats/jnats)
[![javadoc](https://javadoc.io/badge2/io.nats/jnats/javadoc.svg)](https://javadoc.io/doc/io.nats/jnats)
[![Coverage Status](https://coveralls.io/repos/github/nats-io/nats.java/badge.svg?branch=main)](https://coveralls.io/github/nats-io/nats.java?branch=main)
[![Build Main Badge](https://github.com/nats-io/nats.java/actions/workflows/build-main.yml/badge.svg?event=push)](https://github.com/nats-io/nats.java/actions/workflows/build-main.yml)
[![Release Badge](https://github.com/nats-io/nats.java/actions/workflows/build-release.yml/badge.svg?event=release)](https://github.com/nats-io/nats.java/actions/workflows/build-release.yml)

### Examples and other documentation...

1. [**Java API Docs**](https://javadoc.io/doc/io.nats/jnats/latest/index.html) - the latest Java API docs.
1. [**NATS by Example**](https://natsbyexample.com) is an evolving collection of runnable, cross-client reference examples for NATS.
1. The [**examples directory**](https://github.com/nats-io/nats.java/tree/main/src/examples/java/io/nats/examples) covers basic api use.
1. The [**Java Nats Examples**](https://github.com/nats-io/java-nats-examples) GitHub repo, a collection of simple use case examples.
1. [**Java Orbit**](https://github.com/synadia-io/orbit.java) is a set of independent utilities or extensions for this client. 

## Table of Contents
* [Simplification](#simplification)
* [Service Framework](#service-framework)
* [Recent Version Notes](#recent-version-notes)
* [Installation](#installation)
* [Basic Usage](#basic-usage)
* [Connection Options](#connection-options)
* [JetStream](#jetstream)
* [Connection Security](#connection-security)
* [Manual Pull Subscriptions](#manual-pull-subscriptions)
* [Version Notes](#version-notes)
* [Benchmarking](#benchmarking)
* [Building From Source](#building-from-source)
* [License](#license)

## Simplification

There is a new simplified api that makes working with streams and consumers well, simpler! Simplification is released as of 2.16.14.

Check out the examples:

* [ContextExample](src/examples/java/io/nats/examples/jetstream/simple/ContextExample.java)
* [FetchBytesExample](src/examples/java/io/nats/examples/jetstream/simple/FetchBytesExample.java)
* [FetchMessagesExample](src/examples/java/io/nats/examples/jetstream/simple/FetchMessagesExample.java)
* [IterableConsumerExample](src/examples/java/io/nats/examples/jetstream/simple/IterableConsumerExample.java)
* [MessageConsumerExample](src/examples/java/io/nats/examples/jetstream/simple/MessageConsumerExample.java)
* [NextExample](src/examples/java/io/nats/examples/jetstream/simple/NextExample.java)

## Service Framework

The service API allows you to easily build NATS services. The Service Framework is released as of 2.16.14

The Services Framework introduces a higher-level API for implementing services with NATS. NATS has always been a strong technology on which to build services, 
as they are straightforward to write, are location and DNS independent, and can be scaled up or down by simply adding or removing instances of the service.

The Services Framework further streamlines their development by providing observability and standardization. 
The Service Framework allows your services to be discovered and queried for status without additional work.

Check out the [ServiceExample](src/examples/java/io/nats/examples/service/ServiceExample.java)

## Recent Version Notes

### Version 2.21.2

Starting with 2.21.2 snapshots, the project has been migrated to Sonatype's Maven Central Repository. 
Releases will still propagate to Maven Central as usual, but releases that are just published and not yet propagated,
and `-SNAPSHOTS` are available at a different urls. 
See [Using Gradle](#using-gradle) or [Using Maven](#using-maven) for more information

### Version 2.18.0 (AKA 2.17.7)

2.18.0 attempts to start us on the road to proper [Semantic Version (semver)](https://semver.org/). 
In the last few patch releases, there were technically things that should cause a minor version bump, 
but were numbered as a patch.

Even if just one api is newly added, semver requires that we bump the minor version. The `forceReconnect` api
is an example of one api being added to the Connection interface. It should have resulted in a minor version bump.

Going forward, when a release contains only bug fixes, it's appropriate to simply bump the patch. 
But if an api is added, even one, then the minor version will be bumped.

#### Force Reconnect

There is are new `Connection` interface apis:
* `void forceReconnect() throws IOException, InterruptedException;`
* `void forceReconnect(ForceReconnectOptions options) throws IOException, InterruptedException;`

If you call `forceReconnect`, your connection will be immediately closed and the reconnect logic will be executed.

#### Reverse Proxy Support

This version supports connecting to a reverse proxy securely while the proxy connects to the server insecurely. 
See the [Reverse Proxy Section](#reverse-proxy) for more details.

### Version 2.17.4 Core Improvements

This release was full of core improvements which improve the use of more asynchronous behaviors including
* removing use of `synchronized` in favor of `ReentrantLock`
* The ability to have a dispatcher use an executor to dispatch listener event messages instead of the dispatcher thread blocking to deliver the event.

### Version 2.17.3 Socket Write Timeout

The client, unless overridden, uses a java.net.Socket for connections. 
This java.net.Socket implementation does not support a write timeout, so writing data to the socket is a blocking call.

Under some conditions it will block indefinitely, freezing that connection on the client.
One way this could happen is if the server was too busy to read what was being sent.
Or, it could be a device, network, or connection issue.
Whatever it is, it blocks the jvm Socket write implementation which _used to_ block us.
It's rare, but it does happen.

To address this, we now monitor socket writes to ensure they complete within a timeout.
The timeout is configurable in Options via the builder and `socketWriteTimeout(duration|milliseconds)`.
The default is 1 minute if you don't set it. 
You can turn the watching off by setting a null duration or 0 milliseconds.

When the watcher is turned on, a background task watches the write operations and makes sure they complete within the timeout. 
If a write operation fails to complete, the task tells the connection to close the socket, which triggers the retry logic.
There may still be messages in the output queue, and messages that were in transit are in an unknown state. 
Handling disconnections and output queue is left for another discussion.

### Version Notes for older releases
See [Version Notes](#version-notes)

## Installation

The java-nats client is provided in a single jar file, with a single external dependency for the encryption in NKey support. 
See [Building From Source](#building-from-source) for details on building the library.
Replace `{major.minor.patch}` with the correct version in the examples.

### Downloading the Jar

You can download the latest jar at [https://search.maven.org/remotecontent?filepath=io/nats/jnats/2.21.5/jnats-2.21.5.jar](https://search.maven.org/remotecontent?filepath=io/nats/jnats/2.21.5/jnats-2.21.5.jar).

The examples are available at [https://search.maven.org/remotecontent?filepath=io/nats/jnats/2.21.5/jnats-2.21.5-examples.jar](https://search.maven.org/remotecontent?filepath=io/nats/jnats/2.21.5/jnats-2.21.5-examples.jar).

### Using Gradle

The NATS client is available in the Maven central repository, and can be imported as a standard dependency in your `build.gradle` file:

```groovy
dependencies {
    implementation 'io.nats:jnats:{major.minor.patch}'
}
```

If you need the latest and greatest before Maven central updates, you can use:

```groovy
repositories {
    mavenCentral()
    maven {
        url "https://repo1.maven.org/maven2/"
    }
}
```

If you need a snapshot version, you must add the url for the snapshots and change your dependency.

```groovy
repositories {
    mavenCentral()
    maven {
        url "https://central.sonatype.com/repository/maven-snapshots"
    }
}

dependencies {
   implementation 'io.nats:jnats:{major.minor.patch}-SNAPSHOT'
}
```

### Using Maven

The NATS client is available on the Maven Central Repository and can be imported as a normal dependency in your pom.xml file:

```xml
<dependency>
    <groupId>io.nats</groupId>
    <artifactId>jnats</artifactId>
    <version>{major.minor.patch}</version>
</dependency>
```

If you need the absolute latest, before it propagates to maven central, you can use the repository:

```xml
<repositories>
    <repository>
        <id>sonatype releases</id>
        <url>https://repo1.maven.org/maven2/</url>
        <releases>
           <enabled>true</enabled>
        </releases>
    </repository>
</repositories>
```

If you need a snapshot version, you must enable snapshots and change your dependency.

```xml
<repositories>
    <repository>
        <id>sonatype snapshots</id>
        <url>https://central.sonatype.com/repository/maven-snapshots</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>

<dependency>
    <groupId>io.nats</groupId>
    <artifactId>jnats</artifactId>
    <version>{major.minor.patch}-SNAPSHOT</version>
</dependency>
```

### Integration with GraalVM

To include this library with a GraalVM project, you must use configure these `initialize-at-run-time` classes. 
* `--initialize-at-run-time=java.security.SecureRandom`.
* `--initialize-at-run-time=io.nats.client.support.RandomUtils`
* `--initialize-at-run-time=io.nats.client.NUID`

These will instruct GraalVM to initialize specified classes at runtime so that these instances don't have fixed seeds. 
GraalVM won't compile without these parameters.

For a much more thorough discussion of the subject, please visit the [nats-graalvm-example](https://github.com/YunaBraska/nats-graalvm-example) repository 
made by one of our contributors. There is a detailed demonstration for creating an efficient NATS client with GraalVM.
This example leverages the client to connect to a server.

## Basic Usage

Sending and receiving with NATS is as simple as connecting to the nats-server and publishing or subscribing for messages.

Please see the examples in this project. The [Examples Readme](src/examples/java/io/nats/examples/README.md) is a good place to start.

There are also examples in the [java-nats-examples](https://github.com/nats-io/java-nats-examples) repo. 

### Connecting

There are five different ways to connect using the Java library, 
each with a parallel method that will allow doing reconnect logic if the initial connection fails.
The ability to reconnect on the initial connection failure is _NOT_ an Options setting.

1. Connect to a local server on the default url. From the Options class: `DEFAULT_URL = "nats://localhost:4222";`

    ```java
    // default options 
    Connection nc = Nats.connect();
   
    // default options, reconnect on connect 
    Connection nc = Nats.connectReconnectOnConnect();
    ```

1. Connect to one or more servers using a URL:

    ```java
    // single URL, all other default options
    Connection nc = Nats.connect("nats://myhost:4222");

    // comma-separated list of URLs, all other default options
    Connection nc = Nats.connect("nats://myhost:4222,nats://myhost:4223");

    // single URL, all other default options, reconnect on connect
    Connection nc = Nats.connectReconnectOnConnect("nats://myhost:4222");

    // comma-separated list of URLs, all other default options, reconnect on connect
    Connection nc = Nats.connectReconnectOnConnect("nats://myhost:4222,nats://myhost:4223");
    ```

1. Connect to one or more servers with a custom configuration:

    ```java
    Options o = new Options.Builder().server("nats://serverone:4222").server("nats://servertwo:4222").maxReconnects(-1).build();

    // custom options
    Connection nc = Nats.connect(o);

    // custom options, reconnect on connect
    Connection nc = Nats.connectReconnectOnConnect(o);
    ```

1. Connect asynchronously, this requires a callback to tell the application when the client is connected:

    ```java
    Options options = new Options.Builder().server(Options.DEFAULT_URL).connectionListener(handler).build();
    Nats.connectAsynchronously(options, true);
    ```

1. Connect with an authentication handler:

    ```java
    AuthHandler authHandler = Nats.credentials(System.getenv("NATS_CREDS"));

    // single URL, all other default options
    Connection nc = Nats.connect("nats://myhost:4222", authHandler);

    // comma-separated list of URLs, all other default options
    Connection nc = Nats.connect("nats://myhost:4222,nats://myhost:4223", authHandler);

    // single URL, all other default options, reconnect on connect
    Connection nc = Nats.connectReconnectOnConnect("nats://myhost:4222", authHandler);

    // comma-separated list of URLs, all other default options, reconnect on connect
    Connection nc = Nats.connectReconnectOnConnect("nats://myhost:4222,nats://myhost:4223", authHandler);
    ```

#### Clusters & Reconnecting

The Java client will automatically reconnect if it loses its connection to the nats-server. If given a single server, the client will keep trying that one. 
If given a list of servers, the client will rotate between them. When the NATS servers are in a cluster, they will tell the client about the other servers. 
In the simplest case a client could connect to one server, learn about the cluster, and reconnect to another server if its initial one goes down.

To tell the connection about multiple servers for the initial connection, use the `servers()` method on the Options builder, or call `server()` multiple times.

```Java
String[] serverUrls = {"nats://serverOne:4222", "nats://serverTwo:4222"};
Options o = new Options.Builder().servers(serverUrls).build();
```
Reconnection behavior is controlled via a few options. See the Javadoc for the Options.Builder class for specifics on reconnection limits, delays, and buffers.

## Connection Options

Connection options are configured using the `Options` class. There is a Builder that uses a fluent interface.
It can accept `Properties` object or a path to a Properties file.

### Property Names

The `io.nats.client.` prefix is not required in the properties file anymore. These are now equivalent:
```properties
io.nats.client.servers=nats://localhost:4222
```
```properties
servers=nats://localhost:4222
```

### Last-One Wins
The Options builder allows you to use both properties and code. When it comes to the builder, the last one called wins.
This applies to each property.

```java
props.setProperty(Options.PROP_MAX_MESSAGES_IN_OUTGOING_QUEUE, "7000");

o = new Options.Builder()
   .properties(props)
   .maxMessagesInOutgoingQueue(6000)
   .build();
assertEquals(6000, o.getMaxMessagesInOutgoingQueue());

o = new Options.Builder()
   .maxMessagesInOutgoingQueue(6000)
   .properties(props)
   .build();
assertEquals(7000, o.getMaxMessagesInOutgoingQueue());

o = new Options.Builder()
    .maxMessagesInOutgoingQueue(6000)
    .maxMessagesInOutgoingQueue(8000)
    .build();
assertEquals(8000, o.getMaxMessagesInOutgoingQueue());
```

### Properties

```
| Name                              | Description                                                                                 
|-----------------------------------|--------------------------------------------------------------------------------------|
| callback.connection               | Configure a connectionListener.                                                      |
| dataport.type                     | Configure a dataPortType.                                                            |
| callback.error                    | Configure an errorListener.                                                          |
| time.trace                        | Configura a TimeTraceLogger to receive trace events related to this connection       |
| statisticscollector               | Configure the statisticsCollector.                                                   |
| maxpings                          | Configure maxPingsOut.                                                               |
| pinginterval                      | Configure pingInterval.                                                              |
| cleanupinterval                   | Configure requestCleanupInterval.                                                    |
| timeout                           | Configure connectionTimeout.                                                         |
| socket.write.timeout              | Set the timeout around socket writes, providing support where Java is lacking        | 
| socket.so.linger                  | Configure the socket SO LINGER property for built in data port implementations       |
| reconnect.buffer.size             | Configure reconnectBufferSize.                                                       |
| reconnect.wait                    | Configure reconnectWait.                                                             |
| reconnect.max                     | Configure maxReconnects.                                                             |
| reconnect.jitter                  | Configure reconnectJitter.                                                           |
| reconnect.jitter.tls              | Configure reconnectJitterTls.                                                        |
| pedantic                          | Configure pedantic.                                                                  |
| verbose                           | Configure verbose.                                                                   |
| noecho                            | Configure noEcho.                                                                    |
| noheaders                         | Configure noHeaders.                                                                 |
| name                              | Configure connectionName.                                                            |
| nonoresponders                    | Configure noNoResponders.                                                            |
| norandomize                       | Configure noRandomize.                                                               |
| noResolveHostnames                | Configure noResolveHostnames.                                                        |
| reportNoResponders                | Configure reportNoResponders.                                                        |
| clientsidelimitchecks             | Configure clientsidelimitchecks.                                                     | 
| url                               | Configure server.  The value can be a comma-separated list of server URLs.           |
| servers                           | Configure servers. The value can be a comma-separated list of server URLs.           |
| password                          | Configure userinfo password.                                                         |
| username                          | Configure userinfo username.                                                         |
| token                             | Configure token.                                                                     |
| secure                            | See notes above on ssl configruration.                                               |
| opentls                           | See notes above on ssl configruration.                                               |
| outgoingqueue.maxmessages         | Configure maxMessagesInOutgoingQueue.                                                |
| outgoingqueue.discardwhenfull     | Configure discardMessagesWhenOutgoingQueueFull.                                      |
| use.old.request.style             | Configure oldRequestStyle.                                                           |
| max.control.line                  | Configure maxControlLine.                                                            |
| inbox.prefix                      | Property used to set the inbox prefix                                                |
| ignore.discovered.servers         | Preferred property used to set whether to ignore discovered servers when connecting. |
| servers.pool.implementation.class | Preferred property used to set class name for ServerPool implementation.             |
| dispatcher.factory.class          | Property used to set class name for the Dispatcher Factory                           |
| ssl.context.factory.class         | Property used to set class name for the SSLContextFactory                            |
| keyStore                          | Property for the keystore path used to create an SSLContext                          |
| keyStorePassword                  | Property for the keystore password used to create an SSLContext                      |
| trustStore                        | Property for the truststore path used to create an SSLContext                        |
| trustStorePassword                | Property for the truststore password used to create an SSLContext                    |
| tls.algorithm                     | Property for the algorithm used to create an SSLContext                              |
| credential.path                   | Property used to set the path to a credentials file to be used in a FileAuthHandler  |
| tls.first                         | Property used to set TLS Handshake First behavior                                    |
| use.timeout.exception             | Instruct the client to throw TimeoutException instead of CancellationException       |
| use.dispatcher.with.executor      | Instruct dispatchers to dispatch all messages as a task                              |
| fast.fallback                     | Use fast fallback algorithm for socket connection                                    |
```

### AuthHandler and JWT Credentials
You can manually create the AuthHandler and set it in the options
```java
AuthHandler ah = Nats.credentials("path/to/my.creds");
Options options = new Options.Builder()
    .authHandler(ah)
    .build();
```

or you can now set the file path directly and an AuthHandler will be created:
```java
Options options = new Options.Builder()
    .credentialPath("path/to/my.creds")
    .build();
```
The developer can also set the credential path in a properties file:
```properties
io.nats.client.credential.path=path/to/my.creds
```
### Fast Fallback
For mobile or frontend clients, you can enable fast fallback ([RFC](https://datatracker.ietf.org/doc/html/rfc6555)) to improve connection speed and reliability when connecting 
to hostnames that resolve to multiple IP addresses (IPv4/IPv6). This feature helps the client quickly select the fastest 
available address, reducing latency and improving resiliency in diverse network environments.
```java
Options options = new Options.Builder()
    .enableFastFallback()
    .build();
```

### SSLContext

The Options builder has several options set use or affect creation of an `SSLContext`

```java
// Provide the SSLContext
public Builder sslContext(SSLContext ctx)

// Generic SSLContext Creation
public Builder secure()
public Builder opentls()

// Custom SSLContext Creation Properties
public Builder keystore(String keystore)
public Builder keystorePassword(char[] keystorePassword)
public Builder truststore(String truststore)
public Builder truststorePassword(char[] truststorePassword)
public Builder tlsAlgorithm(String tlsAlgorithm)
```

There are equivalent properties for these builder methods (except sslContext):
```properties
# Generic SSLContext Creation
io.nats.client.secure=true
io.nats.client.opentls=true

# Custom SSLContext Creation Properties
io.nats.client.keyStore=path/to/keystore.jks
io.nats.client.keyStorePassword=kspassword
io.nats.client.trustStore=path/to/truststore.jks
io.nats.client.trustStorePassword=tspassword
io.nats.client.tls.algorithm=SunX509
```

When options are built, the SSLContext will be accepted or created in the following order.
1. If it's directly provided via the builder `sslContext(SSLContext ctx)` method.
2. If `keyStore` is provided, an SSLContext will be created using all custom properties. If not supplied, the tls algorithm is `SunX509`
3. If `opentls` is true or any of the bootstrap servers has `opentls` as their scheme, a generic SSLContext will be created that **"trusts all certs"**.
4. If `secure` is true or any of the bootstrap servers has `tls` or `wss` as their scheme, the `javax.net.ssl.SSLContext.getDefault()` will be used.

### Publishing

Once connected, publishing is accomplished via one of three methods:

1. With a subject and message body:

    ```java
    nc.publish("subject", "hello world".getBytes(StandardCharsets.UTF_8));
    ```

2. With a subject and message body, as well as a subject for the receiver to reply to:

    ```java
    nc.publish("subject", "replyto", "hello world".getBytes(StandardCharsets.UTF_8));
    ```

3. As a request that expects a reply. This method uses a Future to allow the application code to wait for the response. Under the covers a request/reply pair is the same as a publish/subscribe only the library manages the subscription for you.

    ```java
    Future<Message> incoming = nc.request("subject", "hello world".getBytes(StandardCharsets.UTF_8));
    Message msg = incoming.get(500, TimeUnit.MILLISECONDS);
    String response = new String(msg.getData(), StandardCharsets.UTF_8);
    ```

All of these methods, as well as the incoming message code use byte arrays for maximum flexibility. Applications can
send JSON, Strings, YAML, Protocol Buffers, or any other format through NATS to applications written in a wide range of
languages.

#### ReplyTo When Making A Request

The Message object allows you to set a replyTo, but in requests,
the replyTo is reserved for internal use as the address for the
server to respond to the client with the consumer's reply.

### Listening for Incoming Messages

The Java NATS library provides two mechanisms to listen for messages, three if you include the request/reply discussed above.

1. Synchronous subscriptions where the application code manually asks for messages and blocks until they arrive. Each subscription is associated with a single subject, although that subject can be a wildcard.

    ```java
    Subscription sub = nc.subscribe("subject");
    Message msg = sub.nextMessage(Duration.ofMillis(500));

    String response = new String(msg.getData(), StandardCharsets.UTF_8);
    ```

2. A Dispatcher that will call application code in a background thread. Dispatchers can manage multiple subjects with a single thread and shared callback.

    ```java
    Dispatcher d = nc.createDispatcher((msg) -> {
        String response = new String(msg.getData(), StandardCharsets.UTF_8);
        ...
    });

    d.subscribe("subject");
    ```

    A dispatcher can also accept individual callbacks for any given subscription.

    ```java
    Dispatcher d = nc.createDispatcher((msg) -> {});

    Subscription s = d.subscribe("some.subject", (msg) -> {
        String response = new String(msg.getData(), StandardCharsets.UTF_8);
        System.out.println("Message received (up to 100 times): " + response);
    });
    d.unsubscribe(s, 100);
    ```

## JetStream

Publishing and subscribing to JetStream-enabled servers are straightforward. A 
JetStream-enabled application will connect to a server, establish a JetStream 
context, and then publish or subscribe.  This can be mixed and matched with a standard
NATS subject, and JetStream subscribers, depending on configuration, receive messages
from both streams and directly from other NATS producers.

### The JetStream Context

After establishing a connection as described above, create a JetStream Context.

```java
JetStream js = nc.jetStream();
```

You can pass options to configure the JetStream client, although the defaults should
suffice for most users.  See the `JetStreamOptions` class.

There is no limit to the number of contexts used, although normally one would only
require a single context.  Contexts may be prefixed to be used in conjunction with
NATS authorization.

#### Subject and Queue Name Validation

For subjects, the client was strict when validating subject names for consumer subject filters and subscriptions.
It only allowed printable ascii characters except for `*`, `>`, `.`, `\\` and `/`. 
This restriction has been changed to the following:
* cannot contain spaces \r \n \t
* cannot start or end with a subject token delimiter
* cannot have empty segments

This means that UTF characters are now allowed in subjects in this client.

For queue names, there has been inconsistent validation, if any. Queue names now require the same validation as subjects.

#### Subscribe Subject Validation

Additionally, for subjects used in the subscribe api, applications may start throwing an exception:

```text
90011 Subject does not match consumer configuration filter
```
Let's say you have a stream with subject `foo.>` And you are subscribing to `foo.a`.
When you don't supply a filter subject on a consumer, it becomes `>`, which means all subjects.

So this is a problem, because you think you are subscribing to `foo.a` but in reality, without this check,
you will be getting all messages `foo.>` subjects, not just `foo.a`

Validating a subscribe subject against the filter subject is needed to prevent this.
Unfortunately, this makes existing code throw the `90011` exception.

### Publishing

To publish messages, use the `JetStream.publish(...)` API.  A stream must be established
before publishing. You can publish in either a synchronous or asynchronous manner.

#### Synchronous

```java
// create a typical NATS message
Message msg = NatsMessage.builder()
   .subject("foo")
   .data("hello", StandardCharsets.UTF_8)
   .build();

PublishAck pa = js.publish(msg);
```

See `NatsJsPub.java` in the JetStream examples for a detailed and runnable example.

If there is a problem, an exception will be thrown, and the message may not have been
persisted.  Otherwise, the stream name and sequence number are returned in the Publish
acknowledgement.

There are a variety of publish options that can be set when publishing.  When duplicate
checking has been enabled on the stream, a message ID should be set. One set of options
are publish expectations, such as a particular stream name, previous message ID, or previous sequence number.
These are hints to the server that it should reject messages where these are not met, primarily for enforcing your ordering
or ensuring messages are not stored on the wrong stream.

The PublishOptions are immutable, but the builder an be re-used for expectations by clearing the expected.

For example:

```java
PublishOptions.Builder pubOptsBuilder = PublishOptions.builder()
        .expectedStream("TEST")
        .messageId("mid1");
PublishAck pa = js.publish("foo", null, pubOptsBuilder.build());

pubOptsBuilder.clearExpected()
        .setExpectedLastMsgId("mid1")
        .setExpectedLastSequence(1)
        .messageId("mid2");
pa = js.publish("foo", null, pubOptsBuilder.build());
```

See `NatsJsPubWithOptionsUseCases.java` in the JetStream examples for a detailed and runnable example.

#### Asynchronous

```java
List<CompletableFuture<PublishAck>> futures = new ArrayList<>();
for (int x = 1; x < roundCount; x++) {
    // create a typical NATS message
    Message msg = NatsMessage.builder()
    .subject("foo")
    .data("hello", StandardCharsets.UTF_8)
    .build();

    // Publish a message
    futures.add(js.publishAsync(msg));
}

for (CompletableFuture<PublishAck> future : futures) {
   ... process the futures
}
```

See the `NatsJsPubAsync.java` in the JetStream examples for a detailed and runnable example.

#### ReplyTo When Publishing

The Message object allows you to set a replyTo, but in a Publish,
the replyTo is reserved for internal use as the address for the
server to respond to the client with the PublishAck.

### Consuming Messages

There are two methods of subscribing, **Push** and **Pull** with each variety having its own set of options and abilities. 

#### Asynchronous Push Subscription

Push subscriptions can be synchronous or asynchronous. The server *pushes* messages to the client.

```java
Dispatcher disp = ...;

MessageHandler handler = (msg) -> {
// Process the message.
// Ack the message depending on the ack model
};

PushSubscribeOptions so = PushSubscribeOptions.builder()
   .durable("optional-durable-name")
   .build();

boolean autoAck = ...

js.subscribe("my-subject", disp, handler, autoAck);
```

See the `NatsJsPushSubWithHandler.java` in the JetStream examples for a detailed and runnable example.

#### Subscribe Subject

With the introduction of simplification, the various original subscribe methods available will eventually be deprecated.
But since they are available, we need to address the concept of the "subscribe subject".

Consider the example:
```java
js.subscribe("my-subject", disp, handler, autoAck);
```

The subscribe method takes a "subject" as the first parameter. We call this the "subscribe subject".
The subject could be something like `my.subject` or `my.star.*` or `my.gt.>` or even `>`.
This parameter is used and validated in different ways depending on the context of the call,
including looking up the stream if the stream is not provided via subscribe options.

The "subscribe subject" could be used to make a simple subscription. In this case it is required.
An ephemeral consumer will be created for that subject, assuming that subject can be looked up in a stream.

```java
JetStream js = nc.jetStream();
JetStreamSubscription sub = subscribe(subject)
```

If a subscribe call has either a PushSubscribeOptions or PullSubscribeOptions that have a ConsumerConfiguration
with one or more filter subjects, the "subscribe subject" is optional since we can use the first filter subject as
the "subscribe subject".

```java
PushSubscribeOptions pso = ConsumerConfiguration.builder().filterSubject("my.subject").buildPushSubscribeOptions();
js.subscribe(null, pso);
```

The other time you can skip the subject parameter is when you bind. Since the stream name and consumer name are
part of the bind, the subject will be retrieved from the consumer looked-up via the bind stream and consumer name information.

#### Synchronous Consuming

See `NatsJsPushSub.java` in the JetStream examples for a detailed and runnable example.

```java
PushSubscribeOptions so = PushSubscribeOptions.builder()
        .durable("optional-durable-name")
        .build();

// Subscribe synchronously, then just wait for messages.
JetStreamSubscription sub = js.subscribe("subject", so);
nc.flush(Duration.ofSeconds(5));

Message msg = sub.nextMessage(Duration.ofSeconds(1));
```

#### Multiple Filter Subjects

The client has the ability to have multiple filter subjects for any single JetStream consumer. 
This can only be set up via the Consumer Configuration.

```java
ConsumerConfiguration cc = ConsumerConfiguration.builder()
    ...
    .filterSubjects("subject1", "subject2")
    .build();
```

### Message Acknowledgements

There are multiple types of acknowledgements in JetStream:

* `Message.ack()`: Acknowledges a message.
* `Message.ackSync(Duration)`: Acknowledges a message and waits for a confirmation. When used with deduplication, this creates exactly once delivery guarantees (within the deduplication window).  This may significantly impact the performance of the system.
* `Message.nak()`: A negative acknowledgment indicating processing failed and the message should be resent later.
* `Message.term()`: Never send this message again, regardless of configuration.
* `Message.inProgress()`:  The message is being processed and reset the redelivery timer in the server.  The message must be acknowledged later when processing is complete.

Note that the exactly once delivery guarantee can be achieved by using a consumer with explicit ack mode attached to stream setup with a deduplication window and using the `ackSync` to acknowledge messages.  The guarantee is only valid for the duration of the deduplication window.

### Ordered Push Subscription Option

You can now configure a Push Subscription to be "Ordered". 
When you set this flag, the library will take over the creation of the consumer and create a subscription that guarantees the order of messages.
This consumer will use flow control with a default heartbeat of 5 seconds. Messages will not require acks as the Ack Policy will be set to No Ack.
When creating the subscription, there are some restrictions for the consumer configuration settings.

- Ack policy must be AckPolicy.None (or left unset). maxAckPending will be ignored.
- Deliver Group (aka Queue) cannot be used
- You cannot set a durable consumer name
- You cannot set the Deliver Subject
- max deliver can only be set to 1 (or left unset)  
- The idle heartbeat cannot be less than 5 seconds. Flow control will automatically be used.

You can, however, set the Deliver Policy which will be used to start the subscription. 

### Client Error Messages

`In addition to some generic validation messages for values in builders, there are also additional grouped and numbered client error messages:
* Subscription building and creation
* Consumer creation
* Object Store operations

```
| Name                                         | Group-Code| Description                                                                                                    |
|----------------------------------------------|-----------|----------------------------------------------------------------------------------------------------------------|
| JsSoDurableMismatch                          | SO-90101  | Builder durable must match the consumer configuration durable if both are provided.                            |
| JsSoDeliverGroupMismatch                     | SO-90102  | Builder deliver group must match the consumer configuration deliver group if both are provided.                |
| JsSoDeliverSubjectMismatch                   | SO-90103  | Builder deliver subject must match the consumer configuration deliver subject if both are provided.            |
| JsSoOrderedNotAllowedWithBind                | SO-90104  | Bind is not allowed with an ordered consumer.                                                                  |
| JsSoOrderedNotAllowedWithDeliverGroup        | SO-90105  | Deliver group is not allowed with an ordered consumer.                                                         |
| JsSoOrderedNotAllowedWithDurable             | SO-90106  | Durable is not allowed with an ordered consumer.                                                               |
| JsSoOrderedNotAllowedWithDeliverSubject      | SO-90107  | Deliver subject is not allowed with an ordered consumer.                                                       |
| JsSoOrderedRequiresAckPolicyNone             | SO-90108  | Ordered consumer requires Ack Policy None.                                                                     |
| JsSoOrderedRequiresMaxDeliverOfOne           | SO-90109  | Max deliver is limited to 1 with an ordered consumer.                                                          |
| JsSoNameMismatch                             | SO-90110  | Builder name must match the consumer configuration name if both are provided.                                  |
| JsSoOrderedMemStorageNotSuppliedOrTrue       | SO-90111  | Mem Storage must be true if supplied.                                                                          |
| JsSoOrderedReplicasNotSuppliedOrOne          | SO-90112  | Replicas must be 1 if supplied.                                                                                |
| JsSoNameOrDurableRequiredForBind             | SO-90113  | Name or Durable required for Bind.                                                                             |
| JsSubPullCantHaveDeliverGroup                | SUB-90001 | Pull subscriptions can't have a deliver group.                                                                 |
| JsSubPullCantHaveDeliverSubject              | SUB-90002 | Pull subscriptions can't have a deliver subject.                                                               |
| JsSubPushCantHaveMaxPullWaiting              | SUB-90003 | Push subscriptions cannot supply max pull waiting.                                                             |
| JsSubQueueDeliverGroupMismatch               | SUB-90004 | Queue / deliver group mismatch.                                                                                |
| JsSubFcHbNotValidPull                        | SUB-90005 | Flow Control and/or heartbeat is not valid with a pull subscription.                                           |
| JsSubFcHbNotValidQueue                       | SUB-90006 | Flow Control and/or heartbeat is not valid in queue mode.                                                      |
| JsSubNoMatchingStreamForSubject              | SUB-90007 | No matching streams for subject.                                                                               |
| JsSubConsumerAlreadyConfiguredAsPush         | SUB-90008 | Consumer is already configured as a push consumer.                                                             |
| JsSubConsumerAlreadyConfiguredAsPull         | SUB-90009 | Consumer is already configured as a pull consumer.                                                             |
| _removed_                                    | SUB-90010 |                                                                                                                |
| JsSubSubjectDoesNotMatchFilter               | SUB-90011 | Subject does not match consumer configuration filter.                                                          |
| JsSubConsumerAlreadyBound                    | SUB-90012 | Consumer is already bound to a subscription.                                                                   |
| JsSubExistingConsumerNotQueue                | SUB-90013 | Existing consumer is not configured as a queue / deliver group.                                                |
| JsSubExistingConsumerIsQueue                 | SUB-90014 | Existing consumer is configured as a queue / deliver group.                                                    |
| JsSubExistingQueueDoesNotMatchRequestedQueue | SUB-90015 | Existing consumer deliver group does not match requested queue / deliver group.                                |
| JsSubExistingConsumerCannotBeModified        | SUB-90016 | Existing consumer cannot be modified.                                                                          |
| JsSubConsumerNotFoundRequiredInBind          | SUB-90017 | Consumer not found, required in bind mode.                                                                     |
| JsSubOrderedNotAllowOnQueues                 | SUB-90018 | Ordered consumer not allowed on queues.                                                                        |
| JsSubPushCantHaveMaxBatch                    | SUB-90019 | Push subscriptions cannot supply max batch.                                                                    |
| JsSubPushCantHaveMaxBytes                    | SUB-90020 | Push subscriptions cannot supply max bytes.                                                                    |
| JsSubPushAsyncCantSetPending                 | SUB-90021 | Pending limits must be set directly on the dispatcher.                                                         |
| JsSubSubjectNeededToLookupStream             | SUB-90022 | Subject needed to lookup stream. Provide either a subscribe subject or a ConsumerConfiguration filter subject. |
| JsConsumerCreate290NotAvailable              | CON-90301 | Name field not valid when v2.9.0 consumer create api is not available.                                         |
| JsConsumerNameDurableMismatch                | CON-90302 | Name must match durable if both are supplied.                                                                  |
| JsMultipleFilterSubjects210NotAvailable      | CON-90303 | Multiple filter subjects not available until server version 2.10.0.                                            |
| OsObjectNotFound                             | OS-90201  | The object was not found.                                                                                      |
| OsObjectIsDeleted                            | OS-90202  | The object is deleted.                                                                                         |
| OsObjectAlreadyExists                        | OS-90203  | An object with that name already exists.                                                                       |
| OsCantLinkToLink                             | OS-90204  | A link cannot link to another link.                                                                            |
| OsGetDigestMismatch                          | OS-90205  | Digest does not match meta data.                                                                               |
| OsGetChunksMismatch                          | OS-90206  | Number of chunks does not match meta data.                                                                     |
| OsGetSizeMismatch                            | OS-90207  | Total size does not match meta data.                                                                           |
| OsGetLinkToBucket                            | OS-90208  | Cannot get object, it is a link to a bucket.                                                                   |
| OsLinkNotAllowOnPut                          | OS-90209  | Link not allowed in metadata when putting an object.                                                           |
```

## Connection Security

NATS supports TLS 1.2. The server can be configured to verify client certificates or not. Depending on this setting, the client has several options.

1. The Java library allows the use of the tls:// protocol in its urls. This setting expects a default SSLContext to be set. You can set this default context using System properties, or in code. For example, you could run the Publish example using:

    ```bash
    java -Djavax.net.ssl.keyStore=src/test/resources/keystore.jks -Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStore=src/test/resources/truststore.jks -Djavax.net.ssl.trustStorePassword=password io.nats.examples.NatsPub tls://localhost:4443 test "hello world"
    ```

    where the following properties are being set:

    ```bash
    -Djavax.net.ssl.keyStore=src/test/resources/keystore.jks
    -Djavax.net.ssl.keyStorePassword=password
    -Djavax.net.ssl.trustStore=src/test/resources/truststore.jks
    -Djavax.net.ssl.trustStorePassword=password
    ```

    This method can be used with or without client verification.

2. During development, or behind a firewall where the client can trust the server, the library supports the opentls:// protocol which will use a special SSLContext that trusts all server certificates but provides no client certificates.

    ```bash
    java io.nats.examples.NatsSub opentls://localhost:4443 test 3
    ```

    This method requires that client verification is off.

3. Your code can build an SSLContext to work with or without client verification.

    ```java
    SSLContext ctx = createContext();
    Options options = new Options.Builder().server(ts.getURI()).sslContext(ctx).build();
    Connection nc = Nats.connect(options);
    ```

If you want to try out these techniques, take a look at the [README.md](src/examples/java/io/nats/examples/README.md) for instructions.

Also, here are some places in the code that may help
https://github.com/nats-io/nats.java/blob/main/src/main/java/io/nats/client/support/SSLUtils.java
https://github.com/nats-io/nats.java/blob/main/src/test/java/io/nats/client/TestSSLUtils.java

### TLS Certs

The raw TLS test certs are in [src/test/resources/certs](src/test/resources/certs) and come from the [nats.go](https://github.com/nats-io/nats.go) repository. 
However, the java client also needs a keystore and truststore.jks files for creating a context. These can be created using the following commands:


```bash
> cd src/test/resources
> keytool -keystore truststore.jks -alias CARoot -import -file certs/ca.pem -storepass password -noprompt -storetype pkcs12
> cat certs/client-key.pem certs/client-cert.pem > combined.pem
> openssl pkcs12 -export -in combined.pem -out cert.p12
> keytool -importkeystore -srckeystore cert.p12 -srcstoretype pkcs12 -deststoretype pkcs12 -destkeystore keystore.jks
> keytool -keystore keystore.jks -alias CARoot -import -file certs/ca.pem -storepass password -noprompt
> rm cert.p12 combined.pem
```

### TLS client versus server checks

When creating a connection, client TLS behavior is set while creating options.
The client assumes TLS is requested if there is an SSLContext instance in the options. 
There are two ways one exists:
1. The user directly supplied one 
2. A default one was created since one was not supplied, but a supplied server url has a secure protocol such as `tls`, `wss` or `opentls`

If there is a mismatch, an IOException will be thrown while connecting.

| server config | client options    | result                                       |
|---------------|-------------------|----------------------------------------------|
| required      | tls not requested | mismatch, "SSL required by server."          | 
| available     | tls not requested | ok                                           |    
| neither       | tls not requested | ok                                           |   
| required      | tls requested     | ok                                           |
| available     | tls requested     | ok                                           |
| neither       | tls requested     | mismatch, "SSL connection wanted by client." |     


### TLS Handshake First
In Server 2.10.3 and later, there is the ability to have TLS Handshake First.

The server config will contain this:

```text
tls {
  ...
  handshake_first: 300ms
}
```

TLS Handshake First is used to instruct the library to perform
the TLS handshake right after connecting but before receiving
the INFO protocol from the server. If this option is enabled
but the server is not configured to perform the TLS handshake
first, the connection will fail.

### Reverse Proxy
In a reverse proxy configuration, the client connects securely to the reverse proxy, 
and the proxy may connect securely or insecurely to the server.

If the proxy connects securely to the server,
then there is nothing special required to do at all.

But most commonly, the proxy connects insecurely to the server.
This is where server configuration comes into play.
You will need to configure the server like so:

```
tls {}
allow_non_tls: true
```

Before this, the client would not connect
because the server was not requiring tls for the proxy,
but the client was configured as secure because it was connecting securely to the proxy.
The client thought that this was a mismatch and would not connect,
essentially failing fast instead of waiting for the server to reject the connection attempt.

The latest version of the client is able to recognize this server configuration
and understands that it's okay to connect securely to the proxy regardless of the
server configuration.

You have to make sure you can properly connect securely to the proxy,
and that's where the code in this sample comes in.

### NKey-based Challenge Response Authentication

The NATS server is adding support for a challenge response authentication scheme based on [NKeys](https://github.com/nats-io/nkeys). Version 2.2.0 of
the Java client supports this scheme via an AuthHandler interface. *Version 2.3.0 replaced several NKey methods that used strings with methods using char[] to improve security.*

### OCSP Stapling
The server supports OCSP stapling. To enable Java to automatically check the stapling
when making TLS connections, you must set system properties. Please be aware that this affects the _entire_ JVM,
so all connections.

These properties can be set from your command line or from your Java code:

```
System.setProperty("jdk.tls.client.enableStatusRequestExtension", "true");
System.setProperty("com.sun.net.ssl.checkRevocation", "true");
```

For more information, see the Oracle Java documentation page on [Client-Driven OCSP and OCSP Stapling](https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/ocsp.html)

Also, there is a detailed [OCSP Example](https://github.com/nats-io/java-nats-examples/tree/main/ocsp) that shows how to create SSL contexts enabling OCSP stapling.

### SSL/TLS Performance

After recent tests we realized that TLS performance is lower than we would like. After researching the problem and possible solutions, we came to a few conclusions:

* TLS performance for the native JDK has not been historically great
* TLS performance is better in JDK12 than JDK8
* A small fix to the library in 2.5.1 allows the use of https://github.com/google/conscrypt and https://github.com/wildfly/wildfly-openssl, Conscrypt provides the best performance in our tests
* TLS still comes at a price (1gb/s vs. 4gb/s in some tests), but using the JNI libraries can result in a 10x boost in our testing
* If TLS performance is reasonable for your application, we recommend using the j2se implementation for simplicity

To use Conscrypt or Wildfly, you will need to add the appropriate jars to your class path and create an SSL context manually. This context can be passed to the Options used when creating a connection. 
The NATSAutoBench example provides a Conscrypt flag which can be used to try out the library, manually including the jar is required.

## Manual Pull Subscriptions

Pull subscriptions are always synchronous. The server organizes messages into a batch
which it sends when requested. You must make a new pull request for each batch.


```java
PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
   .durable("durable-name-is-optional")
   .build();
JetStreamSubscription sub = js.subscribe("subject", pullOptions);
```

#### Bind

Pull subscriptions allow for binding to existing consumers.
The best practice is to provide `null` for the "subscribe subject", but if you do
provide it, it must match the consumer subject filter, or you will receive an
`IllegalArgumentException`. See client errors below and `JsSubSubjectDoesNotMatchFilter 90011`

1. Short Form

    ```java
    PullSubscribeOptions pullOptions = PullSubscribeOptions.bind("stream", "durable-name");
    JetStreamSubscription sub = js.subscribe(null, pullOptions);
    ```

2. Long Form

    ```java
    PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
        .stream("stream")
        .durable("durable-name")
        .bind(true)
        .build();
    ```

#### Fetch

```java
List<Message> message = sub.fetch(100, Duration.ofSeconds(1));
for (Message m : messages) {
   // process message
   m.ack();
}
```

The fetch method is a *macro* pull that uses advanced pulls under the covers to return a list of messages.
The list may be empty or contain at most the batch size.
All status messages are handled for you except terminal status messages.
The client can provide a timeout to wait for the first message in a batch.
The fetch call returns when the batch is ready.
If the timeout is exceeded while messages are in flight, but before they reach the client,
those messages will be available via nextMessage or will be used to fulfill the next fetch.

One important thing to consider when using this is ack wait. Once the server sends a message,
its specific ack-wait timer is started. If you ask for too many messages, you may fail to
ack all messages in time and can get redeliveries.

See `NatsJsPullSubFetch.java` and `NatsJsPullSubFetchUseCases.java`
in the JetStream examples for a detailed and runnable example.

#### Iterate

```java
        Iterator<Message> iter = sub.iterate(100, Duration.ofSeconds(1));
        while (iter.hasNext()) {
            Message m = iter.next();
            // process message
            m.ack();
        }
```

The iterate method is a *macro* pull that uses advanced pulls under the covers to return an iterator.
The iterator may have no messages up to at most the batch size.
All status messages are handled for you except terminal status messages.
The client can provide a timeout to wait for the first message in a batch.
The iterate method call returns the iterator immediately, but under the covers it will wait for the first
message based on the timeout.

The iterate method is usually preferred to the fetch method as it allows you to start processing messages
right away instead of waiting until the entire batch is filled. This reduces problems with ack wait
and generally is more efficient.

See `NatsJsPullSubIterate.java` and `NatsJsPullSubIterateUseCases.java`
in the JetStream examples for a detailed and runnable example.

#### Batch Size

```java
sub.pull(100);
...
Message m = sub.nextMessage(Duration.ofSeconds(1));
```

This is an advanced / raw pull that specifies a batch size. When asked, the server will send whatever
messages it has up to the batch size. If it has no messages, it will wait until it has some to send.
The pull request only completes on the server once the entire batch has been sent.
It's up to you to track this and only send pulls when the batch is complete, or you risk having
pulls stack up and possibly receiving a status `409 Exceeded MaxWaiting` warning.
The nextMessage request may time out, but that does not indicate that there are no more messages in the pull.
Instead, it indicates that there is no message available at that time.
Once the entire batch size has been filled, you must make another pull request.

See `NatsJsPullSubBatchSize.java` and `NatsJsPullSubBatchSizeUseCases.java`
in the JetStream examples for detailed and runnable example.

#### No Wait and Batch Size

```java
sub.pullNoWait(100);
...
Message m = sub.nextMessage(Duration.ofSeconds(1));
```

This is an advanced / raw pull that also specifies a batch size.
When asked, the server will send whatever messages it has at the moment
the pull request is processed by the server, up to the batch size.
If there is less than the batch size available, you will get what is available.

See the `NatsJsPullSubNoWaitUseCases.java` in the JetStream examples for a detailed and runnable example.

#### Expires In and Batch Size

```java
sub.pullExpiresIn(100, Duration.ofSeconds(3));
...
Message m = sub.nextMessage(Duration.ofSeconds(4));
```

Another advanced version of pull specifies a maximum time to wait for the batch to fill.
The server sends messages up until the batch is filled or the time expires. It's important to
set your client's nextMessage timeout to be longer than the time you've asked the server to expire in.
Once "nextMessage()" returns null, you know your pull is done, and you can make another one.

See `NatsJsPullSubExpire.java` and `NatsJsPullSubExpireUseCases.java`
in the JetStream examples for detailed and runnable examples.

## Version Notes

### Version 2.17.2 Message Immutability Headers Bug

Once a message is created, it is intended to be immutable.
Before 2.17.2, the Headers object could be modified (via put, add, remove) after construction,
either directly with the developer's original Headers object or from the one available via `Message.getHeaders()`.
This will cause a protocol failure when the message is written to the server,
because the protocol size had already been calculated.
This calculation is done at construction time because there are multiple places in the workflow
that rely on the protocol size, so it must not change once created.

### Version 2.17.1 Support for TLS Handshake First

There is a new connection Option, `tlsFirst` for "TLS Handshake First"
See the [TLS Handshake First](#tls-handshake-first) for more details.

#### Version 2.17.0: Server 2.10 support.
The release has support for Server 2.10 features and client validation improvements including:

* Stream and Consumer info timestamps
* Stream Configuration
    * Compression Option
    * Subject Transform
    * Consumer Limits
    * First Sequence
* [Multiple Filter Subjects](#multiple-filter-subjects)
* [Subject and Queue Name Validation](#subject-and-queue-name-validation)
* [Subscribe Subject Validation](#subscribe-subject-validation)

### Version 2.16.14: Options properties improvements

In this release, support was added to
* support property keys with or without the prefix 'io.nats.client.'
* allow creation of connections requiring an AuthHandler for JWT to specify the credentials file in a properties files, instead of needing to provide an instance of AuthHandler in code.
* allow creation of connections requiring an SSL context to specify key and trust store information in a properties files so an SSLContext can be created automatically instead of needing to provide an instance of an SSLContext in code.

For details on the other features, see the "Options" sections

### Version 2.16.8: Websocket Support

As of version 2.16.8 Websocket (`ws` and `wss`) protocols are supported for connecting to the server.
For instance, your server bootstrap url might be `ws://my-nats-host:80` or `wss://my-nats-host:443`.

Your server must be properly configured for websocket, see the NATS.IO docs
[WebSocket Configuration Example](https://docs.nats.io/running-a-nats-service/configuration/websocket/websocket_conf)
for more information.

If you use secure websockets (wss), your connection must be securely configured in the same way you would configure a `tls` connection.

### Version 2.16.0: Consumer Create

This release by default will use a new JetStream consumer create API when interacting with nats-server version 2.9.0 or higher.
This changes the subjects used by the client to create consumers, which might in some cases require changes in access and import/export configuration.
The developer can opt out of using this feature by using a custom JetStreamOptions and using it when creating
JetStream, Key Value, and Object Store regular and management contexts.

```java
JetStreamOptions jso = JetStreamOptions.builder().optOut290ConsumerCreate(true).build();

JetStream js = connection.jetStream(jso);
JetStreamManagement jsm = connection.jetStreamManagement(jso);
KeyValue kv = connection.keyValue("bucket", KeyValueOptions.builder(jso).build());
KeyValueManagement kvm = connection.keyValueManagement(KeyValueOptions.builder(jso).build());
ObjectStore os = connection.objectStore("bucket", ObjectStoreOptions.builder(jso).build());
ObjectStoreManagement osm = connection.objectStoreManagement(ObjectStoreOptions.builder(jso).build());
```

## Benchmarking

The `io.nats.examples` package contains two benchmarking tools, modeled after tools in other NATS clients. Both examples run against an existing nats-server. The first called `io.nats.examples.benchmark.NatsBench` runs two simple tests, the first simply publishes messages, the second also receives messages. Tests are run with 1 thread/connection per publisher or subscriber. Running on an iMac (2017), with 4.2 GHz Intel Core i7 and 64GB of memory produced results like:

```AsciiDoc
Starting benchmark(s) [msgs=5000000, msgsize=256, pubs=2, subs=2]
Current memory usage is 966.14 mb / 981.50 mb / 14.22 gb free/total/max
Use ctrl-C to cancel.
Pub Only stats: 9,584,263 msgs/sec ~ 2.29 gb/sec
 [ 1] 4,831,495 msgs/sec ~ 1.15 gb/sec (2500000 msgs)
 [ 2] 4,792,145 msgs/sec ~ 1.14 gb/sec (2500000 msgs)
  min 4,792,145 | avg 4,811,820 | max 4,831,495 | stddev 19,675.00 msgs
Pub/Sub stats: 3,735,744 msgs/sec ~ 912.05 mb/sec
 Pub stats: 1,245,680 msgs/sec ~ 304.12 mb/sec
  [ 1] 624,385 msgs/sec ~ 152.44 mb/sec (2500000 msgs)
  [ 2] 622,840 msgs/sec ~ 152.06 mb/sec (2500000 msgs)
   min 622,840 | avg 623,612 | max 624,385 | stddev 772.50 msgs
 Sub stats: 2,490,461 msgs/sec ~ 608.02 mb/sec
  [ 1] 1,245,230 msgs/sec ~ 304.01 mb/sec (5000000 msgs)
  [ 2] 1,245,231 msgs/sec ~ 304.01 mb/sec (5000000 msgs)
   min 1,245,230 | avg 1,245,230 | max 1,245,231 | stddev .71 msgs
Final memory usage is 2.02 gb / 2.94 gb / 14.22 gb free/total/max
```

The second, called `io.nats.examples.autobench.NatsAutoBench` runs a series of tests with various message sizes. Running this test on the same iMac, resulted in:

```AsciiDoc
PubOnly 0b           10,000,000          8,464,850 msg/s       0.00 b/s
PubOnly 8b           10,000,000         10,065,263 msg/s     76.79 mb/s
PubOnly 32b          10,000,000         12,534,612 msg/s    382.53 mb/s
PubOnly 256b         10,000,000          7,996,057 msg/s      1.91 gb/s
PubOnly 512b         10,000,000          5,942,165 msg/s      2.83 gb/s
PubOnly 1k            1,000,000          4,043,937 msg/s      3.86 gb/s
PubOnly 4k              500,000          1,114,947 msg/s      4.25 gb/s
PubOnly 8k              100,000            460,630 msg/s      3.51 gb/s
PubSub 0b            10,000,000          3,155,673 msg/s       0.00 b/s
PubSub 8b            10,000,000          3,218,427 msg/s     24.55 mb/s
PubSub 32b           10,000,000          2,681,550 msg/s     81.83 mb/s
PubSub 256b          10,000,000          2,020,481 msg/s    493.28 mb/s
PubSub 512b           5,000,000          2,000,918 msg/s    977.01 mb/s
PubSub 1k             1,000,000          1,170,448 msg/s      1.12 gb/s
PubSub 4k               100,000            382,964 msg/s      1.46 gb/s
PubSub 8k               100,000            196,474 msg/s      1.50 gb/s
PubDispatch 0b       10,000,000          4,645,438 msg/s       0.00 b/s
PubDispatch 8b       10,000,000          4,500,006 msg/s     34.33 mb/s
PubDispatch 32b      10,000,000          4,458,481 msg/s    136.06 mb/s
PubDispatch 256b     10,000,000          2,586,563 msg/s    631.49 mb/s
PubDispatch 512b      5,000,000          2,187,592 msg/s      1.04 gb/s
PubDispatch 1k        1,000,000          1,369,985 msg/s      1.31 gb/s
PubDispatch 4k          100,000            403,314 msg/s      1.54 gb/s
PubDispatch 8k          100,000            203,320 msg/s      1.55 gb/s
ReqReply 0b              20,000              9,548 msg/s       0.00 b/s
ReqReply 8b              20,000              9,491 msg/s     74.15 kb/s
ReqReply 32b             10,000              9,778 msg/s    305.59 kb/s
ReqReply 256b            10,000              8,394 msg/s      2.05 mb/s
ReqReply 512b            10,000              8,259 msg/s      4.03 mb/s
ReqReply 1k              10,000              8,193 msg/s      8.00 mb/s
ReqReply 4k              10,000              7,915 msg/s     30.92 mb/s
ReqReply 8k              10,000              7,454 msg/s     58.24 mb/s
Latency 0b    5,000     35 /  49.20 / 134    +/- 0.77  (microseconds)
Latency 8b    5,000     35 /  49.54 / 361    +/- 0.80  (microseconds)
Latency 32b   5,000     35 /  49.27 / 135    +/- 0.79  (microseconds)
Latency 256b  5,000     41 /  56.41 / 142    +/- 0.90  (microseconds)
Latency 512b  5,000     40 /  56.41 / 174    +/- 0.91  (microseconds)
Latency 1k    5,000     35 /  49.76 / 160    +/- 0.80  (microseconds)
Latency 4k    5,000     36 /  50.64 / 193    +/- 0.83  (microseconds)
Latency 8k    5,000     38 /  55.45 / 206    +/- 0.88  (microseconds)
```

It is worth noting that in both cases memory was not a factor, the processor and OS were more of a consideration. To test this, take a look at the NatsBench results again. Those are run without any constraint on the Java heap and end up doubling the used memory. However, if we run the same test again with a constraint of 1Gb using -Xmx1g, the performance is comparable, differentiated primarily by "noise" that we can see between test runs with the same settings.

```AsciiDoc
Starting benchmark(s) [msgs=5000000, msgsize=256, pubs=2, subs=2]
Current memory usage is 976.38 mb / 981.50 mb / 981.50 mb free/total/max
Use ctrl-C to cancel.

Pub Only stats: 10,123,382 msgs/sec ~ 2.41 gb/sec
 [ 1] 5,068,256 msgs/sec ~ 1.21 gb/sec (2500000 msgs)
 [ 2] 5,061,691 msgs/sec ~ 1.21 gb/sec (2500000 msgs)
  min 5,061,691 | avg 5,064,973 | max 5,068,256 | stddev 3,282.50 msgs

Pub/Sub stats: 3,563,770 msgs/sec ~ 870.06 mb/sec
 Pub stats: 1,188,261 msgs/sec ~ 290.10 mb/sec
  [ 1] 594,701 msgs/sec ~ 145.19 mb/sec (2500000 msgs)
  [ 2] 594,130 msgs/sec ~ 145.05 mb/sec (2500000 msgs)
   min 594,130 | avg 594,415 | max 594,701 | stddev 285.50 msgs
 Sub stats: 2,375,839 msgs/sec ~ 580.04 mb/sec
  [ 1] 1,187,919 msgs/sec ~ 290.02 mb/sec (5000000 msgs)
  [ 2] 1,187,920 msgs/sec ~ 290.02 mb/sec (5000000 msgs)
   min 1,187,919 | avg 1,187,919 | max 1,187,920 | stddev .71 msgs


Final memory usage is 317.62 mb / 960.50 mb / 960.50 mb free/total/max
```

## Building From Source

The build depends on Gradle, and contains `gradlew` to simplify the process. After cloning, you can build the repository and run the tests with a single command:

```bash
> git clone https://github.com/nats-io/nats.java
> cd nats.java
> ./gradlew clean build
```

Or to build without tests

```bash
> ./gradlew clean build -x test
```

This will place the class files in a new `build` folder. To just build the jar:

```bash
> ./gradlew jar
```

The jar will be placed in `build/libs`.

You can also build the java doc, and the samples jar using:

```bash
> ./gradlew javadoc
> ./gradlew exampleJar
```

The java doc is located in `build/docs` and the example jar is in `build/libs`. Finally, to run the tests with the coverage report:

```bash
> ./gradlew test jacocoTestReport
```

which will create a folder called `build/reports/jacoco` containing the file `index.html` you can open and use to browse the coverage. Keep in mind we have focused on library test coverage, not coverage for the examples.

Many of the tests run nats-server on a custom port. If nats-server is in your path they should just work, but in cases where it is not, or an IDE running tests has issues with the path you can specify the nats-server location with the environment variable `nats_server_path`.

## License

Unless otherwise noted, the NATS source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.
