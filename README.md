# NATS - Java Client
A [Java](http://java.com) client for the [NATS messaging system](https://nats.io).

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fnats-io%2Fnats-java.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fnats-io%2Fnats-java?ref=badge_shield)
[![Build Status](https://travis-ci.org/nats-io/nats-java.svg?branch=master)](http://travis-ci.org/nats-io/nats-java)
[![Coverage Status](https://coveralls.io/repos/nats-io/nats-java/badge.svg?branch=master&service=github)](https://coveralls.io/github/nats-io/nats-java?branch=master)
[![Javadoc](http://javadoc.io/badge/io.nats/jnats.svg)](http://javadoc.io/doc/io.nats/jnats/2.0.0)


## Installation

The entire client is provided in a single jar file, with no external dependencies.

```bash
# Java client
maven, gradle, jar
tbd

# Server
go get github.com/nats-io/gnatsd
```

## Basic Usage

Sending and receiving with NATS is as simple as connecting to the gnatsd and publishing or subscribing for messages.

### Connecting

There are four different ways to connect using the Java library:

1. Connect to a local server on the default port:

    ```
    Connection nc = Nats.connect();
    ```

2. Connect to a server using a URL:

    ```
    Connection nc = Nats.connect("nats://myhost:4222");
    ```

3. Connect to one or more servers with a custom configuration:

    ```
    Options o = new Options.Builder().server("nats://serverone:4222").server("nats://servertwo:4222").maxReconnects(-1).build();
    Connection nc = Nats.connect(o);
    ```

    See the javadoc for a complete list of configuration options.

4. Connect asynchronously, this requires a callback to tell the application when the client is connected:

    ```
     Options options = new Options.Builder().server(Options.DEFAULT_URL).connectionListener(handler).build();
     Nats.connectAsynchronously(options, true);
    ```

    This feature is experimental, please let us know if you like it.

### Publishing

Once connected, publishing is accomplished via one of three methods:

1. With a subject and message body:

    ```
    nc.publish("subject", "hello world".getBytes(StandardCharsets.UTF_8));
    ```

2. With a subject and message body, as well as a subject for the receiver to reply to:

    ```
    nc.publish("subject", "replyto", "hello world".getBytes(StandardCharsets.UTF_8));
    ```

3. As a request that expects a reply. This method uses a Future to allow the application code to wait for the response. Under the covers 
a request/reply pair is the same as a publish/subscribe only the library manages the subscription for you.

    ```
    Future<Message> incoming = nc.request("subject", "hello world".getBytes(StandardCharsets.UTF_8));
    Message msg = incoming.get(500, TimeUnit.MILLISECONDS);
    String response = new String(msg.getData(), StandardCharsets.UTF_8);
    ```

All of these methods, as well as the incoming message code use byte arrays for maximum flexibility. Applications can
send JSON, Strings, YAML, Protocol Buffers, or any other format through NATS to applications written in a wide range of
languages.

### Listening for Incoming Messages

The Java NATS library provides two mechanisms to listen for messages, three if you include the request/reply discussed above.

1. Synchronous subscriptions where the application code manually asks for messages and blocks until they arrive. Each subscription
is associated with a single subject, although that subject can be a wildcard.

    ```
    Subscription sub = nc.subscribe("subject");
    Message msg = sub.nextMessage(Duration.ofMillis(500));

    String response = new String(msg.getData(), StandardCharsets.UTF_8);
    ```

2. A Dispatcher that will call application code in a background thread. Dispatchers can manage multiple subjects with a single thread
and single callback.

    ```
    Dispatcher d = nc.createDispatcher((msg) -> {
        String response = new String(msg.getData(), StandardCharsets.UTF_8);
        ...
    });

    d.subscribe("subject");
    ```

## Advanced Usage

### TLS

NATS supports TLS 1.2. The server can be configured to verify client certificates or not. Depending on this setting the client has several options.

1. The Java library allows the use of the tls:// protocol in its urls. This setting expects a default SSLContext to be set. You can set this default context using System properties, or in code. For example, you could run the publish example using:

    ```
    java -Djavax.net.ssl.keyStore=src/test/resources/keystore.jks -Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStore=src/test/resources/cacerts -Djavax.net.ssl.trustStorePassword=password io.nats.examples.NatsPub tls://localhost:4443 test "hello world"
    ```

    where the following properties are being set:

    ```
    -Djavax.net.ssl.keyStore=src/test/resources/keystore.jks
    -Djavax.net.ssl.keyStorePassword=password
    -Djavax.net.ssl.trustStore=src/test/resources/cacerts
    -Djavax.net.ssl.trustStorePassword=password
    ```

    This method can be used with or without client verification.

2. During development, or behind a firewall where the client can trust the server, the library supports the opentls:// protocol which will use a special
SSLContext that trusts all server certificates, but provides no client certificates.

    ```
    java io.nats.examples.NatsSub opentls://localhost:4443 test 3
    ```

    This method requires that client verification is off.

3. Your code can build an SSLContext to work with or without client verification.

    ```
    SSLContext ctx = createContext();
    Options options = new Options.Builder().server(ts.getURI()).sslContext(ctx).build();
    Connection nc = Nats.connect(options);
    ```

If you want to try out these techniques, there is a set tls configuration for the server in the test files that can be used to run gnatsd.

```
gnatsd --conf src/test/resources/tls.conf
```

### Clusters & Reconnecting

The Java client will automatically reconnect if it loses its connection the gnatsd. If given a single server, the client will keep trying that one. If given a list of servers, the client will rotate between them. When the gnatsd servers are in a cluster, they will tell the client about the other servers, so that in the simplest case a client could connect to one server, learn about the cluster and reconnect to another server if its initial one goes down.

Reconnection behavior is controlled via a few options, see the javadoc for the Options.Builder class for specifics on reconnect limits, delays and buffers.

## Benchmarking

The `io.nats.examples` package contains two benchmarking tools, modeled after tools in other NATS clients. Both examples run against an existing gnatsd. The first called `io.nats.examples.benchmark.NatsBench` runs two simple tests, the first simply publishes messages, the second also receives messages. Tests are run with 1 thread/connection per publisher or subscriber. Running on an iMac (2017), with 4.2 GHz Intel Core i7 and 64GB of memory produced results like:

```
Starting benchmark(s) [msgs=5000000, msgsize=256, pubs=2, subs=2]
Pub Only stats: 10,394,945 msgs/sec ~ 2.48 GB/sec
 [ 1] 5,441,683 msgs/sec ~ 1.30 GB/sec (2500000 msgs)
 [ 2] 5,197,476 msgs/sec ~ 1.24 GB/sec (2500000 msgs)
  min 5,197,476 | avg 5,319,579 | max 5,441,683 | stddev 122,103.50 msgs
Pub/Sub stats: 3,827,228 msgs/sec ~ 934.38 MB/sec
 Pub stats: 1,276,217 msgs/sec ~ 311.58 MB/sec
  [ 1] 638,543 msgs/sec ~ 155.89 MB/sec (2500000 msgs)
  [ 2] 638,108 msgs/sec ~ 155.79 MB/sec (2500000 msgs)
   min 638,108 | avg 638,325 | max 638,543 | stddev 217.50 msgs
 Sub stats: 2,551,453 msgs/sec ~ 622.91 MB/sec
  [ 1] 1,275,730 msgs/sec ~ 311.46 MB/sec (5000000 msgs)
  [ 2] 1,275,726 msgs/sec ~ 311.46 MB/sec (5000000 msgs)
   min 1,275,726 | avg 1,275,728 | max 1,275,730 | stddev 2.00 msgs
```

The second, called `io.nats.examples.autobench.NatsAutoBench` runs a series of tests with various message sizes. Running this test on the same iMac, resulted in:

```
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

It is worth noting that in both cases memory was not a factor, the processor and OS were more of a consideration.

## License

Unless otherwise noted, the NATS source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fnats-io%2Fnats-java.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fnats-io%2Fnats-java?ref=badge_large)