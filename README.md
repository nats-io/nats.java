# NATS - Java client
A [Java](http://www.java.com) client for the [NATS messaging system](https://nats.io).

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![Build Status](https://travis-ci.org/nats-io/jnats.svg?branch=master)](http://travis-ci.org/nats-io/jnats)
[![Javadoc](http://javadoc-badge.appspot.com/com.github.nats-io/jnats.svg?label=javaadoc)](http://javadoc-badge.appspot.com/com.github.nats-io/jnats)
[![Coverage Status](https://coveralls.io/repos/nats-io/jnats/badge.svg?branch=master)](https://coveralls.io/r/nats-io/jnats?branch=master)

This is a WORK IN PROGRESS. It's not quite a stable release. 
Please refer to the TODO.md for constantly updating information on what things are not complete or not working.
Watch this space for more info as it becomes available.

## Installation

First, download the source code:
```
git clone git@github.com:nats-io/jnats.git .
```

To build the library, use [maven](https://maven.apache.org/). From the root directory of the project:

```
mvn clean install -DskipTests
```

This will build the jnats-0.0.1-SNAPSHOT.tar.gz in /target.

This compressed archive contains the jnats client jar, slf4j dependencies, and this README.

## Basic Usage

```java

ConnectionFactory cf = new ConnectionFactory(Cosntants.DEFAULT_URL)
Connection nc = cf.createConnection();

// Simple Publisher
nc.publish("foo", "Hello World");

// Simple Async Subscriber
nc.subscribe("foo", new MessageHandler(Msg m) {
    fmt.Printf("Received a message: %s\n", string(m.Data))
});

// Simple Sync Subscriber
Subscription sub = nc.subscribeSync("foo");
Message m = sub.nextMsg(timeout);

// Unsubscribing
sub = nc.subscribe("foo");
sub.unsubscribe();

// Requests
msg = nc.request("help", "help me", TimeUnit.SECONDS.toMillis(10));

// Replies
nc.subscribe("help", new MessageHandler (Message m) {
    nc.publish(m.getReplyTo(), "I can help!")
});

// Close connection
nc = nats.Connect("nats://localhost:4222")
nc.Close();
```

## Wildcard Subscriptions

```java

// "*" matches any token, at any level of the subject.
nc.subscribe("foo.*.baz", new MessageHandler(Message m) {
    System.out.printf("Msg received on [%s] : %s\n", m.getSubject(), new String(getData()));
})

nc.subscribe("foo.bar.*", new MessageHandler(Message m) {
    System.out.printf("Msg received on [%s] : %s\n", m.getSubject(), new String(getData()));
})

// ">" matches any length of the tail of a subject, and can only be the last token
// E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
nc.Subscribe("foo.>", new MessageHandler(Message m) {
    System.out.printf("Msg received on [%s] : %s\n", m.getSubject(), new String(getData()));
})

// Matches all of the above
nc.publish("foo.bar.baz", "Hello World")

```

## Queue Groups

```java
// All subscriptions with the same queue name will form a queue group.
// Each message will be delivered to only one subscriber per queue group,
// using queuing semantics. You can have as many queue groups as you wish.
// Normal subscribers will continue to work as expected.

nc.queueSubscribe("foo", "job_workers", new MessageHandler() {
  long received += 1;
});

```

## Advanced Usage

```java

// Flush connection to server, returns when all messages have been processed.
nc.flush()
System.out.println("All clear!");

// FlushTimeout specifies a timeout value as well.
boolean flushed = nc.flush(1, TimeUnit.SECONDS);
if (flushed) {
    System.out.println("All clear!");
} else {
    System.out.println("Flushed timed out!");
}

// Auto-unsubscribe after MAX_WANTED messages received
final static int MAX_WANTED = 10;
sub = nc.subscribe("foo");
sub.autoUnsubscribe(MAX_WANTED);

// Multiple connections
nc1 = nats.Connect("nats://host1:4222")
nc2 = nats.Connect("nats://host2:4222")

nc1.subscribe("foo", func(m *Msg) {
    System.out.printf("Received a message: %s\n", new String(m.getData()));
})

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
opts = nats.DefaultOptions;
opts.setServers(servers);

// Optionally set ReconnectWait and MaxReconnect attempts.
// This example means 10 seconds total per backend.
opts.setMaxReconnect(5);
opts.setReconnectWait(2, TimeUnit.SECONDS);

// Optionally disable randomization of the server pool
opts.setNoRandomize(true);

nc = opts.Connect()

// Setup callbacks to be notified on disconnects and reconnects
nc.setDisconnectedCB(new ConnEventHandler(Connection cc) {
    System.out.printf("Got disconnected!\n")
});

// See who we are connected to on reconnect.
nc.setReconnectedCB(new ConnEventHandler(Connection c) {
    System.out.printf("Got reconnected to %v!\n", c.getConnectedUrl())
});

```


## License

(The MIT License)

Copyright (c) 2012-2015 Apcera Inc.

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

