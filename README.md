# NATS - Java client
A [Java](http://www.java.com) client for the [NATS messaging system](https://nats.io).

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![Build Status](https://travis-ci.org/nats-io/jnats.svg?branch=master)](http://travis-ci.org/nats-io/jnats)
[![Javadoc](http://javadoc-badge.appspot.com/com.github.nats-io/jnats.svg?label=javadoc)](http://javadoc-badge.appspot.com/com.github.nats-io/jnats)
[![Coverage Status](https://coveralls.io/repos/nats-io/jnats/badge.svg?branch=master&service=github)](https://coveralls.io/github/nats-io/jnats?branch=master)

This is a WORK IN PROGRESS. 
Test coverage is roughly equivalent to the Go client.
Documentation (javadoc) is in progress. 

## TODO

- [x] TLSv1.2 support (requires gnatsd v0.7.0)
- [ ] EncodedConnection support w/protobuf
- [ ] Maven Central
- [ ] Complete Javadoc
- [ ] More test coverage
- [ ] More TLS test cases

## Installation

First, download the source code:
```
git clone git@github.com:nats-io/jnats.git .
```

To build the library, use [maven](https://maven.apache.org/). From the root directory of the project:

```
mvn clean install
```

This will build the jnats-0.3.0-alpha.tar.gz in /target.

This compressed archive contains the jnats client jar, slf4j dependencies, and this README.

## Basic Usage

```java

ConnectionFactory cf = new ConnectionFactory(Constants.DEFAULT_URL)
Connection nc = cf.createConnection();

// Simple Publisher
nc.publish("foo", "Hello World");

// Simple Async Subscriber
nc.subscribe("foo", new MessageHandler() {
	@Override
	public void onMessage(Message m) {
    	System.out.println("Received a message: %s\n", string(m.Data));
    }
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
nc.subscribe("help", new MessageHandler() {
	@Override
	public void onMessage(Message m) {
    	nc.publish(m.getReplyTo(), "I can help!");
    }
});

// Close connection
nc = nats.Connect("nats://localhost:4222")
nc.close();
```

## Wildcard Subscriptions

```java

// "*" matches any token, at any level of the subject.
nc.subscribe("foo.*.baz", new MessageHandler() {
	@Override
	public void onMessage(Message m) {
    	System.out.printf("Msg received on [%s] : %s\n", m.getSubject(), new String(m.getData()));
    }
});

nc.subscribe("foo.bar.*", new MessageHandler() {
	@Override
	public void onMessage(Message m) {
    	System.out.printf("Msg received on [%s] : %s\n", m.getSubject(), new String(m.getData()));
    }
})

// ">" matches any length of the tail of a subject, and can only be the last token
// E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
nc.Subscribe("foo.>", new MessageHandler() {
	@Override
	public void onMessage(Message m) {
    	System.out.printf("Msg received on [%s] : %s\n", m.getSubject(), new String(m.getData()));
    }
})

// Matches all of the above
nc.publish("foo.bar.baz", "Hello World");

```

## Queue Groups

```java
// All subscriptions with the same queue name will form a queue group.
// Each message will be delivered to only one subscriber per queue group,
// using queuing semantics. You can have as many queue groups as you wish.
// Normal subscribers will continue to work as expected.

nc.queueSubscribe("foo", "job_workers", new MessageHandler() {
	@Override
	public void onMessage(Message m) {
  		long received += 1;
  	}
});

```

## Advanced Usage

```java

// Flush connection to server, returns when all messages have been processed.
nc.flush()
System.out.println("All clear!");

// FlushTimeout specifies a timeout value as well.
boolean flushed = nc.flush(1000);
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
nc1 = cf.createConnection("nats://host1:4222");
nc2 = cf.createConnection("nats://host2:4222");

nc1.subscribe("foo", new MessageHandler() {
	@Override
	public void onMessage(Message m) {
    	System.out.printf("Received a message: %s\n", new String(m.getData()));
    }
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
nc.setDisconnectedCB(new DisconnectedEventHandler() {
	@Override
	public void onDisconnect(ConnectionEvent event) {
    	System.out.printf("Got disconnected!\n")
    }
});

// See who we are connected to on reconnect.
nc.setReconnectedCB(new ReconnectedEventHandler() {
	@Override
	public void onReconnect(ConnectionEvent event) {
	    System.out.printf("Got reconnected to %s!\n", event.getConnectedUrl())
    }
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

