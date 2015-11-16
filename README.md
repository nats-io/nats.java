# NATS - Java client
A [Java](http://www.java.com) client for the [NATS messaging system](https://nats.io).

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![Build Status](https://travis-ci.org/nats-io/jnats.svg?branch=master)](http://travis-ci.org/nats-io/jnats)
[![Javadoc](http://javadoc-badge.appspot.com/com.github.nats-io/jnats.svg?label=javaadoc)](http://javadoc-badge.appspot.com/com.github.nats-io/jnats)
[![Coverage Status](https://coveralls.io/repos/nats-io/jnats/badge.svg?branch=master)](https://coveralls.io/r/nats-io/jnats?branch=master)

This is a WORK IN PROGRESS. 
Please refer to the TODO.md for more information on what things are not complete or not working.
Watch this space for more info as it becomes available.

## Installation

First, download the source code:
```
git clone git@github.com:nats-io/jnats.git .
```

To build the library, use [maven](https://maven.apache.org/). From the root directory of the project:

```
mvn clean install
```

or use the GUI to setup the way you want. When you are satified with the settings, simply invoke:

```
make
```

on Windows, you may do this for example:

```
cmake --build . --config "Release"
```

This is building the static and shared libraries and also the examples and the test program. Each are located in their respective directories under `build`: `src`, `examples` and `test`.

```
make install
```

Will copy both the static and shared libraries in the folder `install/lib` and the public headers in `install/include`.

You can list all the possible `make` options with the command:

```
make help
```

The most common you will use are:

* clean
* install
* test

On platforms where `valgrind` is available, you can run the tests with memory checks.
Here is an example:

```
make test ARGS="-T memcheck"
```

Or, you can invoke directly the `ctest` program:

```
ctest -T memcheck -V -I 1,4
```
The above command would run the tests with `valgrind` (`-T memcheck`), with verbose output (`-V`), and run the tests from 1 to 4 (`-I 1,4`).

If you add a test to `test/test.c`, you need to add it into the `allTests` array. Each entry contains a name, and the test function. You can add it anywhere into this array.
Build you changes:

```
$ make
[ 44%] Built target nats
[ 88%] Built target nats_static
[ 90%] Built target nats-publisher
[ 92%] Built target nats-queuegroup
[ 94%] Built target nats-replier
[ 96%] Built target nats-requestor
[ 98%] Built target nats-subscriber
Scanning dependencies of target testsuite
[100%] Building C object test/CMakeFiles/testsuite.dir/test.c.o
Linking C executable testsuite
[100%] Built target testsuite
```

Now regenerate the list by invoking the test suite without any argument:

```
$ ./test/testsuite
Number of tests: 77
```

This list the number of tests added to the file `list.txt`. Move this file to the source's test directory.

```
$ mv list.txt ../test/
```

Then, refresh the build:

```
$ cmake ..
-- Configuring done
-- Generating done
-- Build files have been written to: /home/ivan/cnats/build
```

You can use the following environment variables to influence the testsuite behavior.

When running with memory check, timing changes and overall performance is slower. The following variable allows the testsuite to adjust some of values used during the test:

```
NATS_TEST_VALGRIND=yes
```

When running the tests in verbose mode, the following environment variable allows you to see the server output from within the test itself. Without this option, the server output is silenced:

```
NATS_TEST_KEEP_SERVER_OUTPUT=yes
```

If you want to change the default server executable name (`gnastd`) or specify a specific location, use this environment variable:

```
NATS_TEST_SERVER_EXE=<full server executable path>

for instance:

NATS_TEST_SERVER_EXE=c:\test\gnatsd.exe
```
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

