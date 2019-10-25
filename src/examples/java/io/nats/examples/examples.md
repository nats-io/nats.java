# Java Nats Examples

This folder contains a number of examples:

1. `NatsDispatch.java` - uses asynchronous dispatching for incoming messages
2. `NatsPub.java` - publishes a messages
3. `NatsQSub.java` - subscribes on a queue for message load balancing
4. `NatsReply.java` - sends a reply when a request is received
5. `NatsReq.java` - sends a request and prints the reply
6. `NatsSub.java` - reads messages synchronously
7. `autobench` - benchmarks the current system/setup in several scenarios
8. `benchmark` - benchmark that supports multiple threads
9. `stan` - A larger example that implements a server that can respond on multiple subjects, and several clients that send requests on those various subjects.
10. `stability` - a small producer and subscriber that run forever printing some status every so often. These are intended for long running tests without burning the CPU.

All of these examples take the server URL on the command line, which means that you can use the `tls` and `opentls` schemas to test over a secure connection.

## Running the examples

The examples require the client library and the examples to be compiled. See the [readme.md](/readme.md) for specifics on building these.

```bash
java -cp build/libs/jnats-2.0.0.jar:build/libs/jnats-examples-2.0.0.jar io.nats.examples.NatsPub nats://localhost:4222 test "hello world"
```

To see how queues split messages, run the `NatsQSub` in multiple windows and then run `NatsPub`. Messages should be distributed between the clients. On the other hand, if you run `NatsSub` in multiple shells and then run `NatsPub` you will get the message at all subscribers.

## Running with TLS

A set of sample certificates are provided in the repo for testing. These use the highly secure password `password`. To run with the full client and trust default keystore you can use command line arguments to set System properties.

```bash
java -Djavax.net.ssl.keyStore=src/test/resources/keystore.jks -Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStore=src/test/resources/truststore.jks -Djavax.net.ssl.trustStorePassword=password io.nats.examples.NatsPub tls://localhost:4443 test "hello world"
```

To run with the completely unverified client:

```bash
java -cp build/libs/jnats-2.0.0.jar:build/libs/jnats-examples-2.0.0.jar io.nats.examples.NatsSub opentls://localhost:4443 test 3
```

There are a set tls configuration for the server in the test files that can be used to run the NATS server.

```bash
nats-server --conf src/test/resources/tls.conf
```

As well as one with the verify flag set.

```bash
nats-server --conf src/test/resources/tlsverify.conf
```

which will require client certificates.