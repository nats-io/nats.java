# Java Nats Examples

This folder contains a number of examples:

### Regular Nats Examples
1. `ConnectTime.java` - how to build a listener that will track connect time.
1. `NatsPub.java` - publishes messages
1. `NatsPubMany.java` - publishes many messages
1. `NatsReply.java` - sends a reply when a request is received
1. `NatsReq.java` - sends a request and prints the reply
1. `NatsReqFuture.java` - sends a request async using a future 
1. `NatsSub.java` - reads messages synchronously
1. `NatsSubDispatch.java` - uses asynchronous dispatching for incoming messages
1. `NatsSubQueue.java` - subscribes on a queue for message load balancing
1. `NatsSubQueueFull.java` - fuller publish and queue subscribe example 
   
### JetStream Examples
1. `NatsJsPub.java` - publish JetStream messages
1. `NatsJsPubVsRegularPub.java` - publish JetStream messages versus regular publish to the same stream.
1. `NatsJsPullSub.java` - demonstrate a JetStream pull subscription
1. `NatsJsPullSubWithExpireFuture.java` - demonstrate a JetStream pull subscription with a expiration
1. `NatsJsPullSubWithExpireImmediately.java` - demonstrate a JetStream pull subscription with an immediate expiration
1. `NatsJsPullSubWithNoWait.java` - demonstrate a JetStream pull subscription with the no wait option
1. `NatsJsPushSub.java` - demonstrate JetStream push subscribing to read messages synchronously and manually acknowledges messages.
1. `NatsJsPushSubQueue.java` - demonstrate JetStream push subscribing to read messages in a load balance queue.
1. `NatsJsPushSubWithHandler.java` - demonstrate JetStream push subscribing to read messages asynchronously and manually acknowledges messages.
1. `NatsJsUtils.java` - utilities used by examples for reuse such as creating streams and publishing 

### JetStream Management / Admin Examples
1. `NatsJsManageConsumers.java` - demonstrate the management of consumers
1. `NatsJsManageStreams.java` - demonstrate the management of streams

### Other examples
1. `autobench` - benchmarks the current system/setup in several scenarios
1. `benchmark` - benchmark that supports multiple threads
1. `stan` - A larger example that implements a server that can respond on multiple subjects, and several clients that send requests on those various subjects.
1. `stability` - a small producer and subscriber that run forever printing some status every so often. These are intended for long running tests without burning the CPU.

All of these examples take the server URL on the command line, which means that you can use the `tls` and `opentls` schemas to test over a secure connection.

## Running the examples

The examples require the client library and the examples to be compiled. See the [readme.md](/README.md) for specifics on building these.

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
