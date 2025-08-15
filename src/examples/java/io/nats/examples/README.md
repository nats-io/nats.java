![NATS](../../../../../../src/main/javadoc/images/large-logo.png)

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
In the `io.nats.examples.jetstream` package...

1. `NatsJsPub.java` - publish JetStream messages
1. `NatsJsPubAsync.java` - publish JetStream messages asynchronously
1. `NatsJsPubAsync2.java` - publish JetStream messages asynchronously
1. `NatsJsPubVersusCorePub.java` - publish JetStream messages versus core publish to the same stream.
1. `NatsJsPubWithOptionsUseCases` - publish JetStream with examples on using publish options
1. `NatsJsPullSubBatchSize.java` - pull subscription example specifying batch size and manual handling
1. `NatsJsPullSubBatchSizeUseCases.java` - pull subscription example specifying batch size with examples of manual handling various cases of available messages  
1. `NatsJsPullSubExpiresIn.java` - pull subscription example specifying expiration and manual handling
1. `NatsJsPullSubExpiresInUseCases.java` - pull subscription example specifying expiration with examples of manual handling various cases of available messages
1. `NatsJsPullSubFetch.java` - pull subscription example using fetch list macro function
1. `NatsJsPullSubFetchUseCases.java` - pull subscription example using fetch list macro function with examples of various cases of available messages
1. `NatsJsPullSubIterate.java` - pull subscription example using iterate macro function
1. `NatsJsPullSubIterateUseCases.java` - pull subscription example using iterate macro function with examples of various cases of available messages
1. `NatsJsPullSubMultipleWorkers.java` - pull subscribing using a durable consumer and sharing processing of the messages
1. `NatsJsPullSubNoWaitUseCases.java` - pull subscription example specifying no wait with examples of manual handling various cases of available messages
1. `NatsJsPushSubAsyncQueueDurable.java` - push async subscribing using a durable consumer and a queue
1. `NatsJsPushSubBasicAsync.java` - basic asynchronous push subscribing with a handler
1. `NatsJsPushSubBasicSync.java` - push subscribing to read messages synchronously and manually acknowledges messages
1. `NatsJsPushSubBindDurable.java` - push subscribing with the bind options
1. `NatsJsPushSubDeliverSubject.java` - push subscribing with a deliver subject and how the subject can be read as a regular Nats Message  
1. `NatsJsPushSubFilterSubject.java` - push subscribing with a filter on the subjects
1. `NatsJsPushSubFlowControl.java` - listening to flow control messages through the Error Listener
1. `NatsJsPushSubQueueDurable.java` - push subscribing to read messages in a load balance queue using a durable consumer

### JetStream Management / Admin Examples
1. `NatsJsManageConsumers.java` - demonstrate the management of consumers
1. `NatsJsManageStreams.java` - demonstrate the management of streams
1. `NatsJsMirrorSubUseCases.java` -
1. `NatsJsPrefix.java` - demonstrate connecting on an account that uses a custom prefix

### Key Value / Object Store
1. `NatsKeyValueFull.java` -

### Other examples
1. `autobench` - benchmarks the current system/setup in several scenarios
1. `benchmark` - benchmark that supports multiple threads
1. `stability` - a small producer and subscriber that run forever printing some status every so often. These are intended for long running tests without burning the CPU.
1. `jsmulti` - a multi-faceted tool that allows you to do a variety of publishing and subscribing to both core NATS and JetStream subjects. Please see the [JsMultiTool README](jsmulti/README.md) 

### Example Support
1. `ExampleArgs.java` - Helper to manage command line arguments.
1. `ExampleAuthHandler.java` - Example of an auth handler.
1. `ExampleUtils.java` - Miscellaneous utils used to start or in running examples.
1. `NatsJsUtils.java` - Miscellaneous utils specific to JetStream examples.

All of these examples take the server URL on the command line, which means that you can use the `tls` and `opentls` schemas to test over a secure connection.

## Running the examples

* The examples require both the client library and the examples to be compiled and then the jars be used in the classpath.
When you build locally, `-SNAPSHOT` is appended to the version.
See the project [README](/README.md) for specifics on building these.

* When you run, if you supply an unknown parameter or miss a required parameter, the usage string will be shown and the program will exit.

* If you purposefully want to see the usage string run the program with `-h`, `-help`, `-u`, `-usage`.

* All examples require a server url, but it's always optional to provide one as part of the command.
If not supplied, the program will use `nats://localhost:4222`

* If you want to run the program from an ide, you can take advantage of the ide runner to provide arguments.
You can also just insert some code before the arguments are processed to set the arguments directly. For example;
    ```java
    args = "-arg1 myArg1 -arg2 myArg2".split(" ");
    args = new String[] {"-arg1", "myArg1", "-arg2", "myArg2"};
    ```

### Classpath 

In the examples, the usage will show `java <program> -cp <classpath> ...` Make sure you add both the client library and examples into the classpath.
Replace `{major.minor.patch}` with the correct version.
For example:

```bash
java -cp build/libs/jnats-{major.minor.patch}-SNAPSHOT.jar:build/libs/jnats-{major.minor.patch}-SNAPSHOT-examples.jar io.nats.examples.NatsPub nats://localhost:4222 test "hello world"
```

### Some examples depend on others

To see how queues split messages, run the `NatsQSub` in multiple windows and then run `NatsPub`. Messages should be distributed between the clients. On the other hand, if you run `NatsSub` in multiple shells and then run `NatsPub` you will get the message at all subscribers.

## Running with TLS

A set of sample certificates are provided in the repo for testing. These use the highly secure password `password`. To run with the full client and trust default keystore you can use command line arguments to set System properties.

```bash
java -Djavax.net.ssl.keyStore=src/test/resources/keystore.jks -Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStore=src/test/resources/truststore.jks -Djavax.net.ssl.trustStorePassword=password io.nats.examples.NatsPub tls://localhost:4443 test "hello world"
```

To run with the completely unverified client:

```bash
java -cp build/libs/jnats-{major.minor.patch}-SNAPSHOT.jar:build/libs/jnats-{major.minor.patch}-SNAPSHOT-examples.jar io.nats.examples.NatsSub opentls://localhost:4443 test 3
```

There are a set tls configuration for the server in the test files that can be used to run the NATS server.

```bash
nats-server --config src/test/resources/tls.conf
```

As well as one with the verify flag set.

```bash
nats-server --config src/test/resources/tlsverify.conf
```

which will require client certificates.
