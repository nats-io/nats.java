![NATS](../../../../../../../src/main/javadoc/images/large-logo.png)

# JetStream Multi Tool

## About this Tooling

This is a tool to provide complete exercising and benchmarking specific to the NATS Java client with the intent of establishing a baseline for client performance and behavior.

Administration and server related benchmarking of NATS should prefer the [NATS CLI](https://docs.nats.io/nats-tools/natscli) tooling, especially in production environments.

### Running from the Command Line

Change into the nats.java directory, and then using gradle, build the source:

```shell
gradle build -x test
``` 

Once the code is built, you can run the multi tool by adding the libraries to your java classpath
or the java command line, with the fully qualified name of the JsMulti program, the required parameters and the optional parameters.

```shell
java -cp build/libs/jnats-2.11.5.jar:build/libs/jnats-2.11.5-examples.jar io.nats.examples.jsmulti.JsMulti <configuration options>
```

### Running from an IDE

The JsMulti program has a `main` method so it's easy enough to run from any ide.
You can also call it from another program using the `public static void run(Arguments a)` method.
We have provided an `ArgumentBuilder` class to help build configurations.
You could also use your IDE and build a runtime configuration that passes command line arguments.

The examples below show both command line and builder examples.

### Number Arguments

Number arguments are all integers. When providing numbers on the command line, you can use underscore `_`, comma `,` or period `.`
to make it more readable. So these are all valid for 1 million `1000000`, `1,000,000`, `1.000.000` or `1_000_000`

### Action Argument

`-a` The action to execute. Always required. One of the following (case ignored)

* `pubSync` publish synchronously
* `pubAsync` publish asynchronously
* `pubCore` publish synchronously using the core api
* `subPush` push subscribe read messages (synchronously)
* `subQueue` push subscribe read messages with queue (synchronously)
* `subPull` pull subscribe read messages (different durable if threaded)
* `subPullQueue` pull subscribe read messages (all with same durable)

#### Using the builder

The builder was created to give another way to build configurations if you typically run from an IDE. 
You could use the builder and modify the `JsMulti` class or just create your own classes that make it easier to run from the command line.

The actions all have smart builder creation methods...

```java
Arguments a = ArgumentBuilder.pubSync("subject-name") ... .build();
Arguments a = ArgumentBuilder.pubAsync("subject-name") ... .build();
Arguments a = ArgumentBuilder.pubCore("subject-name") ... .build();
Arguments a = ArgumentBuilder.subPush("subject-name") ... .build();
Arguments a = ArgumentBuilder.subQueue("subject-name") ... .build();
Arguments a = ArgumentBuilder.subPull("subject-name") ... .build();
Arguments a = ArgumentBuilder.subPull("subject-name") ... .build();
Arguments a = ArgumentBuilder.subPullQueue("subject-name") ... .build();
```

You could build your own custom class to run from the command line:

```java
public class MyMulti {
    public static void main(String[] args) throws Exception {
        Arguments a = ArgumentBuilder.pubSync("subject-name") ... .build();
        JsMulti.run(new Arguments(args));
    }
}
```

### Server Argument

`-s` The url of the server. if not provided, will default to `nats://localhost:4222`

_Command Line_

```shell
... JsMulti ... -s nats://myhost:4444 ...
```

_Builder_

```java
Arguments a = ArgumentBuilder ... .server("nats://myhost:4444") ...
```

### Report Frequency

`-rf` report frequency (number) how often to print progress, defaults to 1000 messages. <= 0 for no reporting. 
Reporting time is excluded from timings.

_Command Line_

```shell
... JsMulti ... -rf 5000 ...
... JsMulti ... -rf -1 ...
```

_Builder_

```java
Arguments a = ArgumentBuilder ... .reportFrequency(5000) ...
Arguments a = ArgumentBuilder ... .noReporting() ...
```

## Publishing and Subscribing

Publishing and subscribing have some options in common.

### Required Arguments

`-u` subject (string), required for publishing or subscribing

### Optional Arguments

`-m` message count (number) for publishing or subscribing, defaults to 1 million

`-d` threads (number) for publishing or subscribing, defaults to 1

`-n` connection strategy (shared|individual) when threading, whether to share
* `shared` When running with more than 1 thread, only connect to the server once and share the connection among all threads. This is the default.
* `individual` When running with more than 1 thread, each thread will make its own connection to the server.

`-j` jitter (number) between publishes or subscribe message retrieval of random 
number from 0 to j-1, in milliseconds, defaults to 0 (no jitter), maximum 10_000
time spent in jitter is excluded from timings

```shell
... JsMulti ... -u subject-name -m 1_000_000 -d 4 -n shared -j 1500 
... JsMulti ... -u subject-name -d 4 -j 1500 
... JsMulti ... -u subject-name -m 2_000_000 -d 4 -n individual -j 1500 
```


### Publish Only Optional Arguments

`-ps` payload size (number) for publishing, defaults to 128, maximum 8192"

`-rs` round size (number) for pubAsync, default to 100, maximum 1000.
Publishing asynchronously uses the "sawtooth" pattern. This means we publish a round of messages, collecting all the futures that receive the PublishAck
until we reach the round size. At that point we process all the futures we have collected, then we start over until we have published all the messages. 

```shell
... JsMulti ... -ps 512 -rs 50 
```

> In real applications there are other algorithms one might use. For instance, you could optionally set up a separate thread to process the Publish Ack. 
This would require some concurrent mechanism like the `java.util.concurrent.LinkedBlockingQueue`
Publishing would run on one thread and place the futures in the queue. The second thread would
be pulling from the queue.

### Subscribe Push or Pull Optional Arguments

`-kp` ack policy (explicit|none|all) for subscriptions. Ack Policy must be `explicit` on pull subscriptions.

* `explicit` Explicit Ack Policy. Acknowledge each message received. This is the default.
* `none` None Ack Policy. Configures the consumer to not have to ack messages.
* `all` All Ack Policy. Configures the consumer to ack with one message for all the previous messages.

`-kf` ack frequency (number), applies to Ack Policy all, ack after kf messages, defaults to 1, maximum 256. 
For Ack Policy `explicit`, all messages will be acked after kf number of messages are received. 
For Ack Policy `all`, the last message will be acked once kf number of messages are received.
Does not apply to Ack Policy `none` 

### Subscribe Pull Optional Arguments

`-pt` pull type (fetch|iterate) defaults to iterate

`-bs` batch size (number) for subPull/subPullQueue, defaults to 10, maximum 256

```shell
... JsMulti ... -pt fetch -kp explicit -kf 100 -bs 50 
... JsMulti ... -pt iterate -kp explicit -kf 100 -bs 50 
... JsMulti ... -kp all -kf 100 
... JsMulti ... -kp none 
```

## Publish Examples

#### Synchronous processing of the acknowledgement

* publish to `subject-name`
* 1 thread (default)
* 1 million messages (default)
* payload size of 128 bytes (default)
* shared connection (default)

_Command Line_

```shell
... JsMulti -a pubSync -u subject-name
```

_Builder_

```java
Arguments a = ArgumentBuilder.pubSync("subject-name").build();
```

#### Synchronous processing of the acknowledgement

* publish to `subject-name`
* 5 million messages
* 5 threads
* payload size of 256 bytes
* jitter of 1 second (1000 milliseconds)

_Command Line_

```shell
... JsMulti -a pubSync -u subject-name -d 5 -m 5_000_000 -p 256 -j 1000
```

_Builder_

```java
Arguments a = ArgumentBuilder.pubSync("subject-name")
    .messageCount(5_000_000)
    .threads(5)
    .jitter(1000)
    .payloadSize(256)
    .build();
```

#### Asynchronous processing of the acknowledgement

* publish to `subject-name`
* 2 million messages
* 2 threads
* payload size of 512 bytes
* shared/individual connections to the server

_Command Line_

```shell
... JsMulti -a pubAsync -u subject-name -m 2_000_000 -d 2 -p 512 -n shared
... JsMulti -a pubAsync -u subject-name -m 2_000_000 -d 2 -p 512 -n individual
```

_Builder_

```java
Arguments a = ArgumentBuilder.pubSync("subject-name")
    .messageCount(2_000_000)
    .threads(2)
    .payloadSize(512)
    .sharedConnection()
    .build();

Arguments a = ArgumentBuilder.pubSync("subject-name")
    .messageCount(2_000_000)
    .threads(2)
    .payloadSize(512)
    .individualConnection()
    .build();
```

#### Synchronous core style publishing

* publish to `subject-name`
* 2 million messages
* 2 threads
* payload size of 512 bytes
* individual connections to the server

_Command Line_

```shell
... JsMulti -a pubCore -u subject-name -m 2_000_000 -d 2 -p 512 -n individual
```

_Builder_

```java
Arguments a = ArgumentBuilder.pubCore("subject-name")
    .messageCount(2_000_000)
    .threads(2)
    .payloadSize(512)
    .individual()    
    .build();
```

## Subscribe Examples

#### Push subscribe

* from 'subject-name'
* 1 thread (default)
* 1 million messages (default)
* explicit ack (default)

_Command Line_

```shell
... JsMulti -a subPush -u subject-name
```

_Builder_

```java
Arguments a = ArgumentBuilder.subPush("subject-name").build();
```

#### Push subscribe with a Queue

> IMPORTANT subscribing (push or pull) with a queue requires multiple threads

* from 'subject-name'
* 2 threads
* 1 million messages (default)
* ack none

_Command Line_

```shell
... JsMulti -a subQueue -u subject-name -d 2 -kp none
```

_Builder_

```java
Arguments a = ArgumentBuilder.subQueue("subject-name")
    .threads(2)
    .ackNone()
    .build();
```

#### Pull subscribe iterate

* from 'subject-name'
* 2 threads
* 1 million messages (default)
* iterate pull type (default)
* batch size 20 (default is 100, max is 256)

_Command Line_

```shell
... JsMulti -a subPull -u subject-name -d 2 -bs 20
```

_Builder_

```java
Arguments a = ArgumentBuilder.subPull("subject-name")
        .threads(2)
        .batchSize(20)
        .build();
```

#### Pull subscribe fetch

* from 'subject-name'
* 2 threads
* 1 million messages (default)
* fetch pull type
* batch size 20 (default is 100, max is 256)

_Command Line_

```shell
... JsMulti -a subPull -u subject-name -d 2 -pt fetch -bs 20
```

_Builder_

```java
Arguments a = ArgumentBuilder.subPull("subject-name")
    .threads(2)
    .fetch()    
    .batchSize(20)
    .build();
```
