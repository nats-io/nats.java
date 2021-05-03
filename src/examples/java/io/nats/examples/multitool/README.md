# JetStream Multi Tool Usage Notes

## Command Line Usage

Change into the nats.java directory, and then using gradle, build the source

```shell
gradle build -x test
``` 

Once the code is built, you can run the multi tool by adding the libraries to your java classpath
or the java command line, with the fully qualified name of the JsMulti program, the required parameters and the optional parameters.

```shell
java -cp build/libs/jnats-2.11.1.jar:build/libs/jnats-2.11.1-examples.jar io.nats.examples.jsmulti.JsMulti requireds [optionals]
```

## From your IDE

The JsMulti program has a `public static void main(String[] args)` method, so it's easy enough to run from any ide.

Most IDEs allow you to build a runtime configuration and pass command line arguments.
I like to just modify the code and set the arguments there. Check out the comments in the code's main method for examples.

## Arguments

For ease of reading, when providing numbers, since they are all integers, you can use java underscore format, US or European style. So these are all valid:

```
1000000
1_000_000
1,000,000
1.000.000
```

### Action Argument

`-a` The action to execute. Always required. One of the following (case ignored)

* `create` create the stream
* `delete` delete the stream
* `info` get the stream info
* `pubSync` publish synchronously
* `pubAsync` publish asynchronously
* `subPush` push subscribe read messages (synchronously)
* `subQueue` push subscribe read messages with queue (synchronously)
* `subPull` pull subscribe read messages (different durable if threaded)
* `subPullQueue` pull subscribe read messages (all with same durable)

### Server Argument

`-s` The url of the server. if not provided, will defaults to `nats://localhost:4222`

```java
... JsMulti -s localhost ...
... JsMulti -s nats://myhost:4444 ...
```

## Stream Management

The multi tool allows you to:

* `create` create the stream
* `delete` delete the stream
* `info` get the stream info

(You can do all three of these functions with our [natscli](https://github.com/nats-io/natscli) command line tool.)

### Required Argument

`-t` The name of the stream

### Optional Arguments

`-o` file|memory when creating the stream, default to memory

`-c` replicas when creating the stream, default to 1. Make sure you are running a cluster!

#### Examples

Create a stream called `multistream` with memory based storage

```java
... JsMulti -a create -t multistream
```

Create a stream called `multistream` with file based storage and replication factor of 3

```java
... JsMulti -a create -o file -c 3 -t multistream
```

Delete a stream

```java
... JsMulti -a delete -t multistream
```

Get info for a stream.

```java
... JsMulti -a info -t multistream

StreamInfo{
    created=2021-05-03T19:55:06.901279100Z[GMT]
    StreamConfiguration{
        name='multistream'
        subjects=[multisubject]
        retentionPolicy=limits
        maxConsumers=-1
        maxMsgs=-1
        maxBytes=-1
        maxAge=PT0S
        maxMsgSize=-1
        storageType=memory
        replicas=1
        noAck=false
        template='null'
        discardPolicy=old
        duplicateWindow=PT2M
        mirror=null
        placement=null
        sources=[]
    }
    StreamState{
        msgs=0
        bytes=0
        firstSeq=0
        lastSeq=0
        consumerCount=0
        firstTime=0001-01-01T00:00Z[GMT]
        lastTime=0001-01-01T00:00Z[GMT]
    }
    cluster=null
    mirror=null
    sources=null
}
```

## Publishing and Subscribing

Publishing and subscribing have some options in common.

### Required Arguments

`-u` The subject for publishing or subscribing

### Optional Arguments

`-m` the total number of messages to publish or subscribe (consuming). Defaults to 1,000,000

`-n shared` When running with more than 1 thread, only connect to the server once and share the connection among all threads. This is the default.

`-n individual` When running with more than 1 thread, each thread will make it's own connection to the server.

`-d` Number of threads to use for publishing or subscribe (consuming). Defaults to 1 thread.

`-j` Jitter between publishing or subscribe (consuming), in milliseconds, defaults to 0.
A random amount of time from 0 up to the number of milliseconds to sleep between publishing or requesting the next message while subscribing. Intended to help simulate a more real world scenario.

### Publish Only Optional Arguments

`-p` For publishing only, the payload size (the number of data bytes) to publish. Defaults to 128 bytes

`-z` Round size for `pubAsync` action, default to 100.
Publishing asynchronously uses the "sawtooth" pattern. This means we publish a round of messages, collecting all the futures that receive the PublishAck
untill we reach the round size. At that point we process all the futures we have collected, then we start over until we have published all the messages.

### Subscribe (Consume) Only Optional Arguments

`-k explicit` Explicit ack policy. Acknowledge each message received. This is the default.

`-k none` None ack policy. Configures the consumer to not have to ack messages.

`-z` Batch size for `subPull` or `subPullQueue` actions, default to 100. Pull subscriptions work in batches, maximum of 256

## Publish Examples

#### synchronous processing of the acknowledgement

* publish to `multisubject`
* 1 thread (default)
* 1 million messages (default)
* payload size of 128 bytes (default)
* shared connection (default)

```java
... JsMulti -a pubSync -u multisubject
```

#### synchronous processing of the acknowledgement

* publish to `multisubject`
* 5 threads
* 5 million messages
* payload size of 256 bytes
* jitter of 1 second (1000 milliseconds)

```java
... JsMulti -a pubSync -u multisubject -d 5 -m 5_000_000 -p 256 -j 1000
```

#### asynchronous processing of the acknowledgement

* publish to `multisubject`
* 2 threads
* 2 million messages
* payload size of 512 bytes
* round size of 20
* individual connections to the server

```java
... JsMulti -a pubAsync -u multisubject -d 2 -m 2_000_000 -p 512 -z 20 -n individual
```

## Subscribe Examples

####  push subscribe

* from `multisubject`
* 1 thread (default)
* 1 million messages (default)
* explicit ack (default)

```java
... JsMulti -a subPush -u multisubject
```

####  push subscribe to a queue

> IMPORTANT subscribing (push or pull) with a queue requires multiple threads

* from `multisubject`
* 2 threads
* 1 million messages (default)
* explicit ack (default)

```java
... JsMulti -a subQueue -u multisubject -d 2
```

####  pull subscribe

* from `multisubject`
* 2 threads
* 1 million messages (default)
* ack none
* batch size 20 (default is 100, max is 256)

```java
... JsMulti -a subQueue -u multisubject -d 2 -k none -z 20
```
