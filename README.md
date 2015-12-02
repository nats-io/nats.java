A Java client for [NATS](http://nats.io/).
=========================================

# Overview

This packages requires maven to build (along with various dependencies that maven will take care of). At runtime, netty is also required. The package is allowed to allow both client and server based NATS applications to be built easily, with an expected performance profile all in a productive manner...

## Usage

To use the client libraries, the following needs to be in your pom.xml:

```xml
<dependency>
  <groupId>io.nats</groupId>
  <artifactId>nats-java</artifactId>
  <version>0.1.1</version>
</dependency>
```

## Building and Examples

We distribute a maven-supported build (obviously). Typical commands work...

```sh
$ mvn compile
# or
$ mvn package
# etc
# Some of the examples of interest can be run as follows (pass -h for help).
# To run tools/examples/etc...
$ mvn exec:java -Dexec.mainClass=io.nats.tools.Sniffer
$ mvn exec:java -Dexec.mainClass=io.nats.tools.Publisher
$ mvn exec:java -Dexec.mainClass=io.nats.tools.RequestResponse
```

# APIs
The NATS APIs are in flux. We intend to offer a standardized, open API that is small, easy to embed in micro-editions and has the API architecture and flexibility to scale for more complex usage patterns.

With that in mind we offer a number of APIs in this package, feed back is welcome:

- _Public API_. This is contained within _io.nats__. All classes and interfaces here are public and intended to be the _standardized_ NATS API. We hope that other NATS client framework providers will implement this API with the factory pattern here so that clients and users of NATS do not need to change their applications to use  a different framework provider.

- [JMS API](https://en.wikipedia.org/wiki/Java_Message_Service). We indent to offer a 1.1 and simplified 1.2 JMS API that offers full JMS capabilities for the types of messaging which NATS provides.

- _Server API_. We believe that many messaging-based applications will have critical security, latency, memory, throughput and other constraints that do not allow _out of the box_ solutions to be met. This API builds upon the patterns within _netty_ to allow super-scale NATS solutions to be built.

- _Encoding API_. Many of the previous NATS Java clients supported simple byte arrays as message payloads. This package builds upon the capabilities in netty to allow just about any messaging payload to be encoded and decoded easily, with low memory and CPU overhead.

- _Groovy and spring_. Forthcoming once the _API_ has matured.

- Protocol API and parser generator. Its easy to specify a protocol using a regular-expression like language and then generate a parser in various languages to decode that protocol into a higher level format. Forthcoming after _API_ maturity points are met.
 
## Design Goals

The design goals this framework are as follows:

1. Lightweight, simple, minimalistic framework and API footprint

2. The throughput, latency profile required to build super-scale NATs applications.

3. Usage and design flexibility for users and implementors of the package for everything from threading, parsing, networking, timing, etc...

## Design and Implementation Overview

I've hacked enough networking code to not want to do so again at the metal level and decided to use [netty](http://netty.io/). I can here the _jeers_... You _decided_ to use _what_? Don't you _want_ to _read from sockets_ and _listen_ on _ports_? Don't you want to _write a thread pool_? I started my career hacking network code, have written several products and now am back at the same place. With all I've learned and done, I can confidently state:

> No, I do not.
> - jam

With that said. Why netty? Because its fast, has a proven, easy to understand I/O, concurrency and API model. It is  
tested around the world by more folks than I can dream of. It is lightweight if you pick out what you want. It is super
extensible. Want spdy tunnel support? *Done*. Want to proxy over UDP? *Done*. Want a simple concurrency model? *Done*. 
_Etc_. And if its not _done_, the framework is _extremely extensible_ for you to _make it so_.

> Netty allows most of our design goals to be met out of the box

So if you don't know netty, go do some research, write some code and use it. People like nm and t and others have contributed greatly to the community in code, support, documentation, white papers and other artifacts that you can learn from.

At a high level: all our nats.io.imp classes are running within a pipeline on a netty channel. This channel is attached typically to a gnatsd process and is acting as a client (assumption for the rest of this section).

All I/O, timers, etc. are run in a thread safe manner via netty APIs without us having to do anything. We only need to worry about two things:

1. Calls into our clases from outside of the event loop

2. Calling into user code that may block

For #1 we hande that with the minimal locks as required and they are outside of ALL API routines.

For #2 we gate over that with a hack somewhat. The thought  was that people want to truly hack each byte of memory and performance away if at all possible for for _super scale_ or _super micro_ services. You may find such service in  in a low latency medical application or an IoT appliance such as a refrigerator. Wouldn't you want the fastest, etc offering for your value chain experience. So with that goal (aka "design feature"/hack) in mind:

We allow applications to extend the NATS library essentially using standard _netty_ semantics around pipelines, channels, etc.
In a nutshell, that means you _may_ create a _standard netty handler_ to service NATS protocol verbs, messages and lifecycle events easily.
We respect channel length maximums,  but if you are unable to keep up with calls as the underlying pipeline is decoding them you may either drop messages, be marked as a slow subscriber for the given topic or _kill_ the _event loop_ and cause all subscribers to be marked slow.
We support both async and sync styles for the Server API. See the ServerAPI example for more information.

> Be aware that server side API usage DOES NOT start any threads. You are responsible for doing so and running the tasks passed to your handler in the thread group's executor to get message flow going (or in your channel handler's method).
 


# License
Copyright 2015, [Apcera, Inc](http://www.apcera.com/)
All rights reserved

