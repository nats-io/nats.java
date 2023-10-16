# Client Compatibility Test

## Pre-requisites

1. Java 1.8 or Java 11
2. Gradle 6.8.3 or later 6.x.x

## How to build and run the test.

From the root folder of the java client project

Compile the code:
```shell
> gradle clean compileTestJava
```

Start the responder...
```shell
> java -cp build/classes/java/main;build/classes/java/test io.nats.compatibility.ClientCompatibilityMain
```

You will see this when the engine is ready... 
```shell
[main@473171716] Ready
```

You can start the client compatibility CLI, for instance:
```shell
client-compatibility suite object-store
```