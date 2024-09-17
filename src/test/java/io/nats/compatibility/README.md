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

## Server Url

You can supply the url for the server in two ways. If not supplied, the program will assume `nats://localhost:4222`

1. The program first checks the command line
    ```shell
    > java -cp ... io.nats.compatibility.ClientCompatibilityMain nats://myhost:4333
    ```
2. If there is no url on the command line, it checks the NATS_URL environment variable.
3. If it's not provided in arguments or environment, the default is used.
