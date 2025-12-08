![NATS](../../../../../../../src/main/javadoc/images/large-logo.png)

# Nats Documentation Examples

These are examples that get published in the NATS documentation.

### Basic Examples
1. `BasicsSubscribe.java` - subscribe to a subject
1. `BasicsPublish.java` - publishes a message

## Running the examples

The easiest way to run the examples is from an IDE, but they can be run from the command line. First, Build the Uber Jar
```bash
gradlew clean uberJar
```
Then run with the jar in the classpath

```
java -cp build/libs/java-nats-{major.minor.patch}-SNAPSHOT-uber.jar io.nats.examples.doc.BasicsSubscribe
```
