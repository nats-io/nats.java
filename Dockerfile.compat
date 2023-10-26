FROM gradle:6-jdk11 AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle clean compileTestJava --no-daemon

FROM openjdk:11-jre-slim
RUN mkdir /app
COPY --from=build /home/gradle/src/build/classes/java /app
ENTRYPOINT ["java", "--class-path=/app/main:/app/test", "io.nats.compatibility.ClientCompatibilityMain"]