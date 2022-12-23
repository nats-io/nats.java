package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.MessageHandler;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.nats.client.support.Validator.*;
import static io.nats.service.ServiceUtil.DEFAULT_DRAIN_TIMEOUT;

public class ServiceBuilder {
    Connection conn;
    String name;
    String description;
    String version;
    String subject;
    String schemaRequest;
    String schemaResponse;
    MessageHandler serviceMessageHandler;
    Dispatcher dUserDiscovery;
    Dispatcher dUserService;
    Supplier<StatsData> statsDataSupplier;
    Function<String, StatsData> statsDataDecoder;
    Duration drainTimeout = DEFAULT_DRAIN_TIMEOUT;

    public ServiceBuilder connection(Connection conn) {
        this.conn = conn;
        return this;
    }

    public ServiceBuilder name(String name) {
        this.name = name;
        return this;
    }

    public ServiceBuilder description(String description) {
        this.description = description;
        return this;
    }

    public ServiceBuilder version(String version) {
        this.version = version;
        return this;
    }

    public ServiceBuilder subject(String subject) {
        this.subject = subject;
        return this;
    }

    public ServiceBuilder schemaRequest(String schemaRequest) {
        this.schemaRequest = schemaRequest;
        return this;
    }

    public ServiceBuilder schemaResponse(String schemaResponse) {
        this.schemaResponse = schemaResponse;
        return this;
    }

    public ServiceBuilder serviceMessageHandler(MessageHandler userMessageHandler) {
        this.serviceMessageHandler = userMessageHandler;
        return this;
    }

    public ServiceBuilder userDiscoveryDispatcher(Dispatcher dUserDiscovery) {
        this.dUserDiscovery = dUserDiscovery;
        return this;
    }

    public ServiceBuilder userServiceDispatcher(Dispatcher dUserService) {
        this.dUserService = dUserService;
        return this;
    }

    public ServiceBuilder statsDataHandlers(Supplier<StatsData> statsDataSupplier, Function<String, StatsData> statsDataDecoder) {
        this.statsDataSupplier = statsDataSupplier;
        this.statsDataDecoder = statsDataDecoder;
        return this;
    }

    public ServiceBuilder drainTimeout(Duration drainTimeout) {
        this.drainTimeout = drainTimeout;
        return this;
    }

    public Service build() {
        required(conn, "Connection");
        required(serviceMessageHandler, "Service Message Handler");
        validateIsRestrictedTerm(name, "Name", true);
        validateSemVer(version, "Version", true);
        if ((statsDataSupplier != null && statsDataDecoder == null)
            || (statsDataSupplier == null && statsDataDecoder != null)) {
            throw new IllegalArgumentException("You must provide neither or both the stats data supplier and decoder");
        }
        return new Service(this);
    }
}
