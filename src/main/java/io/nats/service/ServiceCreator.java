package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.MessageHandler;

import static io.nats.client.support.Validator.required;
import static io.nats.client.support.Validator.validateIsRestrictedTerm;

public class ServiceCreator {
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
    StatsDataSupplier statsDataSupplier;
    StatsDataDecoder statsDataDecoder;

    public static ServiceCreator instance() {
        return new ServiceCreator();
    }

    public ServiceCreator connection(Connection conn) {
        this.conn = conn;
        return this;
    }

    public ServiceCreator name(String name) {
        this.name = name;
        return this;
    }

    public ServiceCreator description(String description) {
        this.description = description;
        return this;
    }

    public ServiceCreator version(String version) {
        this.version = version;
        return this;
    }

    public ServiceCreator subject(String subject) {
        this.subject = subject;
        return this;
    }

    public ServiceCreator schemaRequest(String schemaRequest) {
        this.schemaRequest = schemaRequest;
        return this;
    }

    public ServiceCreator schemaResponse(String schemaResponse) {
        this.schemaResponse = schemaResponse;
        return this;
    }

    public ServiceCreator serviceMessageHandler(MessageHandler userMessageHandler) {
        this.serviceMessageHandler = userMessageHandler;
        return this;
    }

    public ServiceCreator userDiscoveryDispatcher(Dispatcher dUserDiscovery) {
        this.dUserDiscovery = dUserDiscovery;
        return this;
    }

    public ServiceCreator userServiceDispatcher(Dispatcher dUserService) {
        this.dUserService = dUserService;
        return this;
    }

    public ServiceCreator statsDataHandlers(StatsDataSupplier statsDataSupplier, StatsDataDecoder statsDataDecoder) {
        this.statsDataSupplier = statsDataSupplier;
        this.statsDataDecoder = statsDataDecoder;
        return this;
    }

    /**
     * Synonym for startService
     * @return a new running service
     */
    public Service build() {
        return startService();
    }

    public Service startService() {
        required(conn, "Connection");
        required(serviceMessageHandler, "Service Message Handler");
        validateIsRestrictedTerm(name, "Name", true);
        required(version, "Version");
        if ((statsDataSupplier != null && statsDataDecoder == null)
            || (statsDataSupplier == null && statsDataDecoder != null)) {
            throw new IllegalArgumentException("You must provide neither or both the stats data supplier and decoder");
        }

        return new Service(this);
    }
}
