package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.MessageHandler;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.nats.client.support.Validator.*;
import static io.nats.service.ServiceUtil.DEFAULT_DRAIN_TIMEOUT;

public class ServiceBuilder {
    Connection conn;
    String name;
    String description;
    String version;
    String rootSubject;
    String schemaRequest;
    String schemaResponse;
    MessageHandler rootMessageHandler;
    Dispatcher dUserDiscovery;
    Dispatcher dUserService;
    Supplier<StatsData> statsDataSupplier;
    Function<String, StatsData> statsDataDecoder;
    Duration drainTimeout = DEFAULT_DRAIN_TIMEOUT;
    Map<String, MessageHandler> endpointMap = new HashMap<>();

    public ServiceBuilder connection(Connection conn) {
        this.conn = conn;
        return this;
    }

    public ServiceBuilder name(String name) {
        this.name = validateIsRestrictedTerm(name, "Name", true);
        return this;
    }

    public ServiceBuilder description(String description) {
        this.description = description;
        return this;
    }

    public ServiceBuilder version(String version) {
        this.version = validateSemVer(version, "Version", true);
        return this;
    }

    public ServiceBuilder rootSubject(String rootSubject) {
        this.rootSubject = rootSubject;
        return this;
    }

    public ServiceBuilder rootMessageHandler(MessageHandler rootMessageHandler) {
        this.rootMessageHandler = rootMessageHandler;
        return this;
    }

    public ServiceBuilder endpoint(String endpoint, MessageHandler endPointHandler) {
        endpointMap.put(
            validateIsRestrictedTerm(endpoint, "Endpoint", true),
            (MessageHandler)validateNotNull(endPointHandler, "Endpoint Handler"));
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
        required(name, "Name");
        required(rootSubject, "Root Subject");
        required(version, "Version");

        if (endpointMap.size() == 0) {
            required(rootMessageHandler, "Root Message Handler");
        }
        else if (rootMessageHandler != null){
            throw new IllegalArgumentException("Root Message Handler is not allowed when there are endpoints.");
        }

        if ((statsDataSupplier != null && statsDataDecoder == null)
            || (statsDataSupplier == null && statsDataDecoder != null)) {
            throw new IllegalArgumentException("You must provide both or neither the stats data supplier and decoder");
        }
        return new Service(this);
    }
}
