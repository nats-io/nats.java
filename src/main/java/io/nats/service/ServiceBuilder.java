package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.nats.client.support.Validator.*;
import static io.nats.service.ServiceUtil.DEFAULT_DRAIN_TIMEOUT;

public class ServiceBuilder {
    Connection conn;
    String name;
    String description;
    String version;
    String apiUrl;
    Map<String, ServiceEndpoint> serviceEndpoints = new HashMap<>();
    Duration drainTimeout = DEFAULT_DRAIN_TIMEOUT;
    Dispatcher pingDispatcher;
    Dispatcher infoDispatcher;
    Dispatcher schemaDispatcher;
    Dispatcher statsDispatcher;

    public static ServiceBuilder instance() {
        return new ServiceBuilder();
    }

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

    public ServiceBuilder apiUrl(String apiUrl) {
        this.apiUrl = apiUrl;
        return this;
    }

    public ServiceBuilder addServiceEndpoint(ServiceEndpoint endpoint) {
        serviceEndpoints.put(endpoint.getName(), endpoint);
        return this;
    }

    public ServiceBuilder drainTimeout(Duration drainTimeout) {
        this.drainTimeout = drainTimeout;
        return this;
    }

    public ServiceBuilder pingDispatcher(Dispatcher pingDispatcher) {
        this.pingDispatcher = pingDispatcher;
        return this;
    }

    public ServiceBuilder infoDispatcher(Dispatcher infoDispatcher) {
        this.infoDispatcher = infoDispatcher;
        return this;
    }

    public ServiceBuilder schemaDispatcher(Dispatcher schemaDispatcher) {
        this.schemaDispatcher = schemaDispatcher;
        return this;
    }

    public ServiceBuilder statsDispatcher(Dispatcher statsDispatcher) {
        this.statsDispatcher = statsDispatcher;
        return this;
    }

    public Service build() {
        required(conn, "Connection");
        required(name, "Name");
        required(version, "Version");
        required(serviceEndpoints, "Endpoints");
        return new Service(this);
    }
}
