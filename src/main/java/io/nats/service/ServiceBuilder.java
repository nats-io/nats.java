package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.nats.client.support.Validator.*;

/**
 * Build a Service using a fluent builder.
 * Use the Service static method <code>builder()</code> or <code>new ServiceBuilder()</code> to get an instance.
 */
public class ServiceBuilder {
    /**
     * Constant for the default drain timeout in millis
     */
    public static final long DEFAULT_DRAIN_TIMEOUT_MILLIS = 5000;

    /**
     * Constant for the default drain timeout as duration
     */
    public static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofMillis(DEFAULT_DRAIN_TIMEOUT_MILLIS);

    Connection conn;
    String name;
    String description;
    String version;
    Map<String, String> metadata;
    final Map<String, ServiceEndpoint> serviceEndpoints = new HashMap<>();
    Duration drainTimeout = DEFAULT_DRAIN_TIMEOUT;
    Dispatcher pingDispatcher;
    Dispatcher infoDispatcher;
    Dispatcher statsDispatcher;

    /**
     * Construct an instance of the builder
     */
    public ServiceBuilder() {}

    /**
     * The connection the service runs on
     * @param conn connection
     * @return the ServiceBuilder
     */
    public ServiceBuilder connection(Connection conn) {
        this.conn = conn;
        return this;
    }

    /**
     * The simple name of the service
     * @param name the name
     * @return the ServiceBuilder
     */
    public ServiceBuilder name(String name) {
        this.name = validateIsRestrictedTerm(name, "Service Name", true);
        return this;
    }

    /**
     * The simple description of the service
     * @param description the description
     * @return the ServiceBuilder
     */
    public ServiceBuilder description(String description) {
        this.description = description;
        return this;
    }

    /**
     * The simple version of the service.
     * @param version the version
     * @return the ServiceBuilder
     */
    public ServiceBuilder version(String version) {
        this.version = validateSemVer(version, "Service Version", true);
        return this;
    }

    /**
     * Any meta information about this service
     * @param metadata the meta
     * @return the ServiceBuilder
     */
    public ServiceBuilder metadata(Map<String, String> metadata) {
        this.metadata = metadata;
        return this;
    }

    /**
     * Add a service endpoint into the service. There can only be one instance of a service endpoint by name
     * @param serviceEndpoint the service endpoint
     * @return the ServiceBuilder
     */
    public ServiceBuilder addServiceEndpoint(ServiceEndpoint serviceEndpoint) {
        serviceEndpoints.put(serviceEndpoint.getName(), serviceEndpoint);
        return this;
    }

    /**
     * The timeout when stopping a service. Defaults to {@value #DEFAULT_DRAIN_TIMEOUT_MILLIS} milliseconds
     * @param drainTimeout the drain timeout
     * @return the ServiceBuilder
     */
    public ServiceBuilder drainTimeout(Duration drainTimeout) {
        this.drainTimeout = drainTimeout == null ? DEFAULT_DRAIN_TIMEOUT : drainTimeout;
        return this;
    }

    /**
     * The timeout when stopping a service. Defaults to {@value #DEFAULT_DRAIN_TIMEOUT_MILLIS} milliseconds
     * @param drainTimeoutMillis the drain timeout in milliseconds
     * @return the ServiceBuilder
     */
    public ServiceBuilder drainTimeout(long drainTimeoutMillis) {
        this.drainTimeout = Duration.ofMillis(drainTimeoutMillis);
        return this;
    }

    /**
     * Optional dispatcher for the ping service
     * @param pingDispatcher the dispatcher
     * @return the ServiceBuilder
     */
    public ServiceBuilder pingDispatcher(Dispatcher pingDispatcher) {
        this.pingDispatcher = pingDispatcher;
        return this;
    }

    /**
     * Optional dispatcher for the info service
     * @param infoDispatcher the dispatcher
     * @return the ServiceBuilder
     */
    public ServiceBuilder infoDispatcher(Dispatcher infoDispatcher) {
        this.infoDispatcher = infoDispatcher;
        return this;
    }

    /**
     * A NOOP method to maintain compatibility with the old schema dispatcher, superseded by endpointMetadata
     * @deprecated No longer used, see {@link ServiceEndpoint.Builder#endpointMetadata(Map)} instead
     * @param schemaDispatcher the dispatcher
     * @return the ServiceBuilder
     */
    @Deprecated
    public ServiceBuilder schemaDispatcher(Dispatcher schemaDispatcher) {
        return this;
    }

    /**
     * Optional dispatcher for the stats service
     * @param statsDispatcher the dispatcher
     * @return the ServiceBuilder
     */
    public ServiceBuilder statsDispatcher(Dispatcher statsDispatcher) {
        this.statsDispatcher = statsDispatcher;
        return this;
    }

    /**
     * Build the Service instance.
     * @return the Service instance
     */
    public Service build() {
        required(conn, "Connection");
        required(name, "Name");
        required(version, "Version");
        return new Service(this);
    }
}
