// Copyright 2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.service;

import io.nats.client.Dispatcher;
import io.nats.client.support.JsonValue;
import io.nats.client.support.Validator;

import java.util.Map;
import java.util.function.Supplier;

import static io.nats.client.support.NatsConstants.DOT;

/**
 * The ServiceEndpoint represents the working {@link Endpoint}
 * <ul>
 * <li>It allows the endpoint to be grouped.</li>
 * <li>It is where you can define the handler that will respond to incoming requests</li>
 * <li>It allows you to define it's dispatcher if desired giving granularity to threads running subscribers</li>
 * <li>It gives you a hook to provide custom data for the {@link EndpointStats}</li>
 * </ul>
 * <p>To create a ServiceEndpoint, use the ServiceEndpoint builder, which can be instantiated
 * via the static method <code>builder()</code> or <code>new ServiceEndpoint.Builder() to get an instance.</code>
 * </p>
 */
public class ServiceEndpoint {
    private final Group group;
    private final Endpoint endpoint;
    private final ServiceMessageHandler handler;
    private final Dispatcher dispatcher;
    private final Supplier<JsonValue> statsDataSupplier;

    private ServiceEndpoint(Builder b, Endpoint e) {
        this.group = b.group;
        this.endpoint = e;
        this.handler = b.handler;
        this.dispatcher = b.dispatcher;
        this.statsDataSupplier = b.statsDataSupplier;
    }

    // internal use constructor
    ServiceEndpoint(Endpoint endpoint, ServiceMessageHandler handler, Dispatcher dispatcher) {
        this.group = null;
        this.endpoint = endpoint;
        this.handler = handler;
        this.dispatcher = dispatcher;
        this.statsDataSupplier = null;
    }

    /**
     * Get the name of the {@link Endpoint}
     * @return the endpoint name
     */
    public String getName() {
        return endpoint.getName();
    }

    /**
     * Get the subject of the ServiceEndpoint which takes into account the group path and the {@link Endpoint} subject
     * @return the endpoint subject
     */
    public String getSubject() {
        return group == null ? endpoint.getSubject() : group.getSubject() + DOT + endpoint.getSubject();
    }

    /**
     * Get the name of the {@link Group}
     * @return the group name or null if there is no group
     */
    public String getGroupName() {
        return group == null ? null : group.getName();
    }

    /**
     * Get a copy of the metadata of the {@link Endpoint}
     * @return the copy of endpoint metadata
     */
    public Map<String, String> getMetadata() {
        return endpoint.getMetadata();
    }

    protected Group getGroup() {
        return group;
    }

    protected Endpoint getEndpoint() {
        return endpoint;
    }

    protected ServiceMessageHandler getHandler() {
        return handler;
    }

    protected Dispatcher getDispatcher() {
        return dispatcher;
    }

    protected Supplier<JsonValue> getStatsDataSupplier() {
        return statsDataSupplier;
    }

    /**
     * Get an instance of a ServiceEndpoint Builder.
     * @return the instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Build an ServiceEndpoint using a fluent builder.
     */
    public static class Builder {
        private Group group;
        private ServiceMessageHandler handler;
        private Dispatcher dispatcher;
        private Supplier<JsonValue> statsDataSupplier;
        private Endpoint.Builder endpointBuilder = Endpoint.builder();

        /**
         * Set the {@link Group} for this ServiceEndpoint
         * @param group the group
         * @return the ServiceEndpoint.Builder
         */
        public Builder group(Group group) {
            this.group = group;
            return this;
        }

        /**
         * Set the {@link Endpoint} for this ServiceEndpoint
         * replacing all existing endpoint information.
         * @param endpoint the endpoint to clone
         * @return the ServiceEndpoint.Builder
         */
        public Builder endpoint(Endpoint endpoint) {
            endpointBuilder = Endpoint.builder().endpoint(endpoint);
            return this;
        }

        /**
         * Set the name for the {@link Endpoint} for this ServiceEndpoint replacing any name already set.
         * @param name the endpoint name
         * @return the ServiceEndpoint.Builder
         */
        public Builder endpointName(String name) {
            endpointBuilder.name(name);
            return this;
        }

        /**
         * Set the subject for the {@link Endpoint} for this ServiceEndpoint replacing any subject already set.
         * @param subject the subject
         * @return the ServiceEndpoint.Builder
         */
        public Builder endpointSubject(String subject) {
            endpointBuilder.subject(subject);
            return this;
        }

        /**
         * Set the metadata for the {@link Endpoint} for this ServiceEndpoint replacing any metadata already set.
         * @param metadata the metadata
         * @return the ServiceEndpoint.Builder
         */
        public Builder endpointMetadata(Map<String, String> metadata) {
            endpointBuilder.metadata(metadata);
            return this;
        }

        /**
         * Set the {@link ServiceMessageHandler} for this ServiceEndpoint
         * @param handler the handler
         * @return the ServiceEndpoint.Builder
         */
        public Builder handler(ServiceMessageHandler handler) {
            this.handler = handler;
            return this;
        }

        /**
         * Set the user {@link Dispatcher} for this ServiceEndpoint
         * @param dispatcher the dispatcher
         * @return the ServiceEndpoint.Builder
         */
        public Builder dispatcher(Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
            return this;
        }

        /**
         * Set the {@link EndpointStats} data supplier for this ServiceEndpoint
         * @param statsDataSupplier the data supplier
         * @return the ServiceEndpoint.Builder
         */
        public Builder statsDataSupplier(Supplier<JsonValue> statsDataSupplier) {
            this.statsDataSupplier = statsDataSupplier;
            return this;
        }

        /**
         * Build the ServiceEndpoint instance.
         * @return the ServiceEndpoint instance
         */
        public ServiceEndpoint build() {
            Endpoint endpoint = endpointBuilder.build();
            Validator.required(handler, "Message Handler");
            return new ServiceEndpoint(this, endpoint);
        }
    }
}
