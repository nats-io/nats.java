// Copyright 2022 The NATS Authors
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
import io.nats.client.MessageHandler;
import io.nats.client.support.JsonValue;
import io.nats.client.support.Validator;
import io.nats.service.api.Endpoint;

import java.util.function.Supplier;

import static io.nats.client.support.NatsConstants.DOT;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class ServiceEndpoint {
    protected final Group group;
    protected final Endpoint endpoint;
    protected final MessageHandler handler;
    protected final Supplier<JsonValue> statsDataSupplier;
    protected final Dispatcher dispatcher;

    private ServiceEndpoint(Builder b) {
        this.group = b.group;
        this.endpoint = b.endpoint;
        this.handler = b.handler;
        this.statsDataSupplier = b.statsDataSupplier;
        this.dispatcher = b.dispatcher;
    }

    protected ServiceEndpoint(ServiceEndpoint se) {
        this.group = se.group;
        this.endpoint = se.endpoint;
        this.handler = se.handler;
        this.statsDataSupplier = se.statsDataSupplier;
        this.dispatcher = se.dispatcher;
    }

    public String getName() {
        return endpoint.getName();
    }

    public String getSubject() {
        return group == null ? endpoint.getSubject() : group.getSubject() + DOT + endpoint.getSubject();
    }

    public Group getGroup() {
        return group;
    }

    public Endpoint getEndpoint() {
        return endpoint;
    }

    public MessageHandler getHandler() {
        return handler;
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public Supplier<JsonValue> getStatsDataSupplier() {
        return statsDataSupplier;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Group group;
        private MessageHandler handler;
        private Dispatcher dispatcher;
        private Supplier<JsonValue> statsDataSupplier;
        private Endpoint endpoint;

        public Builder group(Group group) {
            this.group = group;
            return this;
        }

        public Builder endpoint(Endpoint endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder handler(MessageHandler handler) {
            this.handler = handler;
            return this;
        }

        public Builder dispatcher(Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
            return this;
        }

        public Builder statsDataSupplier(Supplier<JsonValue> statsDataSupplier) {
            this.statsDataSupplier = statsDataSupplier;
            return this;
        }

        public ServiceEndpoint build() {
            Validator.required(endpoint, "Endpoint");
            Validator.required(handler, "Message Handler");
            return new ServiceEndpoint(this);
        }
    }
}
