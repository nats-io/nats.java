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

package io.nats.service.api;

import io.nats.client.support.*;

import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.JsonValueUtils.readValue;
import static io.nats.service.ServiceUtil.validateEndpointName;
import static io.nats.service.ServiceUtil.validateEndpointSubject;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class Endpoint implements JsonSerializable {
    private final String name;
    private final String subject;
    private final Schema schema;

    static List<Endpoint> listOf(JsonValue vEndpoints) {
        return JsonValueUtils.listOf(vEndpoints, Endpoint::new);
    }

    private Endpoint(Builder b) {
        this(b.name, b.subject,
            Schema.optionalInstance(b.schemaRequest, b.schemaResponse));
    }

    protected Endpoint(JsonValue vEndpoint) {
        name = readString(vEndpoint, NAME);
        subject = readString(vEndpoint, SUBJECT);
        schema = Schema.optionalInstance(readValue(vEndpoint, SCHEMA));
    }

    public Endpoint(String name, String subject, Schema schema) {
        this.name = validateEndpointName(name);
        if (Validator.emptyAsNull(subject) == null) {
            this.subject = name;
        }
        else {
            this.subject = validateEndpointSubject(subject);
        }
        this.schema = schema;
    }

    public Endpoint(String name, String subject) {
        this(name, subject, null);
    }

    public Endpoint(String name) {
        this(name, null, null);
    }

    public Endpoint(String name, String subject, String schemaRequest, String schemaResponse) {
        this(name, subject, Schema.optionalInstance(schemaRequest, schemaResponse));
    }

    @Override
    public String toJson() {
        StringBuilder sb = JsonUtils.beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, SUBJECT, subject);
        JsonUtils.addField(sb, SCHEMA, schema);
        return endJson(sb).toString();
    }

    @Override
    public String toString() {
        return toJson();
    }

    public String getName() {
        return name;
    }

    public String getSubject() {
        return subject;
    }

    public Schema getSchema() {
        return schema;
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private String subject;
        private String schemaRequest;
        private String schemaResponse;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder subject(String subject) {
            this.subject = subject;
            return this;
        }

        public Builder schemaRequest(String schemaRequest) {
            this.schemaRequest = schemaRequest;
            return this;
        }

        public Builder schemaResponse(String schemaResponse) {
            this.schemaResponse = schemaResponse;
            return this;
        }

        public Builder schema(Schema schema) {
            if (schema == null) {
                schemaRequest = null;
                schemaResponse = null;
            }
            else {
                schemaRequest = schema.getRequest();
                schemaResponse = schema.getResponse();
            }
            return this;
        }

        public Endpoint build() {
            return new Endpoint(this);
        }
    }
}
