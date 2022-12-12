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

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;
import io.nats.service.ServiceDescriptor;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * TBD
 */
public class SchemaResponse implements JsonSerializable {
    private final String serviceId;
    private final String name;
    private final String version;
    private final Schema schema;

    public SchemaResponse(String id, String name, String version, Schema schema) {
        this.serviceId = id;
        this.name = name;
        this.version = version;
        this.schema = schema;
    }

    public SchemaResponse(String serviceId, ServiceDescriptor descriptor)  {
        this.serviceId = serviceId;
        this.name = descriptor.name;
        this.version = descriptor.version;
        this.schema = new Schema(descriptor);
    }

    public SchemaResponse(String json) {
        name = JsonUtils.readString(json, string_pattern(NAME));
        serviceId = JsonUtils.readString(json, string_pattern("id"));
        version = JsonUtils.readString(json, VERSION_RE);
        schema = Schema.optionalInstance(json);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, "id", serviceId);
        JsonUtils.addField(sb, VERSION, version);
        addField(sb, "schema", schema);
        return endJson(sb).toString();
    }

    /**
     * The kind of the service reporting the status
     * @return the service name
     */
    public String getName() {
        return name;
    }

    /**
     * The unique ID of the service reporting the status
     * @return the service id
     */
    public String getServiceId() {
        return serviceId;
    }

    /**
     * Version of the schema
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public String toString() {
        return toJson();
    }
}
