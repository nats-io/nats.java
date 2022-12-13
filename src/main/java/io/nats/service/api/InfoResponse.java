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
public class InfoResponse implements JsonSerializable {
    private final String name;
    private final String serviceId;
    private final String description;
    private final String version;
    private final String subject;

    public InfoResponse(String id, ServiceDescriptor descriptor) {
        this.serviceId = id;
        this.name = descriptor.name;
        this.description = descriptor.description;
        this.version = descriptor.version;
        this.subject = descriptor.subject;
    }

    public InfoResponse(String json) {
        name = JsonUtils.readString(json, string_pattern(NAME));
        serviceId = JsonUtils.readString(json, string_pattern("id"));
        description = JsonUtils.readString(json, DESCRIPTION_RE);
        version = JsonUtils.readString(json, VERSION_RE);
        subject = JsonUtils.readString(json, SUBJECT_RE);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, "id", serviceId);
        JsonUtils.addField(sb, DESCRIPTION, description);
        JsonUtils.addField(sb, VERSION, version);
        JsonUtils.addField(sb, SUBJECT, subject);
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
     * Description for the service
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Version of the service
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * Subject where the service can be invoked
     * @return the subject
     */
    public String getSubject() {
        return subject;
    }

    @Override
    public String toString() {
        return toJson();
    }
}
