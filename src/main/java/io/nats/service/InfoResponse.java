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

import io.nats.client.support.*;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.readString;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class InfoResponse implements JsonSerializable {
    public static final String TYPE = "io.nats.micro.v1.info_response";

    private final String serviceId;
    private final String name;
    private final String version;
    private final String description;
    private final String subject;

    public InfoResponse(String serviceId, String name, String version, String description, String subject) {
        this.serviceId = serviceId;
        this.name = name;
        this.version = version;
        this.description = description;
        this.subject = subject;
    }

    public InfoResponse(byte[] jsonBytes) {
        JsonValue jv = JsonParser.parse(jsonBytes);
        name = readString(jv, NAME);
        serviceId = readString(jv, ID);
        description = readString(jv, DESCRIPTION);
        version = readString(jv, VERSION);
        subject = readString(jv, SUBJECT);
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, ApiConstants.TYPE, TYPE);
        JsonUtils.addField(sb, ID, serviceId);
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
     * The type of this. Always {@value #TYPE}
     * @return the type string
     */
    public String getType() {
        return TYPE;
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
