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

import io.nats.client.support.JsonUtils;
import io.nats.client.support.JsonValue;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.nats.client.support.ApiConstants.DESCRIPTION;
import static io.nats.client.support.ApiConstants.SUBJECTS;
import static io.nats.client.support.JsonValueUtils.readString;
import static io.nats.client.support.JsonValueUtils.readStringList;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class InfoResponse extends ServiceResponse {
    public static final String TYPE = "io.nats.micro.v1.info_response";

    private final String description;
    private final List<String> subjects;

    public InfoResponse(String id, String name, String version, Map<String, String> metadata, String description, List<String> subjects) {
        super(TYPE, id, name, version, metadata);
        this.description = description;
        this.subjects = subjects;
    }

    public InfoResponse(byte[] jsonBytes) {
        this(parseMessage(jsonBytes));
    }

    private InfoResponse(JsonValue jv) {
        super(TYPE, jv);
        description = readString(jv, DESCRIPTION);
        subjects = readStringList(jv, SUBJECTS);
    }

    @Override
    protected void subToJson(StringBuilder sb) {
        JsonUtils.addField(sb, DESCRIPTION, description);
        JsonUtils.addStrings(sb, SUBJECTS, subjects);
    }

    /**
     * Description for the service
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Subjects that can be invoked
     * @return the subjects
     */
    public List<String> getSubjects() {
        return subjects;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        InfoResponse that = (InfoResponse) o;

        if (!Objects.equals(description, that.description)) return false;
        return Objects.equals(subjects, that.subjects);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (subjects != null ? subjects.hashCode() : 0);
        return result;
    }
}
