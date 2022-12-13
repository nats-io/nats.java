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

import java.util.Objects;

public class ServiceDescriptor {

    public final String name;
    public final String description;
    public final String version;
    public final String subject;
    public final String schemaRequest;
    public final String schemaResponse;

    public ServiceDescriptor(String name, 
                             String description, 
                             String version, 
                             String subject, 
                             String schemaRequest, 
                             String schemaResponse) {
        this.name = name;
        this.description = description;
        this.version = version;
        this.subject = subject; // TODO validate subject
        this.schemaRequest = schemaRequest;
        this.schemaResponse = schemaResponse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ServiceDescriptor that = (ServiceDescriptor) o;

        if (!Objects.equals(name, that.name)) return false;
        if (!Objects.equals(description, that.description)) return false;
        if (!Objects.equals(version, that.version)) return false;
        if (!Objects.equals(subject, that.subject)) return false;
        if (!Objects.equals(schemaRequest, that.schemaRequest))
            return false;
        return Objects.equals(schemaResponse, that.schemaResponse);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (subject != null ? subject.hashCode() : 0);
        result = 31 * result + (schemaRequest != null ? schemaRequest.hashCode() : 0);
        result = 31 * result + (schemaResponse != null ? schemaResponse.hashCode() : 0);
        return result;
    }
}
