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

import static io.nats.client.support.Validator.required;
import static io.nats.client.support.Validator.validateIsRestrictedTerm;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
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
        this.name = required(name, "name");
        this.description = description;
        this.version = required(version, "version");
        this.subject = validateIsRestrictedTerm("subject", subject, true);
        this.schemaRequest = schemaRequest;
        this.schemaResponse = schemaResponse;
    }
}
