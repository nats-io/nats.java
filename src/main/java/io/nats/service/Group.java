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

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.service.ServiceUtil.validateGroupName;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class Group {
    protected final String name;
    protected Group next;

    public Group(String name) {
        this.name = validateGroupName(name);
    }

    public Group appendGroup(Group group) {
        this.next = group;
        return group;
    }

    public String getSubject() {
        return next == null ? name : name + DOT + next.getName();
    }

    public String getName() {
        return name;
    }

    public Group getNext() {
        return next;
    }
}
