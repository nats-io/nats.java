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

import java.util.Objects;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.client.support.Validator.validateSubject;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class Group {
    private final String name;
    private Group next;

    public Group(String name) {
        this.name = validateSubject(name, "Group Name", true, true);
    }

    public Group appendGroup(Group group) {
        Group last = this;
        while (last.next != null) {
            last = last.next;
        }
        last.next = group;
        return this;
    }

    public String getSubject() {
        return next == null ? name : name + DOT + next.getSubject();
    }

    public String getName() {
        return name;
    }

    public Group getNext() {
        return next;
    }

    @Override
    public String toString() {
        return "Group [" + getSubject().replace('.', '/') + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Group group = (Group) o;

        if (!Objects.equals(name, group.name)) return false;
        return Objects.equals(next, group.next);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (next != null ? next.hashCode() : 0);
        return result;
    }
}
