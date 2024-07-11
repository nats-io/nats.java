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

package io.nats.client.api;

import io.nats.client.support.JsonValue;

import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.JsonValueUtils.getLong;

public class Subject implements Comparable<Subject> {
    private final String name;
    private final long count;

    static List<Subject> listOf(JsonValue vSubjects) {
        List<Subject> list = new ArrayList<>();
        if (vSubjects != null && vSubjects.map != null) {
            for (String subject : vSubjects.map.keySet()) {
                Long count = getLong(vSubjects.map.get(subject));
                if (count != null) {
                    list.add(new Subject(subject, count));
                }
            }
        }
        return list;
    }

    public Subject(String name, long count) {
        this.name = name;
        this.count = count;
    }

    /**
     * Get the subject name
     * @return the subject
     */
    public String getName() {
        return name;
    }

    /**
     * Get the subject message count
     * @return the count
     */
    public long getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "Subject{" +
            "name='" + name + '\'' +
            ", count=" + count +
            '}';
    }

    @Override
    public int compareTo(Subject o) {
        return name.compareTo(o.name);
    }
}
