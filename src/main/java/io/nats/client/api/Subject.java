// Copyright 2020 The NATS Authors
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

import io.nats.client.support.JsonUtils;

import java.util.ArrayList;
import java.util.List;

public class Subject {
    private final String name;
    private final long count;

    static List<Subject> optionalListOf(String json) {
        List<Subject> list = new ArrayList<>();
        if (json != null) {
            // we have to manually parse this
            int s1 = json.indexOf('"');
            while (s1 != -1) {
                int s2 = json.indexOf('"', s1 + 1);
                int c1 = json.indexOf(':', s2);
                int c2 = json.indexOf(',', s2);
                if (c2 == -1) {
                    c2 = json.indexOf('}', s2);
                }
                String subject = json.substring(s1 + 1, s2).trim();
                long count = JsonUtils.safeParseLong(json.substring(c1 + 1, c2).trim(), 0);
                list.add(new Subject(subject, count));
                s1 = json.indexOf('"', c2);
            }
        }
        return list.isEmpty() ? null : list;
    }

    private Subject(String name, long count) {
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
}
