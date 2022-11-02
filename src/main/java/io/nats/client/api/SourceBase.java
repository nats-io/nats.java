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

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonUtils;

import java.time.ZonedDateTime;

import static io.nats.client.JetStreamOptions.convertDomainToPrefix;
import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

abstract class SourceBase implements JsonSerializable {
    private final String name;
    private final long startSeq;
    private final ZonedDateTime startTime;
    private final String filterSubject;
    private final External external;
    private final String objectName;

    SourceBase(String objectName, String json) {
        name = JsonUtils.readString(json, NAME_RE);
        startSeq = JsonUtils.readLong(json, OPT_START_SEQ_RE, 0);
        startTime = JsonUtils.readDate(json, OPT_START_TIME_RE);
        filterSubject = JsonUtils.readString(json, FILTER_SUBJECT_RE);
        external = External.optionalInstance(json);
        this.objectName = normalize(objectName);
    }

    @SuppressWarnings("rawtypes") // Don't need the type of the builder to get its vars
    SourceBase(String objectName, SourceBaseBuilder b) {
        this.name = b.name;
        this.startSeq = b.startSeq;
        this.startTime = b.startTime;
        this.filterSubject = b.filterSubject;
        this.external = b.external;
        this.objectName = normalize(objectName);
    }

    /**
     * Returns a JSON representation of this mirror
     *
     * @return json mirror json string
     */
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        if (startSeq > 0) {
            JsonUtils.addField(sb, OPT_START_SEQ, startSeq);
        }
        JsonUtils.addField(sb, OPT_START_TIME, startTime);
        JsonUtils.addField(sb, FILTER_SUBJECT, filterSubject);
        JsonUtils.addField(sb, EXTERNAL, external);
        return endJson(sb).toString();
    }

    /**
     * Get the name of the source. Same as getName()
     * @return get the source name
     */
    public String getSourceName() {
        return name;
    }

    /**
     * Get the name of the source. Same as getSourceName()
     * @return the source name
     */
    public String getName() {
        return name;
    }

    public long getStartSeq() {
        return startSeq;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public String getFilterSubject() {
        return filterSubject;
    }

    public External getExternal() {
        return external;
    }

    @Override
    public String toString() {
        return objectName + "{" +
                "name='" + name + '\'' +
                ", startSeq=" + startSeq +
                ", startTime=" + startTime +
                ", filterSubject='" + filterSubject + '\'' +
                ", " + objectString("external", external) +
                '}';
    }

    public abstract static class SourceBaseBuilder<T> {
        String name;
        long startSeq;
        ZonedDateTime startTime;
        String filterSubject;
        External external;

        abstract T getThis();

        public SourceBaseBuilder() {}

        public SourceBaseBuilder(SourceBase base) {
            this.name = base.name;
            this.startSeq = base.startSeq;
            this.startTime = base.startTime;
            this.filterSubject = base.filterSubject;
            this.external = base.external;
        }

        public T sourceName(String name) {
            this.name = name;
            return getThis();
        }

        public T name(String name) {
            this.name = name;
            return getThis();
        }

        public T startSeq(long startSeq) {
            this.startSeq = startSeq;
            return getThis();
        }

        public T startTime(ZonedDateTime startTime) {
            this.startTime = startTime;
            return getThis();
        }

        public T filterSubject(String filterSubject) {
            this.filterSubject = filterSubject;
            return getThis();
        }

        public T external(External external) {
            this.external = external;
            return getThis();
        }

        public T domain(String domain) {
            external = External.builder().api(convertDomainToPrefix(domain)).build();
            return getThis();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SourceBase that = (SourceBase) o;

        if (startSeq != that.startSeq) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (startTime != null ? !startTime.equals(that.startTime) : that.startTime != null) return false;
        if (filterSubject != null ? !filterSubject.equals(that.filterSubject) : that.filterSubject != null)
            return false;
        if (external != null ? !external.equals(that.external) : that.external != null) return false;
        return objectName.equals(that.objectName);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (int) (startSeq ^ (startSeq >>> 32));
        result = 31 * result + (startTime != null ? startTime.hashCode() : 0);
        result = 31 * result + (filterSubject != null ? filterSubject.hashCode() : 0);
        result = 31 * result + (external != null ? external.hashCode() : 0);
        result = 31 * result + objectName.hashCode();
        return result;
    }
}
