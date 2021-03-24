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

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * Mirror Information
 */
public class Mirror implements JsonSerializable {
    private final String name;
    private final long startSeq;
    private final ZonedDateTime startTime;
    private final String filterSubject;
    private final External external;

    public static Mirror optionalInstance(String fullJson) {
        String objJson = JsonUtils.getJsonObject(MIRROR, fullJson, null);
        return objJson == null ? null : new Mirror(objJson);
    }

    public Mirror(String json) {
        name = JsonUtils.readString(json, NAME_RE);
        startSeq = JsonUtils.readLong(json, OPT_START_SEQ_RE, 0);
        startTime = JsonUtils.readDate(json, OPT_START_TIME_RE);
        filterSubject = JsonUtils.readString(json, FILTER_SUBJECT_RE);
        external = External.optionalInstance(json);
    }

    public Mirror(String name, long startSeq, ZonedDateTime startTime, String filterSubject, External external) {
        this.name = name;
        this.startSeq = startSeq;
        this.startTime = startTime;
        this.filterSubject = filterSubject;
        this.external = external;
    }

    /**
     * Returns a JSON representation of this mirror
     *
     * @return json mirror json string
     */
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addField(sb, NAME, name);
        JsonUtils.addField(sb, OPT_START_SEQ, startSeq);
        JsonUtils.addField(sb, OPT_START_TIME, startTime);
        JsonUtils.addField(sb, FILTER_SUBJECT, filterSubject);
        return endJson(sb).toString();
    }

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
        return "Mirror{" +
                "name='" + name + '\'' +
                ", startSeq=" + startSeq +
                ", startTime=" + startTime +
                ", filterSubject='" + filterSubject + '\'' +
                ", " + objectString("external", external) +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String name;
        private long startSeq;
        private ZonedDateTime startTime;
        private String filterSubject;
        private External external;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder startSeq(long startSeq) {
            this.startSeq = startSeq;
            return this;
        }

        public Builder startTime(ZonedDateTime startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder filterSubject(String filterSubject) {
            this.filterSubject = filterSubject;
            return this;
        }

        public Builder external(External external) {
            this.external = external;
            return this;
        }

        public Mirror build() {
            return new Mirror(name, startSeq, startTime, filterSubject, external);
        }
    }
}
