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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * Object used to make a request for message get requests.
 */
public class MessageGetRequest implements JsonSerializable {
    private final long sequence;
    private final String lastBySubject;
    private final String nextBySubject;
    private final ZonedDateTime startTime;
    private boolean noHeaders;

    public MessageGetRequest noHeaders() {
        noHeaders = true;
        return this;
    }

    @NonNull
    public static MessageGetRequest forSequence(long sequence) {
        return new MessageGetRequest(sequence, null, null, null);
    }

    @NonNull
    public static MessageGetRequest lastForSubject(String subject) {
        return new MessageGetRequest(-1, subject, null, null);
    }

    @NonNull
    public static MessageGetRequest firstForSubject(String subject) {
        return new MessageGetRequest(-1, null, subject, null);
    }

    @NonNull
    public static MessageGetRequest firstForStartTime(ZonedDateTime startTime) {
        return new MessageGetRequest(-1, null, null, startTime);
    }

    @NonNull
    public static MessageGetRequest firstForStartTimeAndSubject(ZonedDateTime startTime, String subject) {
        return new MessageGetRequest(-1, null, subject, startTime);
    }

    @NonNull
    public static MessageGetRequest nextForSubject(long sequence, String subject) {
        return new MessageGetRequest(sequence, null, subject, null);
    }

    private MessageGetRequest(long sequence, String lastBySubject, String nextBySubject, ZonedDateTime startTime) {
        this.sequence = sequence;
        this.lastBySubject = lastBySubject;
        this.nextBySubject = nextBySubject;
        this.startTime = startTime;
        noHeaders = false;
    }

    private MessageGetRequest(MessageGetRequest r, boolean noHeaders) {
        this.sequence = r.sequence;
        this.lastBySubject = r.lastBySubject;
        this.nextBySubject = r.nextBySubject;
        this.startTime = r.startTime;
        this.noHeaders = noHeaders;
    }

    public long getSequence() {
        return sequence;
    }

    @Nullable
    public String getLastBySubject() {
        return lastBySubject;
    }

    @Nullable
    public String getNextBySubject() {
        return nextBySubject;
    }

    public boolean isSequenceOnly() {
        return sequence > 0 && nextBySubject == null;
    }

    public boolean isLastBySubject() {
        return lastBySubject != null;
    }

    public boolean isNextBySubject() {
        return nextBySubject != null;
    }

    public boolean isNoHeaders() {
        return noHeaders;
    }

    @Override
    @NonNull
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, SEQ, sequence);
        addField(sb, LAST_BY_SUBJECT, lastBySubject);
        addField(sb, NEXT_BY_SUBJECT, nextBySubject);
        addField(sb, START_TIME, startTime);
        addFldWhenTrue(sb, NO_HDR, noHeaders);
        return endJson(sb).toString();
    }

    /**
     * @deprecated use static method forSequence with .serialize instead
     * @param sequence start sequence
     * @return rendered output
     */
    @Deprecated
    public static byte[] seqBytes(long sequence) {
        return forSequence(sequence).serialize();
    }

    /**
     * @deprecated use static method lastForSubject with .serialize instead
     * @param subject filter subject
     * @return rendered output
     */
    @Deprecated
    public static byte[] lastBySubjectBytes(String subject) {
        return lastForSubject(subject).serialize();
    }

    /**
     * @deprecated use static method forSequence instead
     *
     * @param sequence start sequence number
     */
    @Deprecated
    public MessageGetRequest(long sequence) {
        this(sequence, null, null, null);
    }

    /**
     * @deprecated use static method lastForSubject instead
     *
     * @param lastBySubject filter subject
     */
    @Deprecated
    public MessageGetRequest(String lastBySubject) {
        this(-1, lastBySubject, null, null);
    }
}
