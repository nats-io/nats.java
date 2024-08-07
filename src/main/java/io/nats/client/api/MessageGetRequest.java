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

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

/**
 * Object used to make a request for special message get requests.
 */
public class MessageGetRequest implements JsonSerializable {
    private final long sequence;
    private final String lastBySubject;
    private final String nextBySubject;

    public static MessageGetRequest forSequence(long sequence) {
        return new MessageGetRequest(sequence, null, null);
    }

    public static MessageGetRequest lastForSubject(String subject) {
        return new MessageGetRequest(-1, subject, null);
    }

    public static MessageGetRequest firstForSubject(String subject) {
        return new MessageGetRequest(-1, null, subject);
    }

    public static MessageGetRequest nextForSubject(long sequence, String subject) {
        return new MessageGetRequest(sequence, null, subject);
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
        this(sequence, null, null);
    }

    /**
     * @deprecated use static method lastForSubject instead
     * 
     * @param lastBySubject filter subject
     */
    @Deprecated
    public MessageGetRequest(String lastBySubject) {
        this(-1, lastBySubject, null);
    }

    private MessageGetRequest(long sequence, String lastBySubject, String nextBySubject) {
        this.sequence = sequence;
        this.lastBySubject = lastBySubject;
        this.nextBySubject = nextBySubject;
    }

    public long getSequence() {
        return sequence;
    }

    public String getLastBySubject() {
        return lastBySubject;
    }

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

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, SEQ, sequence);
        addField(sb, LAST_BY_SUBJECT, lastBySubject);
        addField(sb, NEXT_BY_SUBJECT, nextBySubject);
        return endJson(sb).toString();
    }
}
