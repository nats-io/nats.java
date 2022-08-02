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
 * Object used to make a request for special message get requests. Used Internally
 */
public class MessageGetRequest implements JsonSerializable {
    private final long sequence;
    private final String lastBySubject;
    private final String nextBySubject;

    public static MessageGetRequest forSequence(long sequence) {
        return new MessageGetRequest(sequence);
    }

    public static MessageGetRequest lastForSubject(String subject) {
        return new MessageGetRequest(subject);
    }

    public static MessageGetRequest firstForSubject(String subject) {
        return new MessageGetRequest(-1, null, subject);
    }

    public static MessageGetRequest nextForSubject(long sequence, String subject) {
        return new MessageGetRequest(sequence, null, subject);
    }

    public static byte[] forSequenceBytes(long sequence) {
        return new MessageGetRequest(sequence, null, null).serialize();
    }

    public static byte[] lastForSubjectBytes(String subject) {
        return new MessageGetRequest(-1, subject, null).serialize();
    }

    public static byte[] firstForSubjectBytes(String subject) {
        return new MessageGetRequest(-1, null, subject).serialize();
    }

    public static byte[] nextForSubjectBytes(long sequence, String subject) {
        return new MessageGetRequest(sequence, null, subject).serialize();
    }

    public MessageGetRequest(long sequence) {
        this(sequence, null, null);
    }

    public MessageGetRequest(String subject) {
        this(-1, subject, null);
    }

    private MessageGetRequest(long sequence, String lastBySubject, String nextBySubject) {
        this.sequence = sequence;
        this.lastBySubject = lastBySubject;
        this.nextBySubject = nextBySubject;
    }

    @Override
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, SEQ, sequence);
        addField(sb, LAST_BY_SUBJECT, lastBySubject);
        addField(sb, NEXT_BY_SUBJECT, nextBySubject);
        return endJson(sb).toString();
    }

    /**
     * @deprecated prefer forSequenceBytes
     */
    @Deprecated
    public static byte[] seqBytes(long sequence) {
        return forSequenceBytes(sequence);
    }

    /**
     * @deprecated prefer lastForSubjectBytes
     */
    @Deprecated
    public static byte[] lastBySubjectBytes(String subject) {
        return lastForSubjectBytes(subject);
    }
}
