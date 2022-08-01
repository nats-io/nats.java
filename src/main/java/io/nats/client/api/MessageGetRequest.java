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

    public static MessageGetRequest seq(long sequence) {
        return new MessageGetRequest(sequence);
    }

    public static MessageGetRequest lastBySubject(String lastBySubject) {
        return new MessageGetRequest(lastBySubject);
    }

    public static MessageGetRequest nextBySubject(String nextBySubject) {
        return new MessageGetRequest(-1, null, nextBySubject);
    }

    public static MessageGetRequest nextBySubject(long sequence, String nextBySubject) {
        return new MessageGetRequest(sequence, null, nextBySubject);
    }

    public static byte[] seqBytes(long sequence) {
        return new MessageGetRequest(sequence, null, null).serialize();
    }

    public static byte[] lastBySubjectBytes(String lastBySubject) {
        return new MessageGetRequest(-1, lastBySubject, null).serialize();
    }

    public static byte[] nextBySubjectBytes(String nextBySubject) {
        return new MessageGetRequest(-1, null, nextBySubject).serialize();
    }

    public static byte[] nextBySubjectBytes(long sequence, String nextBySubject) {
        return new MessageGetRequest(sequence, null, nextBySubject).serialize();
    }

    public MessageGetRequest(long sequence) {
        this(sequence, null, null);
    }

    public MessageGetRequest(String lastBySubject) {
        this(-1, lastBySubject, null);
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
}
