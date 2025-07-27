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

package io.nats.compatibility;

import io.nats.client.Message;

public class TestMessage {
    // info from the message
    public final String subject;
    public final String replyTo;
    public final byte[] payload;

    // info from the subject
    public final Suite suite;
    public final Test test;
    public final String something;
    public final Kind kind;

    public TestMessage(Message m) {
        this.replyTo = m.getReplyTo();
        this.subject = m.getSubject(); // tests.<suite>.<test>.<something>.[command|result]
        this.payload = m.getData();

        String[] split = subject.split("\\.");

        this.suite = Suite.instance(split[1]);
        if (split.length > 2) {
            this.test = Test.instance(split[2]);
            this.something = split[3];
            this.kind = Kind.instance(split[4]);
        }
        else {
            this.test = null;
            this.something = null;
            this.kind = null;
        }
    }

    protected TestMessage(TestMessage tm) {
        this.subject = tm.subject;
        this.replyTo = tm.replyTo;
        this.payload = tm.payload;
        this.suite = tm.suite;
        this.test = tm.test;
        this.something = tm.something;
        this.kind = tm.kind;
    }
}
