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
