package io.nats.client.proto;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;

import java.io.IOException;

public class Scott {

    public static void main(String[] args) {
        // filename-safe      = (printable except dot, asterisk, lt, gt, colon, double-quote, fwd-slash, backslash, pipe, question-mark, ampersand)
        try (Connection nc = Nats.connect("localhost")) {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createStream(jsm, "safe", "safe");
            createStream(jsm, "dot.", "dot");
            createStream(jsm, "star*", "star");
            createStream(jsm, "greaterthan>", "greaterthan");
            createStream(jsm, "fwdslash/", "fwdslash");
            createStream(jsm, "backslash\\", "backslash");
            createStream(jsm, "lessthan<", "lessthan");
            createStream(jsm, "colon:", "colon");
            createStream(jsm, "doublequote\"", "doublequote");
            createStream(jsm, "pipe|", "pipe");
            createStream(jsm, "question?", "question");
            createStream(jsm, "ampersand&", "ampersand");
            createStream(jsm, "pound#", "pound");
            createStream(jsm, "percent%", "percent");
            createStream(jsm, "leftcurly{", "leftcurly");
            createStream(jsm, "rightcurly}", "rightcurly");
            createStream(jsm, "dollar$", "dollar");
            createStream(jsm, "exclamation!", "exclamation");
            createStream(jsm, "singlequote'", "singlequote");
            createStream(jsm, "at@", "at");
            createStream(jsm, "plus+", "plus");
            createStream(jsm, "backtick`", "backtick");
            createStream(jsm, "equal=", "equal");
            createStream(jsm, "carrot^", "carrot");
            createStream(jsm, "leftp(", "leftp");
            createStream(jsm, "rightp)", "rightp");
            createStream(jsm, "leftb[", "leftb");
            createStream(jsm, "rightb]", "rightb");
            createStream(jsm, "semi;", "semi");
            createStream(jsm, "comma,", "comman");
            createStream(jsm, "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890", "long");
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    public static void createStream(JetStreamManagement jsm, String stream, String... subs) throws IOException, JetStreamApiException {
        try {
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(stream)
                    .storageType(StorageType.File)
                    .subjects(subs)
                    .build();
            jsm.addStream(sc);
            System.out.println("Created stream: '" + stream + "'");
        }
        catch (Exception e) {
            System.out.println("Failed creating stream: '" + stream + "' " + e);
        }
    }

}
