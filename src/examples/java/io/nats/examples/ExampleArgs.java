// Copyright 2018 The NATS Authors
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

package io.nats.examples;

import io.nats.client.Options;
import io.nats.client.impl.Headers;

public class ExampleArgs {
    public String server = Options.DEFAULT_URL;
    public Headers headers;
    public String subject;
    public String message;
    public int msgCount = -1;

    // Publish:   [-s server] [-h headerKey:headerValue]* <subject> <message>
    // Subscribe: [-s server] <subject> <msgCount>

    // Request:   [-s server] [-h headerKey:headerValue]* <subject> <message>
    // Reply:     [-s server] <subject> <msgCount>

    public static ExampleArgs readPublishArgs(String[] args, String usageString) {
        ExampleArgs ea = new ExampleArgs(args, true, usageString);
        if (ea.message == null) {
            usage(usageString);
        }
        return ea;
    }

    public static ExampleArgs readRequestArgs(String[] args, String usageString) {
        return readPublishArgs(args, usageString);
    }

    public static ExampleArgs readSubscribeArgs(String[] args, String usageString) {
        ExampleArgs ea = new ExampleArgs(args, false, usageString);
        if (ea.msgCount < 1) {
            usage(usageString);
        }
        return ea;
    }

    public static ExampleArgs readReplyArgs(String[] args, String usageString) {
        return readSubscribeArgs(args, usageString);
    }

    public boolean hasHeaders() {
        return headers != null && headers.size() > 0;
    }

    private ExampleArgs(String[] args, boolean pubReq, String usageString) {
        try {
            for (int x = 0; x < args.length; x++) {
                String arg = args[x];
                if (arg.startsWith("-")) {
                    handleArg(pubReq, args[++x], arg);
                }
                else {
                    handleArg(pubReq, arg, null);
                }
            }
        }
        catch (RuntimeException e) {
            System.out.println("Exception while processing command line arguments: " + e + "\n");
            usage(usageString);
        }
    }

    private void handleArg(boolean pubReq, String value, String name) {
        if (name == null) {
            if (subject == null) {
                subject = value;
            }
            else if (pubReq) {
                message = value;
            }
            else {
                msgCount = Integer.parseInt(value);
            }
        }
        else if (name.equals("-s")) {
            server = value;
        }
        else if (name.equals("-h")) {
            if (headers == null) {
                headers = new Headers();
            }
            String[] hdr = value.split(":");
            headers.add(hdr[0], hdr[1]);
        }
    }

    private static void usage(String usageString) {
        System.err.println(usageString);
        System.exit(-1);
    }
}