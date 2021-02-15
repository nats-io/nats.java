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

package io.nats.examples;

import io.nats.client.Options;
import io.nats.client.impl.Headers;

public class ExampleArgs {

    public enum Expect {MESSAGE, COUNT, QUEUE_AND_COUNT}

    public String server = Options.DEFAULT_URL;
    public Headers headers;
    public String subject;
    public String queue;
    public String message;
    public int msgCount = -1;
    public String stream = null;
    public String consumer = null;
    public String durable = null;
    public int pollSize = 0;
    public long sleep = -1;

    public boolean hasHeaders() {
        return headers != null && headers.size() > 0;
    }

    public static String getServer(String[] args) {
        if (args.length == 1) {
            return args[0];
        } else if (args.length == 2 && args[0].equals("-s")) {
            return args[1];
        }
        return Options.DEFAULT_URL;
    }

    public ExampleArgs(String[] args, Expect expect, String usageString) {
        try {
            for (int x = 0; x < args.length; x++) {
                String arg = args[x];
                if (arg.startsWith("-")) {
                    handleKeyedArg(arg, args[++x]);
                }
                else {
                    handleExpectedArgs(expect, arg);
                }
            }
        }
        catch (RuntimeException e) {
            System.err.println("Exception while processing command line arguments: " + e + "\n");
            System.out.println(usageString);
            System.exit(-1);
        }
    }

    private void handleExpectedArgs(Expect expect, String arg) {
        if (subject == null) { // subject always the first expected
            subject = arg;
        }
        else if (expect == Expect.MESSAGE) {
            if (message == null) {
                message = arg;
            }
            else {
                message = message + " " + arg;
            }
        }
        else if (expect == Expect.QUEUE_AND_COUNT) {
            if (queue == null) {
                queue = arg;
            }
            else {
                msgCount = Integer.parseInt(arg);
            }
        }
        else { // Expect.COUNT
            msgCount = Integer.parseInt(arg);
        }
    }

    private void handleKeyedArg(String key, String value) {
        switch (key) {
            case "-s":
                server = value;
                break;
            case "-consumer":
                consumer = value;
                break;
            case "-stream":
                stream = value;
                break;
            case "-poll":
                pollSize = Integer.parseInt(value);
                break;
            case "-msgCount":
                msgCount = Integer.parseInt(value);
                break;
            case "-durable":
                durable = value;
                break;
            case "-sleep":
                sleep = Long.parseLong(value);
                break;
            case "-h":
                if (headers == null) {
                    headers = new Headers();
                }
                String[] hdr = value.split(":");
                headers.add(hdr[0], hdr[1]);
                break;
        }
    }
}