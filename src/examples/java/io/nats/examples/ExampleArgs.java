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

    public String server = Options.DEFAULT_URL;
    public Headers headers;
    public String subject;
    public String message;
    public int msgCount = -1;
    public String stream = null;
    public String consumer = null;
    public String durable = null;
    public int pollSize = 0;

    public boolean hasHeaders() {
        return headers != null && headers.size() > 0;
    }

    public ExampleArgs(String[] args, boolean pubNotSub, boolean js, String usageString) {
        try {
            for (int x = 0; x < args.length; x++) {
                String arg = args[x];
                if (arg.startsWith("-")) {
                    handleArg(pubNotSub, js, args[++x], arg);
                }
                else {
                    handleArg(pubNotSub, js, arg, null);
                }
            }
        }
        catch (RuntimeException e) {
            System.err.println("Exception while processing command line arguments: " + e + "\n");
            System.out.println(usageString);
            System.exit(-1);
        }
    }

    private void handleArg(boolean pubNotSub, boolean js, String value, String name) {
        if (name == null) {
            if (js && stream == null) {
                stream = value;
            }
            else if (subject == null) {
                subject = value;
            }
            else if (pubNotSub) {
                if (message == null) {
                    message = value;
                }
                else {
                    message = message + " " + value;
                }
            }
            else {
                msgCount = Integer.parseInt(value);
            }
        }
        else if (name.equals("-s")) {
            server = value;
        }
        else if (name.equals("-consumer")) {
            consumer = value;
        }
        else if (name.equals("-stream")) {
            stream = value;
        } else if (name.equals("-poll")) {
            pollSize = Integer.parseInt(value);
        }
        else if (name.equals("-msgCount")) {
            msgCount = Integer.parseInt(value);
        }
        else if (name.equals("-durable")) {
            durable = value;
        }
        else if (name.equals("-h")) {
            if (headers == null) {
                headers = new Headers();
            }
            String[] hdr = value.split(":");
            headers.add(hdr[0], hdr[1]);
        }
    }
}