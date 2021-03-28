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

import static io.nats.examples.ExampleUtils.uniqueEnough;

public class ExampleArgs {

    public enum Trail {MESSAGE, COUNT, QUEUE_AND_COUNT}

    public String server = Options.DEFAULT_URL;
    public String subject;
    public String queue;
    public String message;
    public int msgCount = -1;
    public int subCount = -1;
    public String stream = null;
    public String consumer = null;
    public String durable = null;
    public String deliver = null;
    public int pullSize = 0;
    public Headers headers;
    public String user = null;
    public String pass = null;

    public boolean hasHeaders() {
        return headers != null && headers.size() > 0;
    }

    public ExampleArgs(String[] args, Trail trail, String usageString) {
        try {
            if (args != null) {
                String lastKey = null;
                for (int x = 0; x < args.length; x++) {
                    String arg = args[x];
                    if (arg.startsWith("-")) {
                        handleKeyedArg(arg, args[++x]);
                        lastKey = arg;
                    }
                    else if (trail == null) {
                        if (lastKey != null) {
                            handleKeyedArg(lastKey, arg);
                        }
                    }
                    else {
                        handleTrailingArg(trail, arg);
                        lastKey = null;
                    }
                }
            }
        }
        catch (RuntimeException e) {
            System.err.println("Exception while processing command line arguments: " + e + "\n");
            if (usageString != null) {
                System.out.println(usageString);
            }
            System.exit(-1);
        }
    }

    private void handleTrailingArg(Trail trail, String arg) {
        if (subject == null) { // subject always the first expected
            subject = arg;
        }
        else if (trail == Trail.MESSAGE) {
            if (message == null) {
                message = arg;
            }
            else {
                message = message + " " + arg;
            }
        }
        else if (trail == Trail.QUEUE_AND_COUNT) {
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
            case "-sub":
                subject = value;
                break;
            case "-q":
                queue = value;
                break;
            case "-m":
                if (message == null) {
                    message = value;
                }
                else {
                    message = message + " " + value;
                }
                break;
            case "-con":
                consumer = value;
                break;
            case "-strm":
                stream = value;
                break;
            case "-pull":
                pullSize = Integer.parseInt(value);
                break;
            case "-mcnt":
                msgCount = Integer.parseInt(value);
                break;
            case "-scnt":
                subCount = Integer.parseInt(value);
                break;
            case "-dur":
                durable = value;
                break;
            case "-dlvr":
                deliver = value;
                break;
            case "-user":
                user = value;
                break;
            case "-pass":
                pass = value;
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String subject;
        private String queue;
        private String message;
        private int msgCount = -1;
        public int subCount = -1;
        private String stream = null;
        private String consumer = null;
        private String durable = null;
        private String deliver = null;
        private int pullSize = 0;
        private String user = null;
        private String pass = null;
        private boolean uniqueify = false;

        public Builder defaultSubject(String subject) {
            this.subject = subject;
            return this;
        }

        public Builder defaultQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder defaultMessage(String message) {
            this.message = message;
            return this;
        }

        public Builder defaultMsgCount(int msgCount) {
            this.msgCount = msgCount;
            return this;
        }

        public Builder defaultSubCount(int subCount) {
            this.subCount = subCount;
            return this;
        }

        public Builder defaultStream(String stream) {
            this.stream = stream;
            return this;
        }

        public Builder defaultConsumer(String consumer) {
            this.consumer = consumer;
            return this;
        }

        public Builder defaultDurable(String durable) {
            this.durable = durable;
            return this;
        }

        public Builder defaultDeliver(String deliver) {
            this.deliver = deliver;
            return this;
        }

        public Builder defaultUser(String user) {
            this.user = user;
            return this;
        }

        public Builder defaultPass(String pass) {
            this.pass = pass;
            return this;
        }

        public Builder defaultPullSize(int pullSize) {
            this.pullSize = pullSize;
            return this;
        }

        public Builder uniqueify() {
            uniqueify = true;
            return this;
        }

        public ExampleArgs build(String[] args) {
            ExampleArgs ea = new ExampleArgs(args, null, null);
            if (ea.subject == null) {
                ea.subject = subject;
            }
            if (ea.queue == null) {
                ea.queue = queue;
            }
            if (ea.message == null) {
                ea.message = message;
            }
            if (ea.msgCount == -1) {
                ea.msgCount = msgCount;
            }
            if (ea.subCount == -1) {
                ea.subCount = subCount;
            }
            if (ea.stream == null) {
                ea.stream = stream;
            }
            if (ea.consumer == null) {
                ea.consumer = consumer;
            }
            if (ea.durable == null) {
                ea.durable = durable;
            }
            if (ea.deliver == null) {
                ea.deliver = deliver;
            }
            if (ea.user == null) {
                ea.user = user;
            }
            if (ea.pass == null) {
                ea.pass = pass;
            }
            if (ea.pullSize == 0) {
                ea.pullSize = pullSize;
            }
            if (uniqueify) {
                String u = "-" + uniqueEnough();
                if (ea.stream != null) {
                    ea.stream += u;
                }
                if (ea.subject != null) {
                    ea.subject += u;
                }
                if (ea.queue != null) {
                    ea.queue += u;
                }
                if (ea.consumer != null) {
                    ea.consumer += u;
                }
                if (ea.durable != null) {
                    ea.durable += u;
                }
                if (ea.deliver != null) {
                    ea.deliver += u;
                }
            }
            return ea;
        }
    }
}