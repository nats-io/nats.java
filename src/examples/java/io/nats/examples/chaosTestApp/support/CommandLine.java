// Copyright 2021-2022 The NATS Authors
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

package io.nats.examples.chaosTestApp.support;

import io.nats.client.ConnectionListener;
import io.nats.client.ErrorListener;
import io.nats.client.Options;
import io.nats.examples.chaosTestApp.Output;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.parseInt;

public class CommandLine {

    private void usage() {
        System.out.println(
            "----------------------------------------------------------------------------------------------------\n" +
            "APP COMMAND LINE\n" +
            "----------------------------------------------------------------------------------------------------\n" +
            "--servers <natsServerUrl>[,<natsServerUrl>]*\n" +
            "  * i.e. --servers nats://localhost:4000,nats://localhost:4001,nats://localhost:4002\n" +
            "  * not supplied uses \"ats://localhost:4222\"\n" +
            "--stream <stream name>\n" +
            "  * not supplied uses \"app-stream\"\n" +
            "--subject <subject>\n" +
            "  * not supplied uses \"app-subject\"\n" +
            "--runtime <>m|<>s|<>ms|<>\n" +
            "  * not supplied or zero or negative infinite" +
            "  * m minute, s second ms millseconds no suffix milliseonds\n" +
            "--debug\n" +
            "  * show the debug window\n" +
            "--work\n" +
            "  * work the work window\n" +
            "--screen <left|center|console> console is default\n" +
            "--simple <ephemeral|durable|ordered>,batchSize,expiresInMs\n" +
            "--simple <ephemeral|durable|ordered> batchSize expiresInMs\n" +
            "  * Simple Consumer.\n" +
            "--fetch,<ephemeral|durable> batchSize expiresInMs\n" +
            "  * Simple Fetch Consumer.\n" +
            "--push,<ephemeral|durable|ordered>\n" +
            "  * Push Consumer.\n" +
            "*** One or more consumers is required.\n" +
            "--create\n" +
            "  * (Re)create the stream.\n" +
            "--r3\n" +
            "  * Make the stream R3 when (Re)create the stream.\n" +
            "--publish\n" +
            "  * Turns on publishing.\n" +
            "--pubjitter\n" +
            "  * publish jitter in milliseconds, amount of time to pause between publish\n" +
            "  * not supplied uses 50ms\n" +
            "--logdir\n" +
            "  * Directory to log to. Only logs if supplied\n" +
            "----------------------------------------------------------------------------------------------------\n"
        );
    }

    public final String[] servers;
    public final String stream;
    public final String subject;
    public final String logdir;
    public final long runtime;
    public final long pubjitter;
    public final boolean create;
    public final boolean r3;
    public final boolean publish;
    public final boolean debug;
    public final boolean work;
    public final Output.Screen uiScreen;
    public final List<CommandLineConsumer> commandLineConsumers;

    public Options makeManagmentOptions() {
        return makeOptions((conn, event) -> {}, new ErrorListener() {}, 0);
    }

    public Options makeOptions(ConnectionListener cl, ErrorListener el) {
        return makeOptions(cl, el, -1);
    }

    public Options makeOptions(ConnectionListener cl, ErrorListener el, int maxReconnects) {
        return new Options.Builder()
            .servers(servers)
            .connectionListener(cl)
            .errorListener(el)
            .maxReconnects(maxReconnects)
            .build();
    }

    // ----------------------------------------------------------------------------------------------------
    // ToString
    // ----------------------------------------------------------------------------------------------------
    private void append(StringBuilder sb, String label, Object value, boolean test) {
        if (test) {
            sb.append("--").append(label).append(" ").append(value).append(" ");
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Chaos Test App Config ");
        append(sb, "servers", String.join(",", servers), true);
        append(sb, "stream", stream, true);
        append(sb, "subject", subject, true);
        append(sb, "logdir", logdir, logdir != null);
        append(sb, "runtime", runtime, true);
        append(sb, "create", create, create);
        append(sb, "R3", r3, r3);
        append(sb, "publish", publish, publish);
        append(sb, "pubjitter", pubjitter, publish);
        append(sb, "debug", debug, debug);
        append(sb, "work", work, work);
        append(sb, "screen", uiScreen, true);
        for (CommandLineConsumer cc : commandLineConsumers) {
            append(sb, "consumer", cc, true);
        }
        return sb.toString().trim();
    }

    // ----------------------------------------------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------------------------------------------
    public CommandLine(String[] args) {
        try {
            String[] _servers = new String[]{Options.DEFAULT_URL};
            String _stream = "app-stream";
            String _subject = "app-subject";
            String _logdir = null;
            long _runtime = -1;
            long _publishJitter = 50;
            boolean _debug = false;
            boolean _create = false;
            boolean _r3 = false;
            boolean _publish = false;
            boolean _work = false;
            Output.Screen _uiScreen = Output.Screen.Console;
            List<CommandLineConsumer> _commandLineConsumers = new ArrayList<>();

            if (args != null && args.length > 0) {
                try {
                    for (int x = 0; x < args.length; x++) {
                        String arg = args[x].trim();
                        if (arg.isEmpty()) {
                            continue;
                        }
                        switch (arg) {
                            case "--servers":
                                _servers = asString(args[++x]).split(",");
                                break;
                            case "--stream":
                                _stream = asString(args[++x]);
                                break;
                            case "--subject":
                                _subject = asString(args[++x]);
                                break;
                            case "--logdir":
                                _logdir = asString(args[++x]);
                                break;
                            case "--runtime":
                                _runtime = (long) asNumber("runtime", args[++x], -1) * 1000;
                                break;
                            case "--pubjitter":
                                _publishJitter = asNumber("pubjitter", args[++x], -1);
                                break;
                            case "--create":
                                _create = true;
                                break;
                            case "--r3":
                                _r3 = true;
                                break;
                            case "--publish":
                                _publish = true;
                                break;
                            case "--debug":
                                _debug = true;
                                break;
                            case "--work":
                                _work = true;
                                break;
                            case "--screen":
                                String screen = asString(args[++x]).toLowerCase();
                                if (screen.equals("left")) {
                                    _uiScreen = Output.Screen.Left;
                                }
                                else if (screen.equals("center")) {
                                    _uiScreen = Output.Screen.Main;
                                }
                                else {
                                    throw new IllegalArgumentException("Unknown Screen");
                                }
                                break;
                            case "--simple":
                            case "--fetch":
                                String temp = args[++x];
                                if (temp.contains(",")) {
                                    String[] split = temp.split(",");
                                    _commandLineConsumers.add(new CommandLineConsumer(
                                        arg.substring(2),
                                        split[0],
                                        asNumber("batchSize", split[1], -1),
                                        asNumber("expiresInMs", split[2], -1)
                                    ));
                                }
                                else {
                                    _commandLineConsumers.add(new CommandLineConsumer(
                                        arg.substring(2),
                                        temp,
                                        asNumber("batchSize", args[++x], -1),
                                        asNumber("expiresInMs", args[++x], -1)
                                    ));
                                }
                                break;
                            case "--push":
                                _commandLineConsumers.add(new CommandLineConsumer(args[++x]));
                                break;
                            default:
                                throw new IllegalArgumentException("Unknown argument: " + arg);
                        }
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                    throw new IllegalArgumentException("Exception while parsing, most likely missing an argument value.");
                }
            }

            if (!_create && !_publish && _commandLineConsumers.isEmpty()) {
                throw new IllegalArgumentException("Consumer commands are required if not creating or publishing");
            }

            servers = _servers;
            stream = _stream;
            subject = _subject;
            logdir = _logdir;
            runtime = _runtime;
            create = _create;
            r3 =_r3;
            publish = _publish;
            pubjitter = _publishJitter;
            debug = _debug;
            work = _work;
            uiScreen = _uiScreen;
            commandLineConsumers = _commandLineConsumers;
        }
        catch (RuntimeException e) {
            usage();
            throw e;
        }
    }

    private String asString(String val) {
        return val.trim();
    }

    private int asNumber(String name, String val, int upper) {
        int v = parseInt(val);
        if (upper == -2 && v < 1) {
            return Integer.MAX_VALUE;
        }
        if (upper > 0) {
            if (v > upper) {
                throw new IllegalArgumentException("Value for " + name + " cannot exceed " + upper);
            }
        }
        return v;
    }

    private int asNumber(String name, String val, int lower, int upper) {
        int v = parseInt(val);
        if (v < lower) {
            throw new IllegalArgumentException("Value for " + name + " cannot be less than " + lower);
        }
        if (v > upper) {
            throw new IllegalArgumentException("Value for " + name + " cannot exceed " + upper);
        }
        return v;
    }
}
