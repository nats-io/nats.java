// Copyright 2015-2018 The NATS Authors
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

package io.nats.client.impl;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

public class ParseTesting {
    static String infoJson = "{" + "\"server_id\":\"myserver\"" + "," + "\"version\":\"1.1.1\"" + ","
            + "\"go\": \"go1.9\"" + "," + "\"host\": \"host\"" + "," + "\"tls_required\": true" + ","
            + "\"auth_required\":false" + "," + "\"port\": 7777" + "," + "\"max_payload\":100000000000" + ","
            + "\"connect_urls\":[\"one\", \"two\"]" + "}";

    public static void main(String args[]) throws InterruptedException {
        int iterations = 1_000_000;

        System.out.println("###");
        System.out.printf("### Running parse tests with %s msgs.\n", NumberFormat.getInstance().format(iterations));
        System.out.println("###");

        runTest(iterations, "+OK");
        runTest(iterations, "PONG");
        runTest(iterations, "INFO " + infoJson);
        runTest(iterations, "MSG longer.subject.abitlikeaninbox 22 longer.replyto.abitlikeaninbox 234");
        runTest(iterations, "-ERR some error with spaces in it");

    }

    public static String[] splitCharBuffer(ByteBuffer buffer) {
        ArrayList<String> list = new ArrayList<>();
        StringBuilder builder = new StringBuilder();

        while (buffer.hasRemaining()) {
            byte c = buffer.get();

            if (c == ' ') {
                list.add(builder.toString());
                builder = new StringBuilder();
            } else {
                builder.append(c);
            }
        }

        if (builder.length() > 0) {
            list.add(builder.toString());
        }

        return list.toArray(new String[0]);
    }

    public static String grabNextWithBuilder(CharBuffer buffer) {
        StringBuilder builder = new StringBuilder();

        while (buffer.hasRemaining()) {
            char c = buffer.get();

            if (c == ' ') {
                return builder.toString();
            } else {
                builder.append(c);
            }
        }

        return builder.toString();
    }
    
    public static String grabNextWithSubsequence(CharBuffer buffer) {
        if (!buffer.hasRemaining()) {
            return null;
        }

        int start = buffer.position();

        while (buffer.hasRemaining()) {
            char c = buffer.get();

            if (c == ' ') {
                int end = buffer.position();
                buffer.position(start);
                CharBuffer slice = buffer.subSequence(0, end-start-1); //don't grab the space
                buffer.position(end);
                return slice.toString();
            }
        }

        buffer.position(start);
        String retVal = buffer.toString();
        buffer.position(buffer.limit());
        return retVal;
    }
    
    public static ByteBuffer grabNextAsSubsequence(ByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            return null;
        }

        int start = buffer.position();

        while (buffer.hasRemaining()) {
            byte c = buffer.get();

            if (c == ' ') {
                int end = buffer.position();
                buffer.mark();
                buffer.position(start);
                buffer.limit(end-start-1); //don't grab the space
                ByteBuffer slice = buffer.slice();
                buffer.reset();
                return slice;
            }
        }

        buffer.position(start);
        ByteBuffer retVal = buffer.slice();
        buffer.position(buffer.limit());
        return retVal;
    }

    static byte[] buff = new byte[1024];
    public static String grabNextWithCharArray(ByteBuffer buffer) {
        int remaining = buffer.remaining();

        if (remaining == 0) {
            return null;
        }

        int i = 0;

        while (remaining > 0) {
            byte c = buffer.get();

            if (c == ' ') {
                return new String(buff, 0, i);
            } else {
                buff[i] = c;
                i++;
            }
            remaining--;
        }

        return new String(buff, 0, i);
    }

    public static ByteBuffer protocolFor(byte[] chars, int length) {
        if (length == 3) {
            if (chars[0] == '+' && chars[1] == 'O' && chars[2] == 'K') {
                return NatsConnection.OP_OK;
            } else if (chars[0] == 'M' && chars[1] == 'S' && chars[2] == 'G') {
                return NatsConnection.OP_MSG;
            } else {
                return null;
            }
        } else if (length == 4) { // order for uniqueness
            if (chars[1] == 'I' && chars[0] == 'P' && chars[2] == 'N' && chars[3] == 'G') {
                return NatsConnection.OP_PING;
            } else if (chars[0] == 'P' && chars[1] == 'O' && chars[2] == 'N' && chars[3] == 'G') {
                return NatsConnection.OP_PONG;
            } else if (chars[0] == '-' && chars[1] == 'E' && chars[2] == 'R' && chars[3] == 'R') {
                return NatsConnection.OP_ERR;
            } else if (chars[2] == 'F' && chars[0] == 'I' && chars[1] == 'N' && chars[3] == 'O') {
                return NatsConnection.OP_INFO;
            }  else {
                return null;
            }
        } else {
            return null;
        }
    }

    public static ByteBuffer grabProtocol(ByteBuffer buffer) {
        int remaining = buffer.remaining();

        if (remaining == 0) {
            return null;
        }

        int i = 0;

        while (remaining > 0) {
            byte c = buffer.get();

            if (c == ' ') {
                return protocolFor(buff, i);
            } else {
                buff[i] = c;
                i++;
            }
            remaining--;
        }

        return protocolFor(buff, i);
    }

    public static ByteBuffer grabNext(ByteBuffer buffer) {
        return grabNextAsSubsequence(buffer);
    }

    public static ByteBuffer grabTheRest(ByteBuffer buffer) {
        return buffer;
    }

    public static void runTest(int iterations, String serverMessage) {
        Pattern space = Pattern.compile(" ");
        ByteBuffer protocolBuffer = ByteBuffer.allocate(32 * 1024);
        protocolBuffer.put(serverMessage.getBytes(StandardCharsets.UTF_8));
        protocolBuffer.flip();

        ByteBuffer buffer = protocolBuffer;
        String pl = buffer.toString();
        buffer.rewind();
        protocolBuffer.rewind();

        String[] newversion = splitCharBuffer(buffer);
        String[] oldversion = space.split(pl);

        buffer.rewind();
        ArrayList<String> opAware = new ArrayList<>();
        ByteBuffer s = null;
        while((s = grabNext(buffer)) != null) {
            opAware.add(s.toString());
        }
        String[] opAwareArray = opAware.toArray(new String[0]);

        System.out.printf("### Parsing server string: %s\n", serverMessage);

        boolean newOk = Arrays.equals(newversion, oldversion);
        System.out.println("### Old and new versions are equal: " + newOk);

        if (!newOk) {
            System.exit(-1);
        }

        boolean protoOk = Arrays.equals(opAwareArray, oldversion);
        System.out.println("### Old and op-aware versions are equal: " + protoOk);

        if (!protoOk) {
            System.exit(-1);
        }

        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            StandardCharsets.UTF_8.decode(protocolBuffer).toString();
            protocolBuffer.rewind();
        }
        long end = System.nanoTime();
        System.out.printf("### %s raw utf8 decode/sec.\n",
                NumberFormat.getInstance().format(1_000_000_000L * iterations / (end - start)));

        start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            String protocolLine = StandardCharsets.UTF_8.decode(protocolBuffer).toString();
            space.split(protocolLine);
            protocolBuffer.rewind();
        }
        end = System.nanoTime();

        System.out.printf("### %s old parses/sec.\n",
                NumberFormat.getInstance().format(1_000_000_000L * iterations / (end - start)));

        start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            ByteBuffer charBuffer = protocolBuffer;
            splitCharBuffer(charBuffer);
            protocolBuffer.rewind();
        }
        end = System.nanoTime();

        System.out.printf("### %s new parses/sec.\n",
                NumberFormat.getInstance().format(1_000_000_000L * iterations / (end - start)));

        start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            ByteBuffer charBuffer = protocolBuffer;
            ByteBuffer op = grabProtocol(charBuffer);

            if (op.equals(NatsConnection.OP_MSG))
            {
                grabNext(charBuffer); //subject
                grabNext(charBuffer); // sid
                grabNext(charBuffer); // replyto or length
                grabNext(charBuffer); // length or null
            }
            else if (op.equals(NatsConnection.OP_ERR))
            {
                grabTheRest(charBuffer);
            }
            else if (op.equals(NatsConnection.OP_OK) ||
                    op.equals(NatsConnection.OP_PING) ||
                    op.equals(NatsConnection.OP_PONG) ||
                    op.equals(NatsConnection.OP_INFO))
            {
                grabTheRest(charBuffer);
            }
            protocolBuffer.rewind();
        }
        end = System.nanoTime();

        System.out.printf("### %s op-aware parses/sec.\n",
                NumberFormat.getInstance().format(1_000_000_000L * iterations / (end - start)));
        System.out.println();
    }
}